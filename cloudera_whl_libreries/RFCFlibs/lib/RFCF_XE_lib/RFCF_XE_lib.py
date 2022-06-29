from datetime import timedelta, datetime
import calendar as cl
import pyspark.sql.functions as f
from pyspark.sql.types import *
from collections import OrderedDict
from pyspark.sql import window
import random

from udfs import *
from shared_lib import pro_die, customUnion


def creazione_ambito(decodifiche,codifiche_tariffario):
  """Metodo che prepara i dati di configurazione 
  
  INPUT:  decodifiche -> dataframe
          codifiche_tariffario -> dataframe
  
  OUTPUT: tar --> dataframe
  
  """
  ambito = decodifiche.filter(f.col('tipo') == 'COMUNI_PROVINCE').select(f.col('id').alias('codice_istat_alfanumerico'),f.col('valore').alias('id_provincia'))\
                      .withColumn('codice_istat_alfanumerico',f.substring(f.concat(f.lit('00'),f.col('codice_istat_alfanumerico') ), -6,6) )\
                      .join(decodifiche.filter(f.col('tipo') == 'PROVINCE_REGIONI').select(f.col('id').alias('id_provincia'),f.col('valore').alias('id_regione'))\
                           ,'id_provincia','left')
  #         ambito_xe-regione_xe
  ambito = ambito.join(decodifiche.filter(f.col('tipo') == 'REGIONE_AMBITO').select(f.col('id').alias('id_regione'),f.col('valore_2').alias('ambito_xe'))\
                           ,'id_regione','left')\
                 .join(codifiche_tariffario.filter( (f.col('sist_fat') == 'XE') & (f.col('foglio') == 'VALORI_CODIFICA') & (f.col('tog_valore') == 'REG_ABBR' ) ) \
                           .select(f.col('valore_tcr').alias('id_regione'),f.col('asse_valore').alias('regione_xe') ),'id_regione','left')
  #         fascia_climatica-mezzogiorno
  ambito = ambito.join(codifiche_tariffario.filter( (f.col('foglio') == 'VALORI_CODIFICA') & (f.col('tog_valore') == 'CBL_FACL' ) ) \
                           .select(f.col('valore_tcr').alias('codice_istat_alfanumerico'),f.col('asse_valore').alias('fascia_climatica') ).dropDuplicates(),'codice_istat_alfanumerico','left')\
                 .join(codifiche_tariffario.filter( (f.col('sist_fat') == 'ALL') & (f.col('foglio') == 'VALORI_CODIFICA') & (f.col('tog_valore') == 'STR_ZON' ) &  (f.col('asse_valore')=='NG_ZONA_SUD')) \
                           .select(f.col('valore_tcr').alias('codice_istat_alfanumerico'),f.lit(1).alias('flg_mezz') ),'codice_istat_alfanumerico','left')
  #         aggiornamento regione_xe per fascia_climatica e mezzogiorno                    
  ambito = ambito.withColumn('id_regione_fascia',f.concat(f.col('id_regione'),f.lit('_'),f.col('fascia_climatica') ) )\
                 .join(codifiche_tariffario.filter( (f.col('sist_fat') == 'XE') & (f.col('foglio') == 'VALORI_CODIFICA') & (f.col('tog_valore') == 'REG_FASCIA' ) ) \
                           .select(f.col('valore_tcr').alias('id_regione_fascia'),f.col('asse_valore').alias('fascia_regione_xe') ),'id_regione_fascia','left')
  ambito = ambito.join(codifiche_tariffario.filter( (f.col('sist_fat') == 'XE') & (f.col('foglio') == 'VALORI_CODIFICA') & (f.col('tog_valore') == 'REG_MEZZ' ) ) \
                           .select(f.col('valore_tcr').alias('id_regione'),f.col('asse_valore').alias('mezz_regione_xe'), f.lit(1).alias('flg_mezz') )\
                           ,['id_regione','flg_mezz'],'left')     
  ambito = ambito.withColumn('regione_xe',f.coalesce(f.col('mezz_regione_xe'),f.coalesce(f.col('fascia_regione_xe'),f.col('regione_xe') )  ) )
  
  return ambito

def mapping_indici_fatturazione (indici,spark):
  """Metodo che  
  
  INPUT:  indici -> dataframe
          tariffario -> dataframe
  
  OUTPUT: map_indici --> dataframe
  
  """
  if indici.count() == 0 :
    
    indici_per_map = spark.sql("""SELECT * from lab1_db.rfcf_tariffario_xe_indici_fatturazione_default""")
  
  else:
    
    indici_per_map = indici
    
  indici_condizione = indici_per_map.select('variabili_xe').distinct()\
                            .withColumn('condizione',f.concat(f.lit("formulaprz like('%"),f.col('variabili_xe'),f.lit("%')") ) )
  indici_condizione = indici_condizione.select(f.concat_ws(" or ",f.collect_list(f.col('condizione')) ))
  indici_condizione = indici_condizione.collect()[0][0]
  indici_campo = indici_per_map.select('variabili_xe').distinct()\
                            .withColumn('campo',f.concat(f.lit("regexp_extract(formulaprz,'#"),f.col('variabili_xe'),f.lit("#',0)") ) )
  indici_campo = indici_campo.select(f.concat_ws(" , ",f.collect_list(f.col('campo')) ))
  indici_campo = indici_campo.collect()[0][0]
    
  campi_map_indici = """distinct coditem ind_ci, codpricelist ind_cl, commodity ind_com, formulaprz ind_fprz, concat( """ +indici_campo + """) as ind_indice """
  campi_map_indici_punh =  """distinct coditem ind_ci, codpricelist ind_cl, commodity ind_com, formulaprz ind_fprz, 'PUN_F0' as ind_indice """
  
  query_map_indici = """ SELECT """ + campi_map_indici + """ FROM edl_int.br_tcr_tariffexe_table 
                         WHERE (""" + indici_condizione + """)
                               and codinstallation <> '####'
                         UNION ALL
                         SELECT """ + campi_map_indici + """ FROM edl_int.br_tcr_tariffexe_table 
                         WHERE (""" + indici_condizione + """)
                               and codinstallation = '####'
                         UNION ALL
                         SELECT """ + campi_map_indici + """ FROM edl_int.br_tcr_tariffexe_table t
                         INNER JOIN (
                               SELECT distinct codpricelist cl, codpricelisttype clt, formulaprz f from edl_int.br_tcr_tariffexe_table
                               where (""" + indici_condizione + """)
                                     and codinstallation = '####' ) gen 
                         ON  (gen.cl =t.codpricelist and gen.clt = t.codpricelisttype and gen.f = t.formulaprz)
                         UNION ALL
                         SELECT """ + campi_map_indici_punh + """ FROM edl_int.br_tcr_tariffexe_table 
                         WHERE ( coditem like '%EF%PUNh%' )
                               and codinstallation <> '####'
                         """
						 				 
  map_indici = spark.sql(query_map_indici).filter( (f.col('ind_ci').isNotNull() ) & (f.col('ind_ci')!=f.lit('') ) ).dropDuplicates()
  map_indici = map_indici.withColumn('ind_indice',f.regexp_replace(f.col('ind_indice'),'#','') )	
  
  return map_indici 

def preparazione_forniture(forn, ambito, spark):
  
  # Carico Forniture - classe misuratore, ambito ed aggancio al tariffario
  forn = forn.withColumn('data_cessazione',f.coalesce(f.col('data_cessazione'),f.lit(datetime(9999,12,31)) ) )
  forn = forn.join(ambito.select('codice_istat_alfanumerico','ambito_xe','regione_xe'), 'codice_istat_alfanumerico','left')
  
  # Ultima capacità giornaliera
  cap_gio = spark.sql("""select distinct forniture.cod_pratica_crm as cod_pratica, capacita_giornaliera as capacita_giornaliera_ultima 
                              from edl_int.vw_xe_tcr_tcr_forniture_lr forniture
                              inner join 
                                (select cod_pratica_crm, max(data_decorrenza) max_data_decorrenza 
                                from edl_int.vw_xe_tcr_tcr_forniture_lr 
                                where capacita_giornaliera is not null group by cod_pratica_crm) forniture_decorrenza
                              on (forniture.data_decorrenza = forniture_decorrenza.max_data_decorrenza and forniture.cod_pratica_crm = forniture_decorrenza.cod_pratica_crm)""")
  
  forn = forn.join(cap_gio,['cod_pratica'],'left') 
  
  # Gestione capacità giornaliera
  forn = forn.withColumn('capacita_giornaliera_default', f.when(f.col('forn_commodity') == 'GAS', f.col('consumo_annuo')/365 ).otherwise(None) )
  forn = forn.withColumn('capacita_giornaliera', f.coalesce(f.col('capacita_giornaliera'),f.col('capacita_giornaliera_ultima'), f.col('capacita_giornaliera_default') ) )\
             .drop('capacita_giornaliera_ultima','capacita_giornaliera_default')
  
  return forn

def tariffario_xe_parte1(tariffario,data_inizio_anno,data_fine_anno,forn,ambito, decodifiche,hec,distrng, id_run):
  
  # Tariffe
  tariffario = tariffario.drop('codcontracttype').withColumnRenamed('codcontracttype_tmp','codcontracttype')
  for col in tariffario.columns:
    tariffario = tariffario.withColumnRenamed(col, col.lower())
  tariffario = tariffario.withColumn('dateto',f.coalesce(f.col('dateto'),f.lit(datetime(9999,12,31)) ) )
  tariffario = tariffario.filter( f.col('dateto')>=f.lit(data_inizio_anno)  )
  tariffario = tariffario.withColumn('data_ini_tmp',f.greatest('datefrom',f.lit(data_inizio_anno)) )
  tariffario = tariffario.withColumn('data_end_tmp',f.least('dateto',f.lit(data_fine_anno)) )

  # Tar1 - Componenti non generiche / tar2 - Componenti generiche / tar3 - fornitura generiche
  cond_componenti_generiche = (f.col('price').isNull()) & (f.col('quantityfrom').isNull()) & (f.col('coditem')==f.lit('') )
  tar1 = tariffario.filter( (f.col('codinstallation') != '####' ) & ~(cond_componenti_generiche) ).withColumn('tipologia_tariffe',f.lit(1))
  tar2 = tariffario.filter( (f.col('codinstallation') != '####' ) & (cond_componenti_generiche) ).withColumn('tipologia_tariffe',f.lit(2))
  tar3 = tariffario.filter( f.col('codinstallation') == '####' )

  # Preparo unione tar2 - tar 3
  col_interessate =     ['quantityfrom','quantityto','coditem', 'price', 'thermalcoefficient',  \
                      'thermalcoefficientindex','codsalesunit','formulaprz','pricelistdescription']
  tar2 = tar2.drop(*(i for i in col_interessate))\
             .withColumnRenamed('data_ini_tmp','data_ini_forn_tmp').withColumnRenamed('data_end_tmp','data_end_forn_tmp')
  tar3 = tar3.select('codpricelisttype','codpricelist','commodity','data_ini_tmp','data_end_tmp',*(i for i in col_interessate))
  cond_join =[tar2.codpricelisttype == tar3.codpricelisttype, tar2.codpricelist == tar3.codpricelist,tar2.commodity == tar3.commodity,\
              ~( tar3.data_end_tmp<tar2.data_ini_forn_tmp ) | ~( tar3.data_ini_tmp>tar2.data_end_forn_tmp )\
              ]

  # Controllo listini generici senza componenti generiche
  listini_generici_assenti = tar2.join(tar3,cond_join,'left_anti')
  listini_generici_assenti = listini_generici_assenti.filter( f.col('data_end_forn_tmp') >= f.col('data_ini_forn_tmp') ).select('codpricelist','codpricelisttype').distinct()
  listini_generici_assenti = listini_generici_assenti.withColumn("listini_assenti", f.concat(f.lit('codpricelist = '),f.col('codpricelist'), f.lit(' - codpricelisttype = '), f.col('codpricelisttype') ))
  with open("./TAR_XE_storico_controlli_conteggi.txt", "a") as output:
    output.write('\n\n'+'RUN '+ str(id_run) + " - data creazione " + datetime.now().strftime('%Y%m%d') + " Check Listini Generici Assenti" )
  for i in range(0,listini_generici_assenti.count()):
    with open("./TAR_XE_storico_controlli_conteggi.txt", "a") as output:
      output.write('\n '+ listini_generici_assenti.select('listini_assenti').collect()[i][0])
    
  # Unisco tar2 - tar 3 e applico logica date
  tar2 = tar2.join(tar3,cond_join,'left').drop(tar3.codpricelisttype).drop(tar3.codpricelist).drop(tar3.commodity)
  tar2 = tar2.withColumn('datefrom',f.greatest(f.col('data_ini_forn_tmp'),f.col('data_ini_tmp'))).withColumn('dateto',f.least(f.col('data_end_forn_tmp'),f.col('data_end_tmp')))

  # Concateno i due tariffari
  tar1 = tar1.withColumn('datefrom',f.col('data_ini_tmp')).withColumn('dateto',f.col('data_end_tmp'))

  # Gestione coditem presenti sia come componente specifica che listino generico
  tar_spec = tar1.selectExpr("codinstallation as spec_codinstallation","codpromotion as spec_codpromotion", "codpromotionversion as spec_codpromotionversion",\
                             "datefrom as spec_datefrom","dateto as spec_dateto","codpricelisttype as spec_codpricelisttype","codpricelist as spec_codpricelist",\
                             "commodity as spec_commodity","coditem as spec_coditem","codcontracttype as spec_codcontracttype")
  
  campi_specifici = ['spec_codinstallation','spec_codpromotion','spec_codpromotionversion','spec_codpricelisttype','spec_codpricelist','spec_commodity','spec_coditem','spec_codcontracttype']

  tar_spec = tar_spec.groupBy(campi_specifici).agg(f.min('spec_datefrom').alias('spec_datefrom'), f.max('spec_dateto').alias('spec_dateto') ).withColumn('presenza_spec',f.lit(1) )
  
  join_specifici = [tar2.codinstallation == tar_spec.spec_codinstallation, tar2.codpromotion == tar_spec.spec_codpromotion, tar2.codpromotionversion == tar_spec.spec_codpromotionversion , \
                    tar2.codpricelisttype == tar_spec.spec_codpricelisttype, tar2.codpricelist == tar_spec.spec_codpricelist, tar2.commodity == tar_spec.spec_commodity, \
                    tar2.coditem == tar_spec.spec_coditem , tar2.codcontracttype == tar_spec.spec_codcontracttype \
                   ]
  
  tar_spec_gen = tar2.join(tar_spec,join_specifici, 'left')\
                     .withColumn('presenza_spec', f.when( (f.col('spec_dateto')<f.col('datefrom') ) | (f.col('spec_datefrom')>f.col('dateto') ), f.lit(None)  )\
                                 .otherwise(f.col('presenza_spec')  ) )
  
  campi_specifici.append('spec_datefrom')
  campi_specifici.append('spec_dateto')
  campi_specifici.append('presenza_spec')
  
  # Coditem NON presenti sia come componente specifica che listino generico
  tar2 = tar_spec_gen.filter(f.col('presenza_spec').isNull() ).drop(*[i for i in campi_specifici])
  
  # Coditem presenti sia come componente specifica che listino generico
  #    suddivido i casi in cui abbiamo lo specifico all'interno del periodo di validitÃ , non in un estremo
  tar_spec_gen = tar_spec_gen.filter(f.col('presenza_spec').isNotNull() )\
                             .withColumn('ck_spec',f.when( (f.col('dateto')<=f.col('spec_dateto')) | (f.col('datefrom')>=f.col('spec_datefrom')),f.lit(1) \
                                                         ).otherwise(f.lit(2)) )

  campi_specifici.append('ck_spec')
  
  # Periodo in un estremo
  tar_spec_gen_estremo = tar_spec_gen.filter(f.col('ck_spec')==1) \
                             .withColumn('datefrom', f.when(f.col('datefrom')>=f.col('spec_datefrom'), f.date_add(f.col('spec_dateto'),1) ).otherwise( f.col('datefrom') ) ) \
                             .withColumn('dateto', f.when(f.col('dateto')<=f.col('spec_dateto'), f.date_add(f.col('spec_datefrom'),-1) ).otherwise( f.col('dateto') ) ) \
                             .drop(*[i for i in campi_specifici])
  # Periodo interno - Parte post
  tar_spec_gen_interno = tar_spec_gen.filter(f.col('ck_spec')==2) \
                             .withColumn('datefrom', f.date_add(f.col('spec_dateto'),1) ) \
                             .withColumn('dateto', f.col('dateto') )
  # Periodo interno - Parte pre
  tar_spec_gen_interno = tar_spec_gen_interno.unionByName(\
                             tar_spec_gen.filter(f.col('ck_spec')==2) \
                                 .withColumn('datefrom', f.col('datefrom') ) \
                                 .withColumn('dateto', f.date_add(f.col('spec_datefrom'),-1) )\
                                                         ).drop(*[i for i in campi_specifici])
  
  tar2_completo = tar2.unionByName(tar_spec_gen_estremo).unionByName(tar_spec_gen_interno)
  
  # Unisco le parti in un unico tariffario
  tar = customUnion(tar1,tar2_completo).withColumnRenamed('formulaprz','formulaprz_orig')

  # Escludo coditem in base alle decodifiche
  coditem_esclusi = decodifiche.filter(f.col('tipo') == 'TAR_XE_ESCLUSIONI').select(f.col('id').alias('coditem_esclusioni') ).dropDuplicates()
  tar = tar.join(coditem_esclusi,[tar.coditem == coditem_esclusi.coditem_esclusioni],'left_anti')

  # Preparo il controllo in caso di disallinamenti fra anagrafica e tariffe o di coerenza date
  tar_disallineamenti = tar.select('commodity','codinstallation','codpromotion','codpromotionversion','datefrom','dateto','codcontracttype').dropDuplicates()
  
  # Join con le forniture
  tar = tar.join(forn,[tar.commodity == forn.forn_commodity, \
                       tar.codinstallation == forn.cod_pratica, \
                       tar.codpromotion == forn.id_offerta, \
                       tar.codpromotionversion == forn.id_listino
                      ]).drop(forn.forn_commodity)
  tar = tar.withColumn('datefrom',f.greatest('datefrom','data_decorrenza' ) )
  tar = tar.withColumn('dateto',f.least('dateto','data_cessazione' ) )
  tar = tar.filter( f.greatest(f.col('datefrom'),f.col('dateto')) == f.col('dateto')  ) 
  tar = tar.withColumn('ck_codcontracttype',f.when( f.col('codcontracttype')== f.col('forn_codcontracttype') , f.lit(True)).otherwise(f.lit(False) ) )
  tar = tar.filter(f.col('ck_codcontracttype')== True )
  
  # Produco e scrivo il controllo dei disallinamenti fra anagrafica e tariffe o di coerenza date
  tar_no_disal = tar.selectExpr('codinstallation as tar_no_disal_cod','commodity as tar_no_disal_com').dropDuplicates()
  tar_disallineamenti = tar_disallineamenti.join(forn.select('forn_commodity','cod_pratica','id_offerta','id_listino','data_decorrenza','data_cessazione','forn_codcontracttype'),\
                                                 [tar.commodity == forn.forn_commodity, \
                                                  tar.codinstallation == forn.cod_pratica \
                                                 ]).drop(forn.forn_commodity)
  tar_disallineamenti = tar_disallineamenti.join(tar_no_disal,\
                                                 [tar_disallineamenti.codinstallation == tar_no_disal.tar_no_disal_cod ,\
                                                  tar_disallineamenti.commodity == tar_no_disal.tar_no_disal_com],'left_anti')
  tar_disallineamenti = tar_disallineamenti.withColumn('id_run_cons',f.lit(id_run)).withColumn('data_inserimento',f.lit(datetime.now()))
  tar_disallineamenti.write.format('parquet').saveAsTable("lab1_db.rfcf_tariffario_xe_controllo_disallineamenti", mode = "append")
  
  # Aggancio i valori di hec e dist_rng
  w_let = window.Window.partitionBy(f.col('codremi')).orderBy(f.col('edl_last_modify').desc()) 
  distrng = distrng.withColumn('edl_last_modify', f.to_date('edl_last_modify', 'yyyy-MM-dd')).withColumn('edl_last_modify', f.to_timestamp('edl_last_modify', 'yyyy-MM-dd')) \
                 .withColumn('rank', f.rank().over(w_let)).filter(f.col('rank')==1).drop('edl_last_modify','rank').dropDuplicates()
  distrng = distrng.withColumn('dist_rng',f.regexp_replace('dist_rng_tmp','>15','16'))\
                   .withColumn('dist_rng',f.regexp_replace('dist_rng',',','.').cast('double'))
  w_let = window.Window.partitionBy(f.col('codinstallation_hec')).orderBy(f.col('edl_last_modify').desc()) 
  hec = hec.withColumn('edl_last_modify', f.to_date('edl_last_modify', 'yyyy-MM-dd')).withColumn('edl_last_modify', f.to_timestamp('edl_last_modify', 'yyyy-MM-dd')) \
             .withColumn('rank', f.rank().over(w_let)).filter(f.col('rank')==1).drop('edl_last_modify','rank').dropDuplicates()
  hec = hec.withColumn('dateto_hec',f.coalesce(f.col('dateto_hec'),f.lit(datetime(9999,12,31)) ) )
  hec = hec.filter( ( f.col('dateto_hec')>=data_inizio_anno ) & ( f.col('datefrom_hec')<data_fine_anno ) )
  # inseriamo il valore di default della hec per quelle forniture che presentano un valore nell'anno
  #   che non copre tutto il periodo
  hec_periodi = hec.groupBy('codinstallation_hec').agg(f.min('datefrom_hec').alias('datefrom_hec_min'),f.max('dateto_hec').alias('dateto_hec_max'))
  hec = hec.unionByName( hec_periodi.filter( ( f.greatest( f.col('datefrom_hec_min') , f.lit(data_inizio_anno) ) > data_inizio_anno ) )\
                                    .withColumn('datefrom_hec', f.lit(data_inizio_anno)).withColumn('dateto_hec',f.date_add(f.col('datefrom_hec_min'),-1).cast('timestamp'))\
                                    .withColumn('hec', f.lit('0') ).drop('datefrom_hec_min','dateto_hec_max') )
  hec = hec.unionByName( hec_periodi.filter( ( f.least( f.col('dateto_hec_max'), f.lit(data_fine_anno) ) < data_fine_anno ) )\
                                    .withColumn('dateto_hec', f.lit(data_fine_anno)).withColumn('datefrom_hec',f.date_add(f.col('dateto_hec_max'),1).cast('timestamp'))\
                                    .withColumn('hec', f.lit('0') ).drop('datefrom_hec_min','dateto_hec_max') )

  tar = tar.join(distrng,[distrng.codremi == tar.cod_remi, tar.commodity == 'GAS'],'left').drop(distrng.codremi)
  tar = tar.withColumn('dist_rng', f.when(f.col('commodity')=='GAS',f.coalesce(f.col('dist_rng'),f.lit(0))).otherwise(f.col('dist_rng'))) # DIST_RNG default 0
  tar = tar.join(hec,[tar.commodity == 'PWR', tar.codinstallation == hec.codinstallation_hec],'left').drop(hec.codinstallation_hec)
  tar = tar.withColumn('hec', f.when(f.col('commodity')=='PWR',f.coalesce(f.col('hec'),f.lit('0'))).otherwise(f.col('hec')))
  tar = tar.withColumn('datefrom',f.when(f.col('commodity')=='PWR',f.greatest('datefrom',f.coalesce(f.col('datefrom_hec'),f.lit(datetime(1900,1,1))) )).otherwise(f.col('datefrom')) )
  tar = tar.withColumn('dateto',f.when(f.col('commodity')=='PWR',f.least('dateto',f.coalesce(f.col('dateto_hec'),f.lit(datetime(9999,12,31)))  )).otherwise(f.col('dateto')) )
  tar = tar.filter( f.greatest(f.col('datefrom'),f.col('dateto')) == f.col('dateto')  ) 
                 
  return tar

def tariffario_xe_parte2(codifiche_tariffario,tar):
  """Metodo che gestisce le logiche della classe misuratore e delle fascie
  
  INPUT:  codifiche_tariffario -> dataframe
          tar -> dataframe
  
  OUTPUT: tar --> dataframe
  
  """  

  # Classe misuratore
  clmis = codifiche_tariffario.filter( (f.col('sist_fat') == 'XE') & (f.col('foglio') == 'VALORI_CODIFICA') & (f.col('tog_valore')=='CLS_MIS') )\
                              .select(f.col('valore_tcr').alias('gruppo_misuratore'),f.col('asse_valore').alias('classe_misuratore'))
  classi = clmis.select('gruppo_misuratore').dropDuplicates().collect()
  classi = [* (i[0] for i in classi)]
  classe_mis_aggancio = f.udf(lambda x,y: y if y in x else 'NO')

  tar = tar.withColumn('flg_presenza_classe',f.when(f.col('pricelistdescription').rlike('({})'.format('|'.join(classi))), True)\
                                   .otherwise(False))
  tar = tar.join(clmis,'classe_misuratore','left')
  tar = tar.withColumn('grupppo_misuratore_componente',classe_mis_aggancio(f.col('pricelistdescription'),f.coalesce(f.col('gruppo_misuratore'),f.lit('Classe Misuratore NON Presente')) )  )
  tar = tar.withColumn('flg_cls_mis',f.coalesce( f.when(f.col('flg_presenza_classe')==False,f.lit(True) )\
                             .otherwise(f.coalesce( f.col('grupppo_misuratore_componente')==f.col('gruppo_misuratore'),f.lit(False) ) ), f.lit(True) )  )  
  tar = tar.filter(f.col('flg_cls_mis')== True )

  # Fascia PWR
  codifica_fasce = codifiche_tariffario.filter( (f.col('sist_fat') == 'XE') & (f.col('foglio') == 'VALORI_CODIFICA') &\
                                               (f.col('tog_valore').isin(['FASCE','CON_FAS']) ) & (f.col('commodity') == 'EE' ) )\
                                       .select(f.col('tog_valore'),f.col('valore_tcr').alias('input'),f.col('asse_valore').alias('output'))
  lista_fasce = codifica_fasce.filter(f.col('tog_valore')=='FASCE').select('input').collect()
  lista_fasce = [* (i[0] for i in lista_fasce)]
  
  tar = tar.withColumn('fascia_xe',f.coalesce(  *( f.when(f.col('coditem').rlike('({})'.format(i)), f.lit(i)) for i in lista_fasce ) )  )
  tar = tar.join(codifica_fasce.select(f.col('input').alias('fascia_xe'),f.col('output').alias('fascia_tcr') ),'fascia_xe','left')
  tar = tar.join(codifica_fasce.select(f.col('input').alias('codcountertype'),f.col('output').alias('tipo_fascia') ),'codcountertype','left')
  tar = tar.withColumn('ck_fasce',f.when(f.col('fascia_xe').isNull(), True)\
                           .when( (f.col('tipo_fascia') == 'Mono') & (f.col('fascia_xe').isNotNull() ), f.col('fascia_tcr') == 'F0' ).otherwise(f.col('fascia_tcr') != 'F0')  )
  tar = tar.filter(f.col('ck_fasce')== True) 
  
  return tar

def extra_sistema (tar,codifiche_tariffario, map_indici):
  """Metodo che aggiorna le formule con le parti extra sistema
  
  INPUT:  tar -> dataframe
          codifiche_tariffario -> dataframe
          map_indici -> dataframe
  
  OUTPUT: tar --> dataframe
  
  """
  tar = tar.withColumn('coditem_no_iva', f.regexp_replace(f.col('coditem'),'_VAT10','' ) )\
           .withColumn('coditem_no_iva', f.regexp_replace(f.col('coditem_no_iva'),'_VAT5','' ) )\
           .withColumn('coditem_no_iva', f.regexp_replace(f.col('coditem_no_iva'),'_VAT05','' ) )

  # extra formula standard #
  tar = tar.join(codifiche_tariffario.filter( (f.col('foglio') == 'VALORI_CODIFICA')& (f.col('sist_fat') == 'XE' ) \
                         & (f.col('tog_valore') == 'EXTRA_FORM' ) & (f.col('campo_tcr_src') == 'NORMALE' ) ) \
                   .select(f.col('valore_tcr').alias('coditem_no_iva'),f.col('asse_valore').alias('new_form') ),\
                   'coditem_no_iva','left')
  tar = tar.withColumn('formulaprz_extra', f.when(f.col('new_form').isNotNull(), f.concat(f.col('formulaprz_orig'),f.col('new_form') ))\
                    .otherwise(f.col('formulaprz_orig')) ).drop('new_form')

  # extra formula K_indice #
  map_doppia_ind = map_indici.withColumn('ind_doppia_ind',f.concat(f.lit('K_'),f.col('ind_indice')) )
  map_doppia_ind = map_doppia_ind.withColumn('pos_', f.expr("if(substring(ind_ci,1,5)='PERD_',locate('_',ind_ci,6),locate('_',ind_ci))" )  )
  udf_doppia_ind = f.udf(lambda x,y: x[0:y], StringType())
  map_doppia_ind = map_doppia_ind.withColumn('ind_ci_fix',udf_doppia_ind('ind_ci','pos_') ).withColumn('ind_ci_fix',f.concat(f.col('ind_ci_fix'),f.lit('FIX')))

  map_doppia_ind = map_doppia_ind.selectExpr("ind_ci as ind_doppia_ci","ind_doppia_ind","'INDICE_VAR' as ind_doppia_tipo","'K_indice_var' as ind_doppia_var") \
                   .unionByName( map_doppia_ind.selectExpr("ind_ci_fix as ind_doppia_ci","'K_IND_FIX' as ind_doppia_ind","'INDICE_FIX' as ind_doppia_tipo","'K_indice_fix' as ind_doppia_var") )\
                   .unionByName( map_doppia_ind.selectExpr("ind_ci_fix as ind_doppia_ci","'K_IND_FIX' as ind_doppia_ind","'INDICE_FIX' as ind_doppia_tipo","'K_indice_fix' as ind_doppia_var") )\
                   .dropDuplicates()
  map_doppia_ind = map_doppia_ind.join(codifiche_tariffario.filter( (f.col('foglio') == 'VALORI_CODIFICA')& (f.col('sist_fat') == 'XE' ) \
                                    & (f.col('tog_valore') == 'EXTRA_FORM' ) & (f.col('campo_tcr_src') == 'DOPPIA_INDICIZZAZIONE' ) ) \
                               .selectExpr("valore_tcr as ind_doppia_tipo","asse_valore as new_form" ),\
                    'ind_doppia_tipo','left')
  map_doppia_ind = map_doppia_ind.withColumn('new_form',f.expr("regexp_replace(new_form,ind_doppia_var,ind_doppia_ind)") )\
                   .drop('ind_doppia_tipo','ind_doppia_ind','ind_doppia_var')

  tar = tar.withColumnRenamed('formulaprz_extra','formulaprz_extra_tmp')				
  tar = tar.join(map_doppia_ind,[tar.coditem_no_iva == map_doppia_ind.ind_doppia_ci ],'left')

  # extra valori #
  extra_valori = codifiche_tariffario.filter( (f.col('foglio') == 'VALORI_CODIFICA')& (f.col('sist_fat') == 'XE' ) \
                         & (f.col('tog_valore') == 'EXTRA_FORM' ) & (f.col('campo_tcr_src') == 'CAMBIO_VALORE' ) )\
                   .select(f.col('valore_tcr').alias('campo'),f.col('asse_valore').alias('new_val'),f.col('campo_tcr_dest').alias('filtro')  )
  extra_valori = extra_valori.select('campo','new_val','filtro',f.row_number().over(window.Window.partitionBy().orderBy(extra_valori.campo,extra_valori.filtro)).alias("row_num") )
  lista_extra_valori_campo = extra_valori.select(f.collect_list('campo') ).collect()[0][0]
  lista_extra_valori_row_number = extra_valori.select(f.collect_list('row_num') ).collect()[0][0]

  extra_valori = extra_valori.groupBy('filtro','row_num').pivot('campo').agg(f.first('new_val'))
  
  for i in lista_extra_valori_row_number:
    tar = tar.withColumnRenamed(lista_extra_valori_campo[i-1],lista_extra_valori_campo[i-1]+"_tmp")
    tar = tar.crossJoin(extra_valori.filter(f.col('row_num')==i).select(f.col(lista_extra_valori_campo[i-1]) ) )
    filtro = extra_valori.filter(f.col('row_num')==i).select('filtro').collect()[0][0]
    tar = tar.withColumn(lista_extra_valori_campo[i-1],f.when(eval(filtro), f.col(lista_extra_valori_campo[i-1])).otherwise(f.col(lista_extra_valori_campo[i-1]+"_tmp")) )
    tar=tar.drop(lista_extra_valori_campo[i-1]+"_tmp")
    
  # inserisco extra_formula_K_indice #
  
  tar = tar.withColumn('formulaprz_extra',f.when(f.col('new_form').isNotNull(), f.concat(f.col('formulaprz_extra_tmp'),f.col('new_form') ))\
                                                    .otherwise(f.col('formulaprz_extra_tmp')) ).drop('new_form','ind_doppia_ci','formulaprz_extra_tmp')

  tar = tar.drop('coditem_no_iva')
  
  return tar


def recupero_indici_fatturazione (variabili, indici):
  """Metodo che rileva e restituisce gli indici non presenti nelle variabili xe
  
  INPUT:  variabili -> dataframe
          indici -> dataframe
          
  OUTPUT: indici_new --> dataframe
  
  """
  
  indici = indici.withColumn('variabili_xe',f.upper(f.col('variabili_xe')))
  
  var_indici = variabili.join(indici.select('variabili_xe','ind_commodity').distinct(),\
                              [variabili.codvariable == indici.variabili_xe, variabili.var_commodity == indici.ind_commodity ])\
                        .drop('variabili_xe','ind_commodity')
  # aggiungo riga con ultimo annomese anno competenza precedente, serve per inizio dell'anno 
  var_indici = var_indici.unionByName(var_indici.filter(f.col('flg_generico')==1)
                                                .withColumn('enddate',f.date_add('startdate',-1).cast('timestamp'))\
                                                .withColumn('startdate',f.add_months('startdate',-1).cast('timestamp'))\
                                                .withColumn('flg_generico',f.lit(0))\
                                      )
  var_indici = var_indici.filter(f.col('flg_generico')==0)
  var_indici = var_indici.withColumn('annomese',f.format_string('%s%02d',f.year('enddate'),f.month('enddate') ))
  var_indici_old = var_indici.groupBy('var_commodity','codvariable','var_codinstallation').agg(f.max(f.col('annomese').cast('int') ).cast('string').alias('annomese_var') )
  indici_new = indici.join(var_indici_old, \
                           [var_indici_old.codvariable == indici.variabili_xe, var_indici_old.var_commodity == indici.ind_commodity]\
                           ,'left')
  indici_new = indici_new.filter(f.col('annomese_var').isNotNull() ) # elimino indici mai presenti nelle variabili
  indici_new = indici_new.filter(f.col('annomese_var').cast('int')<f.col('annomese').cast('int') )
  
  indici_new = indici_new.select('var_codinstallation','codvariable','var_commodity','valore','annomese')\
                         .withColumn('flg_generico', f.lit(0) )\
                         .withColumnRenamed('valore', 'variablevalue')\
                         .withColumn('startdate',f.from_unixtime(f.unix_timestamp('annomese', 'yyyyMM')).cast('timestamp'))\
                         .withColumn('enddate',f.last_day(f.from_unixtime(f.unix_timestamp('annomese', 'yyyyMM'))).cast('timestamp') ).drop('annomese')
  
  return indici_new


def recupero_doppia_indicizzazione_fissa (variabili, map_indici):
  """Metodo che rileva e restituisce gli indici non presenti nelle variabili xe
  
  INPUT:  variabili -> dataframe
          map_indici -> dataframe
          
  OUTPUT: indici_doppia_fissa --> dataframe
  
  """
  map_doppia_ind = map_indici.withColumn('ind_doppia_ind',f.concat(f.lit('K_'),f.col('ind_indice')) )
  map_doppia_ind = map_doppia_ind.withColumn('ind_doppia_ind',f.upper(f.col('ind_doppia_ind')))
  map_doppia_ind = map_doppia_ind.select( f.collect_list('ind_doppia_ind') ).collect()[0][0]
    
  indici_doppia_fissa_tmp = variabili.filter( f.col('codvariable').isin(map_doppia_ind) )
  indici_doppia_fissa = indici_doppia_fissa_tmp.filter( f.col('var_codinstallation')!='####').withColumn('codvariable', f.lit('K_IND_FIX'))\
								 .withColumn('variablevalue', f.lit(1)-f.col('variablevalue') )
  indici_doppia_fissa = indici_doppia_fissa.unionByName(indici_doppia_fissa_tmp.filter( f.col('var_codinstallation')=='####')\
                                                          .withColumn('codvariable', f.lit('K_IND_FIX'))\
								                                          .withColumn('variablevalue', f.lit(1)-f.col('variablevalue') ).dropDuplicates() )
  
  return indici_doppia_fissa


def aggancio_variabili(tar,codifiche_tariffario,variabili,indici,map_indici,data_inizio_anno,data_fine_anno,spark):
  """Metodo che valorizza le variabili, fornitura per fornitura e formula per formula
  
  INPUT:  tar -> dataframe
          codifiche_tariffario -> dataframe
          variabili -> dataframe
          data_inizio_anno -> datetime
          data_fine_anno -> datetime
  
  OUTPUT: tar --> dataframe
  
  """

  tar = tar.selectExpr("*", "upper(formulaprz_extra) formulaprz").withColumn('formulaprz', f.regexp_replace('formulaprz',' ',''))
  
  tar = tar.withColumn('formulaprz', f.regexp_replace('formulaprz','=','=='))
  tar = tar.withColumn('formulaprz', f.regexp_replace('formulaprz','<==','<='))
  tar = tar.withColumn('formulaprz', f.regexp_replace('formulaprz','>==','>='))

  formule_repl = codifiche_tariffario.filter( (f.col('sist_fat') == 'XE') & (f.col('foglio') == 'VALORI_CODIFICA') & (f.col('tog_valore') == 'REPL_FORM' ) ) \
                                     .select(f.col('valore_tcr').alias('formula_from'),f.col('asse_valore').alias('formula_to'),f.col('campo_tcr_dest').cast('int').alias('order') ) \
                                     .orderBy('order')
  formula_from = formule_repl.select('formula_from').collect()
  formula_to = formule_repl.select('formula_to').collect()
  
  formula_from = [*(i[0].replace('(',r'\(').replace(')',r'\)').replace('+',r'\+').replace('-',r'\-').replace('==',r'\==').replace('*',r'\*') for i in formula_from) ]
  formula_to = [*(i[0].replace('(',r'\(').replace(')',r'\)').replace('+',r'\+').replace('-',r'\-').replace('==',r'\==').replace('*',r'\*') for i in formula_to) ]          
  i=0
  for form in formula_from:
    tar = tar.withColumn('formulaprz', f.regexp_replace('formulaprz',form , formula_to[i]))
    i +=1

  tar = tar.withColumn('formulaprz_extra', f.col('formulaprz') )
  tar = tar.withColumn('formula_new', f.split('formulaprz', '#')).drop('formulaprz')

  extract= f.udf(lambda x: var_extract(x), ArrayType(ArrayType(StringType())) )
  replace= f.udf(lambda x: var_replace(x), StringType())
  tar = tar.withColumn('var_lista', extract('formula_new'))
  tar = tar.withColumn('var_tmp', f.explode('var_lista'))
  tar = tar.withColumn('rank_var', f.col('var_tmp')[1].cast('int') ).withColumn('var', f.col('var_tmp')[0] ).drop('var_tmp')
  tar = tar.withColumn('formula_new', replace('formula_new'))
  tar = tar.withColumn('value', f.lit(None))
  tar = tar.withColumn('startdate_tmp', f.lit(None))
  tar = tar.withColumn('enddate_tmp', f.lit(None))

  # Carico le variabili
  variabili = variabili.withColumn('flg_generico',f.when( (f.col('var_codinstallation') == f.lit('####') ) & (f.col('enddate').isNull() ) & (f.col('startdate').isNull() ) \
                                                , f.lit(1)).otherwise(f.lit(0)))
  variabili = variabili.withColumn('enddate',f.coalesce(f.col('enddate'),f.lit(datetime(9999,12,31)) ) )\
                       .withColumn('startdate',f.coalesce(f.col('startdate'),f.lit(data_inizio_anno) ) )\
                       .filter( f.col('enddate') >= f.col('startdate') )
  variabili = variabili.filter( (f.col('enddate')>=f.lit(data_inizio_anno) ) & (f.col('startdate')<=f.lit(data_fine_anno) ) )
  variabili = variabili.filter(~(f.col('startdate')==f.col('enddate')) | (f.col('enddate').isNull() ) )
  
  # Gestione K_indici:
  #   nel futuro - in teoria dovrebbero essere sempre valorizzati, più per non spaccare il tariffario nei periodi post consuntivo
  #   nel passato - possibile errore inserimento nella tabella variabile e corretta nostra gestione poichè extra sistema
  tar_k_indici = tar.filter( (f.col('var').like('K_%')) & ( f.col('var')!='K_IND_FIX' ) & ( f.col('var')!='K_MIG' ) )
  tar_k_indici = tar_k_indici.groupBy('codinstallation').agg(f.min('datefrom').alias('datefrom_k_indice'))

  variabili_k_indici = variabili.filter( f.col('codvariable').like('K_%') )
  variabili_k_indici_spec_base = variabili_k_indici.filter(f.col('var_codinstallation')!='####' )
  
  # futuro
  variabili_k_indici_spec = variabili_k_indici_spec_base.groupBy(f.col('var_codinstallation').alias('var_codinst'),f.col('var_commodity').alias('var_com'))\
                                                      .agg( f.max('enddate').alias('max_enddate') )
  variabili_k_indici_spec = variabili_k_indici_spec.filter( ( f.col('max_enddate') >= f.lit(data_inizio_anno) ) & ( f.col('max_enddate')<f.lit(data_fine_anno) ) )
  cond_k_indici_spec = [variabili_k_indici.var_codinstallation == variabili_k_indici_spec.var_codinst, variabili_k_indici.var_commodity == variabili_k_indici_spec.var_com,\
                        variabili_k_indici.enddate == variabili_k_indici_spec.max_enddate ]
  variabili_k_indici = variabili_k_indici.join(variabili_k_indici_spec , cond_k_indici_spec ,'left').drop('var_codinst','var_com')
  variabili_k_indici = variabili_k_indici.withColumn('enddate', f.when( f.col('max_enddate').isNotNull(), f.lit(data_fine_anno) ).otherwise(f.col('enddate') ))\
                                         .drop('max_enddate')
    
  # passato
  variabili_k_indici_spec = variabili_k_indici_spec_base.groupBy(f.col('var_codinstallation').alias('var_codinst'),f.col('var_commodity').alias('var_com'))\
                                                      .agg( f.min('startdate').alias('min_startdate') )
  cond_k_indici_spec = [variabili_k_indici.var_codinstallation == variabili_k_indici_spec.var_codinst, variabili_k_indici.var_commodity == variabili_k_indici_spec.var_com,\
                        variabili_k_indici.startdate == variabili_k_indici_spec.min_startdate ]  
  variabili_k_indici = variabili_k_indici.join(variabili_k_indici_spec , cond_k_indici_spec ,'left').drop('var_codinst','var_com')
  variabili_k_indici = variabili_k_indici.join(tar_k_indici, \
                                               [tar_k_indici.codinstallation == variabili_k_indici.var_codinstallation, variabili_k_indici.min_startdate.isNotNull()  ],\
                                               'left' ).drop('codinstallation')
  variabili_k_indici = variabili_k_indici.withColumn('startdate', f.when( (f.col('datefrom_k_indice')<f.col('startdate')) & (f.col('min_startdate').isNotNull() ),\
                                                                          f.col('datefrom_k_indice') ).otherwise(f.col('startdate') ))\
                                         .drop('min_startdate','datefrom_k_indice')
  
  # unisco variabili k_indici e non 
  variabili_no_k_indici = variabili.filter( ~(f.col('codvariable').like('K_%') ) )
  variabili = variabili_no_k_indici.unionByName(variabili_k_indici)
  
  indici_new = recupero_indici_fatturazione(variabili, indici)
  indici_doppia_fissa = recupero_doppia_indicizzazione_fissa(variabili, map_indici)
  
  variabili = variabili.unionByName(indici_new).unionByName(indici_doppia_fissa)
  
  # ottimizzo le join
  #rand_udf = f.udf(lambda: random.randint(0, 9), IntegerType()).asNondeterministic()
  #tar = tar.withColumn('salt', rand_udf())
  #tar = tar.withColumn('row_num',f.row_number().over(window.Window.partitionBy()
  #                                             .orderBy(tar.commodity,tar.codinstallation,tar.coditem,tar.datefrom)) )
  #tar = tar.withColumn('salt',f.col('row_num')%f.lit(10) ).drop('row_num')
  df_salt = spark.range(0, 10)
  df_salt = df_salt.withColumnRenamed('id', 'var_salt').withColumn('var_salt', f.col('var_salt').cast('int'))
  variabili = variabili.crossJoin(df_salt)
  
  # Aggancio variabili a tariffario
  colonne_variabili = variabili.columns

  #         variabili codificate
  codifica_variabili = codifiche_tariffario.filter( (f.col('sist_fat') == 'XE') & (f.col('foglio') == 'VALORI_CODIFICA') & (f.col('tog_valore') == 'VARIABILI' ) )\
                                           .select(f.col('valore_tcr').alias('variabili'),f.col('asse_valore').alias('valore')\
                                                   ,f.col('campo_tcr_src').alias('tipo_di_codifica'))
  tar = tar.withColumn('value',f.when(f.col('var') == 'DATEINVOICE', f.lit( f.unix_timestamp(None) ) ) )
  tar = tar.join(f.broadcast(codifica_variabili),[tar.var == codifica_variabili.variabili, \
                                     ~(f.col('tipo_di_codifica').isin(['A','D'])) |(f.col('tipo_di_codifica').isNull()) ] ,'left')\
           .withColumn('value',f.coalesce(f.col('value'),f.col('valore') ) ).drop('valore','variabili','tipo_di_codifica')
  tar = tar.join( f.broadcast(codifica_variabili),[tar.var == codifica_variabili.variabili, \
                  (f.col('tipo_di_codifica').isin(['A','D']))] ,'left')
  lista_var = codifica_variabili.filter( (codifica_variabili.tipo_di_codifica == f.lit('A')) |\
                                     (codifica_variabili.tipo_di_codifica == f.lit('D')) ).select('valore').collect()
  lista_var = [* (i[0] for i in lista_var)]
  tar = tar.withColumn('valore',f.coalesce(  *( f.when(f.col('valore') == i, f.col(i)) for i in lista_var ) )  )
  tar = tar.withColumn('value',f.coalesce(f.col('value'),f.col('valore') ) )\
           .withColumn('startdate_tmp',f.when( f.col('value').isNotNull() ,f.col('datefrom') ))\
           .withColumn('enddate_tmp',f.when( f.col('value' ).isNotNull() ,f.col('dateto')) ).drop('valore','variabili','tipo_di_codifica')

  #         per fornitura
  cond_var_forn = [tar.var == variabili.codvariable, tar.codinstallation == variabili.var_codinstallation,\
                  (~( variabili.enddate<tar.datefrom ) & (variabili.startdate<tar.dateto ) ) | (~( variabili.startdate>tar.dateto ) & (variabili.enddate>tar.datefrom ) ), \
                  tar.value.isNull(), tar.commodity == variabili.var_commodity, tar.salt == variabili.var_salt]
  tar = tar.join(variabili,cond_var_forn,'left')\
                  .withColumn('value',f.coalesce(tar.value,variabili.variablevalue))\
                  .withColumn('startdate_tmp',f.coalesce(tar.startdate_tmp,variabili.startdate))\
                  .withColumn('enddate_tmp',f.coalesce(tar.enddate_tmp,variabili.enddate))
  tar = tar.drop(*(i for i in colonne_variabili)) 

  #         per zona
  cond_var_zona = [tar.var == variabili.codvariable, tar.ambito_xe == variabili.var_codinstallation,\
                  (~( variabili.enddate<tar.datefrom ) & (variabili.startdate<tar.dateto ) ) | (~( variabili.startdate>tar.dateto ) & (variabili.enddate>tar.datefrom ) ),\
                   tar.value.isNull(), tar.commodity == variabili.var_commodity, tar.salt == variabili.var_salt]
  tar = tar.join(variabili,cond_var_zona,'left')\
                  .withColumn('value',f.coalesce(tar.value,variabili.variablevalue))\
                  .withColumn('startdate_tmp',f.coalesce(tar.startdate_tmp,variabili.startdate))\
                  .withColumn('enddate_tmp',f.coalesce(tar.enddate_tmp,variabili.enddate))
  tar = tar.drop(*(i for i in colonne_variabili))   

  #         per regione
  cond_var_regione = [tar.var == variabili.codvariable, tar.regione_xe == variabili.var_codinstallation,\
                  (~( variabili.enddate<tar.datefrom ) & (variabili.startdate<tar.dateto ) ) | (~( variabili.startdate>tar.dateto ) & (variabili.enddate>tar.datefrom ) ),\
                      tar.value.isNull(), tar.commodity == variabili.var_commodity, tar.salt == variabili.var_salt]
  tar = tar.join(variabili,cond_var_regione,'left')\
                  .withColumn('value',f.coalesce(tar.value,variabili.variablevalue))\
                  .withColumn('startdate_tmp',f.coalesce(tar.startdate_tmp,variabili.startdate))\
                  .withColumn('enddate_tmp',f.coalesce(tar.enddate_tmp,variabili.enddate))
  tar = tar.drop(*(i for i in colonne_variabili))   

  #         generico
  tmp_var_0 = variabili.filter( (variabili.var_codinstallation == f.lit('####')) & (variabili.flg_generico == f.lit(0) ) )
  tmp_var_1 = variabili.filter( (variabili.var_codinstallation == f.lit('####')) & (variabili.flg_generico == f.lit(1) ) )
  tmp_var = tmp_var_0.selectExpr('var_commodity com','codvariable cvar','enddate')\
                      .groupBy('com','cvar').agg(f.max('enddate').alias('end_tmp'))
  tmp_var_1 = tmp_var_1.join( tmp_var,[tmp_var_1.var_commodity==tmp_var.com, \
                            tmp_var_1.codvariable==tmp_var.cvar],'left' ).drop('com','cvar')
  tmp_var_1 = tmp_var_1.withColumn('startdate', f.when(f.col('end_tmp').isNull(),f.col('startdate')).otherwise(f.date_add(f.col('end_tmp'),1).cast('timestamp') ) )\
                       .drop('end_tmp')
  tmp_var_0 = tmp_var_0.unionByName(tmp_var_1.filter(f.col('startdate')<f.date_add(f.lit(data_fine_anno),1)))
  cond_var_gen = [tar.var == tmp_var_0.codvariable,\
                  (~( tmp_var_0.enddate<tar.datefrom ) & ( tmp_var_0.startdate<tar.dateto ) )  | (~( tmp_var_0.startdate>tar.dateto ) & ( tmp_var_0.enddate>tar.datefrom )) ,\
                  tar.value.isNull(),  tar.commodity == tmp_var_0.var_commodity, tar.salt == variabili.var_salt]
  tar = tar.join(tmp_var_0,cond_var_gen,'left')\
                  .withColumn('value',f.coalesce(tar.value,variabili.variablevalue))\
                  .withColumn('startdate_tmp',f.coalesce(tar.startdate_tmp,variabili.startdate))\
                  .withColumn('enddate_tmp',f.coalesce(tar.enddate_tmp,variabili.enddate))
  tar = tar.drop(*(i for i in colonne_variabili))  

  # Creo data inizio e fine prezzo per componente
  tar = tar.withColumn('dateprz_ini_tmp',f.greatest(f.col('datefrom'),f.col('startdate_tmp')))\
           .withColumn('dateprz_end_tmp',f.least(f.col('dateto'),f.col('enddate_tmp')))
  dateprz = tar.selectExpr("commodity d_commodity","codinstallation d_codinstallation","coditem d_coditem"\
                      ,"codpricelist d_codpricelist","dateprz_ini_tmp dateprz_ini").dropDuplicates()
  windowsdf = window.Window.orderBy('d_commodity','d_codinstallation','d_coditem','d_codpricelist',f.desc('dateprz_ini') )\
                           .partitionBy('d_commodity','d_codinstallation','d_coditem','d_codpricelist') 
  dateprz = dateprz.withColumn('dateprz_end', f.date_add(f.lag('dateprz_ini').over(windowsdf),-1).cast('timestamp'))
  
  #ottimizzo la join
  df_salt = spark.range(0, 10)
  df_salt = df_salt.withColumnRenamed('id', 'd_salt').withColumn('d_salt', f.col('d_salt').cast('int'))
  dateprz = dateprz.crossJoin(df_salt)
  
  cond_date = [tar.commodity == dateprz.d_commodity, tar.codinstallation == dateprz.d_codinstallation,\
               tar.coditem == dateprz.d_coditem, tar.codpricelist == dateprz. d_codpricelist, \
               (tar.dateprz_ini_tmp <= dateprz.dateprz_ini)  | (tar.dateprz_end_tmp >= dateprz.dateprz_end),\
               (tar.dateprz_ini_tmp <= dateprz.dateprz_end) | (dateprz.dateprz_end.isNull() ),\
               (tar.dateprz_end_tmp >= dateprz.dateprz_ini),\
               (tar.salt == dateprz.d_salt)
              ]
  tar = tar.join(dateprz,cond_date,'left')
  tar = tar.withColumn('dateprz_ini', f.coalesce(f.col('dateprz_ini'),f.col('datefrom')) )
  tar = tar.withColumn('dateprz_end', f.coalesce(f.col('dateprz_end'),f.col('dateto')) )
  tar = tar.filter( f.greatest(f.col('dateprz_ini'),f.col('dateprz_end')) == f.col('dateprz_end')  ) 
  tar = tar.drop('d_commodity','d_codinstallation','d_coditem','d_salt')
  
  return tar

def applicazione_formule(tar):
  """Metodo che applica le formule fornitura per fornitura e componente per componente
  
  INPUT:  tar -> dataframe
  
  OUTPUT: tar --> dataframe
  
  """
  
  eval_form = f.udf(lambda x,y,z: ev(x,y,z))

  tar = tar.select('codinstallation','coditem','codpricelist','codsalesunit','codcontracttype','commodity','fascia_tcr','formulaprz_orig','formulaprz_extra',\
                   'quantityfrom', 'quantityto','formula_new','var_lista','var','rank_var', 'codpromotionversion', \
                   'value','dateprz_ini','dateprz_end' )
  windowsdf = window.Window.partitionBy('commodity','codinstallation','coditem','codpricelist','codpromotionversion','codcontracttype','quantityfrom','dateprz_ini').orderBy('rank_var')\
                           .rowsBetween(window.Window.unboundedPreceding,window.Window.unboundedFollowing )
  tar = tar.withColumn('value_var',f.collect_list('value').over(windowsdf) )
  tar = tar.withColumn('name_var',f.collect_list('var').over(windowsdf) )

  tar = tar.groupBy('codinstallation','coditem','codpricelist','codpromotionversion','codcontracttype','codsalesunit','commodity','fascia_tcr','formulaprz_orig','formulaprz_extra',\
                   'quantityfrom','quantityto','value_var','name_var','formula_new','dateprz_ini','dateprz_end','var_lista')\
           .agg(f.max('rank_var').alias('n_var'))
  coerenza = f.udf(lambda x: len(x), IntegerType())
  tar = tar.withColumn('ck_coerenza', ( (f.col('n_var')+1)==coerenza('value_var') ) & ( coerenza('var_lista')==coerenza('value_var') ) ).drop('var_lista')
  tar = tar.filter(f.col('ck_coerenza') == True)

  tar = tar.withColumn('price', eval_form('formula_new','value_var','name_var').cast('double') ) 
  tar = tar.select('codinstallation','coditem','codpricelist','codpromotionversion','codsalesunit','commodity','fascia_tcr','formulaprz_orig','formulaprz_extra',\
                   'quantityfrom','quantityto','price','dateprz_ini','dateprz_end' )
  return tar

def applicazione_formule_errori(tar):
  """Metodo che applica le formule fornitura per fornitura e componente per componente
  
  INPUT:  tar -> dataframe
  
  OUTPUT: tar --> dataframe
  
  """
  
  eval_form = f.udf(lambda x,y,z: ev(x,y,z))

  tar = tar.select('codinstallation','coditem','codpricelist','codsalesunit','codcontracttype','commodity','fascia_tcr','formulaprz_orig','formulaprz_extra',\
                   'quantityfrom', 'quantityto','formula_new','var_lista','var','rank_var', 'codpromotionversion', \
                   'value','dateprz_ini','dateprz_end' )
  windowsdf = window.Window.partitionBy('commodity','codinstallation','coditem','codpricelist','codpromotionversion','codcontracttype','quantityfrom','dateprz_ini').orderBy('rank_var')\
                           .rowsBetween(window.Window.unboundedPreceding,window.Window.unboundedFollowing )
  tar = tar.withColumn('value_var',f.collect_list('value').over(windowsdf) )
  tar = tar.withColumn('name_var',f.collect_list('var').over(windowsdf) )

  tar = tar.groupBy('codinstallation','coditem','codpricelist','codpromotionversion','codcontracttype','codsalesunit','commodity','fascia_tcr','formulaprz_orig','formulaprz_extra',\
                   'quantityfrom','quantityto','value_var','name_var','formula_new','dateprz_ini','dateprz_end','var_lista')\
           .agg(f.max('rank_var').alias('n_var'))
  coerenza = f.udf(lambda x: len(x), IntegerType())
  tar = tar.withColumn('ck_coerenza', ( (f.col('n_var')+1)==coerenza('value_var') ) & ( coerenza('var_lista')==coerenza('value_var') ) ).drop('var_lista')
  tar = tar.filter(f.col('ck_coerenza') == False)

  return tar
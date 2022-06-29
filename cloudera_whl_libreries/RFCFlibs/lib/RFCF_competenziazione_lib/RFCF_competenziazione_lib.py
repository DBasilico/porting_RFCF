from pyspark.sql.types import *
import pyspark.sql.functions as f

from udfs import *
from shared_lib import run_par,pro_die,decodifica_colonna


def comp_df(df,valore_in,inizio_competenza,fine_competenza,nome_col_inizio='start_date',nome_col_fine='end_date'):
    """Metodo per dividere l'intervallo di competenza in mesi.

    Input:  df                            --> dataframe di partenza
            valore_in                     --> nome colonna del valore input
            inizio_competenza             --> colonna data inizio competenza
            fine_competenza               --> colonna data fine competenza
            nome_col_inizio='start_date'   --> nome colonna StartDate di output
            nome_col_fine='end_date'       --> nome colonna EndDate di output

    Output: df                            --> dataframe con periodo di competenza esploso su più record
            """
    df = df.withColumn("SEGNO", f.concat(f.col('SEGNO'), f.lit(1).cast("double")))

    df = df.withColumn("VALORE", df.SEGNO * f.col(valore_in).cast("double"))

    df = df.withColumn('NUM_M', (f.month(fine_competenza) + (12 - f.month(inizio_competenza) + 1) + (f.year(fine_competenza) - f.year(inizio_competenza) - 1) * 12))

    compt = f.udf(lambda x, y, z: comp(x, y, z), StructType([StructField(nome_col_inizio, ArrayType(TimestampType())),
                                                             StructField(nome_col_fine, ArrayType(TimestampType()))]))
    

    df = df.withColumn("Dates", compt(inizio_competenza, fine_competenza, "NUM_M"))

    df = df.withColumn("temp_col", f.arrays_zip("Dates." + nome_col_inizio, "Dates."+ nome_col_fine)) \
      .withColumn("temp_col", f.explode("temp_col")) \
      .withColumn(nome_col_inizio,f.col("temp_col.0")) \
      .withColumn(nome_col_fine,f.col("temp_col.1")) \
      .drop("temp_col").drop('Dates')
      
    return df


def dett_fatt_comp(spark,commodity, startDateEmissione, endDateEmissione, par):
  """Metodo che competenzia i dettagli fattura
  Input:  spark       --> sessione spark
          commodity   --> commodity
  """

  time = datetime.now()
  
  df_decodifiche = spark.table('lab1_db.rfcf_decodifiche').filter(f.col('tipo')=='VOCI_CONSUMO') \
    .drop('id_run','id_2','valore_2','valore','tipo')
  
  df_voce = spark.sql("SELECT id_voce, codice_voce, descrizione_voce, cod_sistema_provenienza as cod_sistema_provenienza_voce FROM edl_ods.vw_dg_l1_voce where flag_ultimo_record_valido = 1")
  df_componente = spark.sql("SELECT id_componente, codice_componente, componente, cod_sistema_provenienza as cod_sistema_provenienza_componente FROM edl_ods.vw_dg_l1_componente where flag_ultimo_record_valido = 1")
  df_causale = spark.sql("SELECT id_causale, codice_causale, causale, cod_sistema_provenienza as cod_sistema_provenienza_causale FROM edl_ods.vw_dg_l1_causale where flag_ultimo_record_valido = 1")
  df_neta = spark.table('lab1_db.rfcf_decodifiche').filter(f.col('tipo')== 'SISTEMI') \
                      .selectExpr('id sist_fat_neta','valore_2 sistema_nome' )
  
  df_sottotestata = spark.sql("SELECT codice_sottotestata, modalita_di_fatturazione FROM edl_ods.vw_dg_l1_sottotestata where flag_ultimo_record_valido = 1") 

  df_saldo_acconto = spark.table('lab1_db.rfcf_decodifiche') \
    .filter(f.col('tipo')== 'SALDO_ACCONTO') \
    .selectExpr('id codice_causale', 'valore saldo_acconto' ) \
    .withColumn('modalita_di_fatturazione', f.lit('L'))
    
  tmp = spark.createDataFrame([[None, 'AC', 'M']],
                              schema =StructType([StructField('codice_causale', StringType()),StructField('saldo_acconto', StringType()),StructField('modalita_di_fatturazione', StringType())] ))
  df_saldo_acconto = df_saldo_acconto.unionByName(tmp)

  df_saldo_acconto_tl = spark.table('lab1_db.rfcf_decodifiche') \
    .filter(f.col('tipo')== 'SALDO_ACCONTO_TL') \
    .selectExpr('id codice_causale', 'id_2 codice_tipologia_lettura' ,'valore saldo_acconto' ) \
    .withColumn('modalita_di_fatturazione', f.lit('L'))


  df_dettaglio_fattura = spark.table('edl_ods.vw_dg_l1_dettaglio_fattura').filter(f.col('commodity').isin(commodity)) \
      .filter(f.col('data_emissione').between(startDateEmissione, endDateEmissione)) \
      .filter(f.col('flag_ultimo_record_valido') == 1).filter(f.year('inizio_periodo')>1)
      
  cond_voce = [df_dettaglio_fattura.codice_voce == df_voce.id_voce, df_dettaglio_fattura.cod_sistema_provenienza == df_voce.cod_sistema_provenienza_voce]
  
  cond_componente = [df_dettaglio_fattura.codice_componente == df_componente.id_componente, df_dettaglio_fattura.cod_sistema_provenienza == df_componente.cod_sistema_provenienza_componente]
  
  cond_causale = [df_causale.cod_sistema_provenienza_causale == df_dettaglio_fattura.cod_sistema_provenienza, df_dettaglio_fattura.codice_causale == df_causale.id_causale]
  
  cond_neta = [df_neta.sist_fat_neta == df_dettaglio_fattura.cod_sistema_provenienza]
  
  cond_sottotestata = [df_sottotestata.codice_sottotestata == df_dettaglio_fattura.codice_sotto_testata]

  df_dettaglio_fattura = df_dettaglio_fattura.join(f.broadcast(df_voce), cond_voce) \
      .join(f.broadcast(df_componente), cond_componente) \
      .join(f.broadcast(df_causale), cond_causale) \
      .join(f.broadcast(df_neta), cond_neta) \
      .join(df_sottotestata, cond_sottotestata)

  
  df = df_dettaglio_fattura.select(df_dettaglio_fattura.inizio_periodo.alias("DATA_INIZIO_COMPETENZA"),
                                   df_dettaglio_fattura.fine_periodo.alias("DATA_FINE_COMPETENZA"),
                                   df_dettaglio_fattura.imponibile_quota,
                                   df_dettaglio_fattura.segno,
                                   df_dettaglio_fattura.data_emissione,
                                   f.concat(df_voce.codice_voce, f.lit('#'), df_dettaglio_fattura.codice_quota_parte, f.lit('#'), df_componente.codice_componente, f.lit('#'), df_neta.sistema_nome).alias("tipo_riga"),
                                   df_dettaglio_fattura.commodity,
                                   f.concat(df_dettaglio_fattura.cod_sistema_provenienza, f.lit('#'), df_dettaglio_fattura.codice_sotto_testata).alias("FK_SOTTOTESTATA"),
                                   df_dettaglio_fattura.cod_sistema_provenienza.alias("SIST_FAT"),
                                   df_dettaglio_fattura.codice_dettaglio_fattura,
                                   df_dettaglio_fattura.codice_societa.alias("SOCIETA_ID"),
                                   df_dettaglio_fattura.codice_sotto_testata,
                                   df_dettaglio_fattura.codice_fornitura.alias("COD_PRATICA_CRM"),
                                   df_dettaglio_fattura.quantita,
                                   df_dettaglio_fattura.iva,
                                   df_dettaglio_fattura.codice_quota_parte,
                                   df_dettaglio_fattura.id_fornitura,
                                   df_dettaglio_fattura.flag_ultimo_record_valido,
                                   df_voce.id_voce,
                                   df_voce.codice_voce,
                                   df_voce.descrizione_voce,
                                   df_componente.id_componente,
                                   df_componente.codice_componente,
                                   df_componente.componente,
                                   df_causale.id_causale,
                                   df_causale.codice_causale,
                                   df_causale.causale,
                                   df_dettaglio_fattura.codice_tipologia_lettura,
                                   df_dettaglio_fattura.progressivo_caricamento,
                                   df_sottotestata.modalita_di_fatturazione
                                   )

  cond_saldo_acconto = ['modalita_di_fatturazione', 'codice_causale']
  cond_saldo_acconto_tl = ['modalita_di_fatturazione', 'codice_causale', 'codice_tipologia_lettura']

  df_join = df.join(f.broadcast(df_saldo_acconto), cond_saldo_acconto, 'leftouter')
  df_join = df_join.filter(df_join.saldo_acconto != 'null').unionByName( \
      df_join.filter(df_join.saldo_acconto.isNull()).join(f.broadcast(df_saldo_acconto_tl), cond_saldo_acconto_tl, 'leftouter') \
          .drop(df_join.saldo_acconto))

  MappingModFat = f.udf(lambda x: 'SA' if x == 'L' else 'AC')

  df = df_join.filter(df_join.saldo_acconto != 'null').unionByName( \
      df_join.filter(df_join.saldo_acconto.isNull()).withColumn("saldo_acconto", MappingModFat(df_join.modalita_di_fatturazione)))
  
  df_consumi = df
  
  df = df.drop('quantita')
  
  df_consumi = df_consumi.join(f.broadcast(df_decodifiche),df_decodifiche.id==df_consumi.codice_voce)
  
  df_consumi = df_consumi.withColumn('tipo_riga', f.lit('CONSUMO')) \
    .withColumn('codice_voce', f.lit('CONSUMO')) \
      .withColumn('descrizione_voce', f.lit('CONSUMO')) \
        .withColumn('id_voce', f.lit('CONSUMO')) \
          .withColumn('iva', f.lit(0)) \
            .withColumn('codice_quota_parte', f.lit(None)) \
              .withColumn('imponibile_quota', f.col('quantita')) \
                .withColumn('codice_componente', f.lit('CONSUMO')) \
                  .withColumn('componente', f.lit('CONSUMO')) \
                    .drop('quantita','id')
                
  df = df.unionByName(df_consumi)
  
  df = comp_df(df, valore_in='imponibile_quota', inizio_competenza="DATA_INIZIO_COMPETENZA", fine_competenza="DATA_FINE_COMPETENZA")

  df = pro_die(df, inizio_competenza="DATA_INIZIO_COMPETENZA", fine_competenza="DATA_FINE_COMPETENZA", inizio_mese='start_date', fine_mese='end_date')

  df = df.withColumn("TRIMESTRE", f.ceil(f.month(df.data_emissione) / 3))
  df = df.withColumn("ANNOMESE", f.concat(f.year(df.data_emissione).cast("string"), f.format_string("%02d", f.month(df.data_emissione))))

  df = df.withColumn('commodity', f.when(f.col('commodity').isin('ENERGIA ELETTRICA', 'Energia Elettrica'), 'EE') \
                     .when(f.col('commodity').contains('GAS'), 'GAS') \
                     .otherwise(f.col('commodity'))).drop('deltaGG', 'NUM_M', 'GiorniEffettivi')

  colonne = ['DATA_INIZIO_COMPETENZA', 
             'DATA_FINE_COMPETENZA', 
             'tipo_riga', 
             'commodity', 
             'FK_SOTTOTESTATA',
             'SIST_FAT',
             'codice_dettaglio_fattura', 
             'SOCIETA_ID', 
             'COD_PRATICA_CRM', 
             'iva', 
             'codice_quota_parte',
             'id_fornitura', 
             'codice_voce', 
             'descrizione_voce', 
             'codice_componente',
             'componente', 
             'id_causale', 
             'codice_causale', 
             'causale', 
             'codice_tipologia_lettura', 
             'VALORE',
             'start_date',
             'end_date', 
             'ANNO_COMP', 
             'MESE_COMP', 
             'VALORE_COMP', 
             'TRIMESTRE', 
             'ANNOMESE', 
             'modalita_di_fatturazione',
             'saldo_acconto', 
             'progressivo_caricamento', 
             'ANNOMESE_COMP',
             'data_emissione']
  
  df = df.select(colonne)
  df = df.withColumn('ID_RUN', f.lit(par.id_run))
  df.write.format("hive").saveAsTable("lab1_db.rfcf_dett_fatt_comp", mode='append', partitionBy=('annomese', 'commodity'))

def aggiornamento_borsellino(spark,last_extr):
    spark.sparkContext.setCheckpointDir('./checkpoints')
    df = decodifica_colonna(df = spark.table('edl_ods.vw_dg_l1_dettaglio_fattura'),
                            col_name = 'commodity',
                            map_dict = {'ENERGIA ELETTRICA':'EE','GAS METANO':'GAS',
                                        'GAS NATURALE':'GAS','Energia Elettrica':'EE'})
    df = df.filter(f.col('dg_last_modify')>last_extr).cache()
    
    if df.count() == 0:
      return None
    
    voci = dict()
    df_voce_gas = spark.sql('''
    SELECT DISTINCT v.map_codice_voce
    FROM edl_tcr.vw_tcr_fcf_mapping_tcr_gas m,
    edl_tcr.vw_tcr_fcf_mapping_voci_contabili_gas v
    WHERE m.data_snapshot = (SELECT max(data_snapshot) from edl_tcr.vw_tcr_fcf_mapping_tcr_gas)
    and v.data_snapshot = (SELECT max(data_snapshot) from edl_tcr.vw_tcr_fcf_mapping_voci_contabili_gas)
    and v.map_componente = m.map_componente
    and m.map_livello_1 = 'BORS'
    and v.map_codice_voce <> ''
    ''')
    voci['GAS'] = [x.map_codice_voce for x in df_voce_gas.collect()]
    del df_voce_gas
    
    df_voce_ee = spark.sql('''
    SELECT DISTINCT v.map_codice_voce
    FROM edl_tcr.vw_tcr_fcf_mapping_tcr_pwr m,
    edl_tcr.vw_tcr_fcf_mapping_voci_contabili_pwr v
    WHERE m.data_snapshot = (SELECT max(data_snapshot) from edl_tcr.vw_tcr_fcf_mapping_tcr_pwr)
    and v.data_snapshot = (SELECT max(data_snapshot) from edl_tcr.vw_tcr_fcf_mapping_voci_contabili_pwr)
    and v.map_componente = m.map_componente
    and m.map_livello_1 = 'BORS'
    and v.map_codice_voce <> ''
    ''')
    voci['EE'] = [x.map_codice_voce for x in df_voce_ee.collect()]
    del df_voce_ee
    
#    if len(set([x.commodity for x in df.select('commodity').distinct().collect()])-set(['EE','GAS','LAVORI']))>0:
#        print(set([x.commodity for x in df.select('commodity').distinct()]))
#        raise Exception('AGGIORNAMENTO BORSELLINO: ci sono decodifiche di commodity nuove')
    
    df_old_compBors = spark.table('lab1_db.rfcf_importi_borsellino')
    
    df_nuovi_importi = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema=df_old_compBors.schema)
    
    df_old_compBors = df_old_compBors.withColumnRenamed('importo','importo_old')
    
    for commodity in ['EE','GAS']:
        df_idVoce = spark.table('edl_ods.vw_dg_l1_voce')\
                         .filter(f.col('flag_ultimo_record_valido')==1)\
                         .filter(f.col('cod_sistema_provenienza')==3)\
                         .filter(f.col('codice_voce').isin(voci[commodity]))\
                         .select('id_voce',f.col('codice_voce').alias('codice_voce_finale')).distinct()
        
        df_comm =  df.filter(f.col('commodity')==commodity).join(f.broadcast(df_idVoce),on=[df.codice_voce==df_idVoce.id_voce],how='inner')\
                     .filter(f.col('flag_ultimo_record_valido')==1)\
                     .withColumn('annomese_emissione',f.concat(f.year(f.col('data_emissione')).cast(StringType()),
                                                      f.lpad(f.month(f.col('data_emissione')).cast(StringType()),2,'0') ))\
                     .withColumn('importo_con_segno', f.when(f.col('segno')=='-', -1*f.col('imponibile_quota')).otherwise(f.col('imponibile_quota')))\
                     .filter(~f.col('commodity').isNull())\
                     .groupBy(['codice_voce_finale','annomese_emissione','commodity']).agg(f.sum('importo_con_segno').alias('importo'))
        
        df_nuovi_importi = df_nuovi_importi.unionByName(df_comm)
        
    df_old_compBors = df_old_compBors.join(df_nuovi_importi,on=['codice_voce_finale','annomese_emissione','commodity'],how='fullouter')\
                                     .fillna(0,subset=['importo','importo_old']).checkpoint()
    
    df_old_compBors = df_old_compBors.withColumn('importo',f.col('importo_old')+f.col('importo')).drop('importo_old')
    
    df_old_compBors.write.mode('overwrite').format('parquet').saveAsTable('lab1_db.rfcf_importi_borsellino')
    
    return None
    
    
    
    
        

        
        
       


from datetime import timedelta, datetime
import calendar as cl
import pyspark.sql.functions as f
from pyspark.sql.types import *
from collections import OrderedDict
from pyspark.sql import Window
import random

from udfs import *
from shared_lib import pro_die
from lib import RFCF_canone_lib as tv
  

def intervalli_fatt(commodity, cadenza, lista):
  """Metodo che calcola gli intervalli di fatturazione data l'emissione.

  Input:  commodity     --> colonna commodity
          cadenza       --> colonna cadenza
          lista         --> colonna con lista date emissione

  Output: start_fattura --> colonna inizio periodo di fatturazione
          end_fattura   --> colonna fine periodo di fatturazione
          date_fattura  --> colonna data emissione
          """
  fm = "%d/%m/%Y"
  start_fattura = []
  end_fattura = []
  date_fattura = []
  lista= [x for x in lista if x!= '']

  if(commodity == 'EE' and cadenza == 'mensile'):
      for i in lista:
        date_fattura.append(datetime.strptime(i,fm))
        dateE = datetime.strptime(i,fm)
        dateE = dateE.replace(day = 1) - timedelta(1)
        end_fattura.append(dateE)
        dateS = dateE.replace(day = 1)
        start_fattura.append(dateS)
  else:
      for i in lista:
        date_fattura.append(datetime.strptime(i,fm))
        dateE = datetime.strptime(i,fm)
        if i == lista[0]:
          dateS = datetime.strptime('01/01/2020',fm)
          start_fattura.append(dateS)
        else:
          dateS = end_fattura[-1]
          dateS = dateS + timedelta(1)
          start_fattura.append(dateS)
        end_fattura.append(dateE)

        
  return  start_fattura, end_fattura, date_fattura 

################################################################################

def tcr_commodity(spark,commodity,data):
    """Metodo per caricare forniture e ricavi del tcr per commodity.

    Input:  spark         --> sparksession
            commodity     --> stringa con commodity

    Output: info_tcr      --> dataframe di join tra forniture e ricavi tcr
          """
    df_forniture_tcr = spark.sql("""SELECT
    fk_forn_fatt,
    cod_pratica_crm,
    cod_pratica,
    commodity,
    lower(mercato) mercato,
    codice_pdf,
    tipo_regime, 
    cod_cliente,
    pod_pdr
    FROM lab1_db.rfcf_vw_forniture_"""+commodity) \
    .withColumn('mercato',f.when(f.col('mercato')=='tutelato','regolato').otherwise(f.col('mercato')))

    df_ricavi_tcr = spark.sql("""SELECT 
    fk_testata,
    fk_forn_fatt,
    data_inizio_competenza,
    data_fine_competenza,
    cast(anno_comp as int),
    cast(mese_comp as int),
    saldo_acconto,
    origine_dato,
    componente_tcr,
    valore,
    sist_fat,
    churn,
    tipo_calcolo
    FROM lab1_db.rfcf_vw_ricavi_"""+commodity+""" \
    WHERE motivo_scarto in ("Valido","")""").filter(f.col('sist_fat')!='ND')

    df_mapping_tcr = spark.sql("""SELECT map_componente, map_tipologia_prezzo, map_flag_comp_ricavi FROM lab1_db.rfcf_vw_mapping_tcr_"""+commodity) \
    .filter((f.col('map_flag_comp_ricavi') == 'S') | (f.col('map_componente').isin('CONSUMO'))).drop('map_flag_comp_ricavi')

    cond_map = [df_ricavi_tcr.componente_tcr == df_mapping_tcr.map_componente]
    df_ricavi_tcr = df_ricavi_tcr.join(df_mapping_tcr,cond_map).drop('map_componente')

    df_ricavi_tcr = df_ricavi_tcr.withColumn('componente_tcr_FV', f.col('map_tipologia_prezzo')).drop('map_tipologia_prezzo')

    rand_udf = f.udf(lambda: random.randint(0, 9), IntegerType()).asNondeterministic()
    df_ricavi_tcr = df_ricavi_tcr.withColumn('salt', rand_udf())

    df_salt = spark.range(0, 10)
    df_salt = df_salt.withColumnRenamed('id', 'salt').withColumn('salt', f.col('salt').cast('int'))

    df_forniture_tcr = df_forniture_tcr.crossJoin(df_salt)

    cond_join = ['fk_forn_fatt', 'salt']
    info_tcr = df_forniture_tcr.join(df_ricavi_tcr, cond_join).drop('salt')

    return info_tcr
  
def tcr_calendario(spark,commodity,data):
    """Metodo per caricare forniture e ricavi del tcr per commodity.

    Input:  spark         --> sparksession
            commodity     --> stringa con commodity

    Output: info_tcr      --> dataframe di join tra forniture e ricavi tcr
          """
    df_forniture_tcr = spark.sql("""SELECT
    fk_forn_fatt,
    cod_pratica_crm,
    cod_pratica,
    commodity,
    lower(mercato) mercato,
    codice_pdf,
    tipo_regime, 
    cod_cliente,
    pod_pdr
    FROM lab1_db.rfcf_vw_forniture_"""+commodity) \
    .withColumn('mercato',f.when(f.col('mercato')=='tutelato','regolato').otherwise(f.col('mercato')))

    df_ricavi_tcr = spark.sql("""SELECT 
    fk_testata,
    fk_forn_fatt,
    data_inizio_competenza,
    data_fine_competenza,
    cast(anno_comp as int),
    cast(mese_comp as int),
    saldo_acconto,
    origine_dato,
    componente_tcr,
    valore,
    sist_fat,
    churn,
    tipo_calcolo
    FROM lab1_db.rfcf_vw_ricavi_"""+commodity+""" \
    WHERE motivo_scarto in ("Valido","")""").filter(f.col('sist_fat')!='ND')

    df_mapping_tcr = spark.sql("""SELECT map_componente, map_tipologia_prezzo, map_flag_comp_ricavi FROM lab1_db.rfcf_vw_mapping_tcr_"""+commodity) \
    .filter((f.col('map_flag_comp_ricavi') == 'S') | (f.col('map_componente').isin('CONSUMO'))).drop('map_flag_comp_ricavi')

    cond_map = [df_ricavi_tcr.componente_tcr == df_mapping_tcr.map_componente]
    df_ricavi_tcr = df_ricavi_tcr.join(df_mapping_tcr,cond_map).drop('map_componente')

    df_ricavi_tcr = df_ricavi_tcr.withColumn('componente_tcr_FV', f.col('map_tipologia_prezzo')).drop('map_tipologia_prezzo')

    rand_udf = f.udf(lambda: random.randint(0, 9), IntegerType()).asNondeterministic()
    df_ricavi_tcr = df_ricavi_tcr.withColumn('salt', rand_udf())

    df_salt = spark.range(0, 10)
    df_salt = df_salt.withColumnRenamed('id', 'salt').withColumn('salt', f.col('salt').cast('int'))

    df_forniture_tcr = df_forniture_tcr.crossJoin(df_salt)

    cond_join = ['fk_forn_fatt', 'salt']
    info_tcr = df_forniture_tcr.join(df_ricavi_tcr, cond_join).drop('salt')

    return info_tcr

def tcr_data(spark, commodity, data):
    """
    Funzione per ricavare le informazioni necessarie per l'assegnazione di cluster-fornitura dalle tabelle TCR
    """
    #Lettura info forniture
    df_forniture_tcr = spark.sql("""SELECT
        fk_forn_fatt,
        codice_pdf,
        cod_pratica_crm,
        cod_pratica,
        commodity,
        lower(mercato) mercato,
        sist_fat,
        cluster,
        codice_conto_cliente
        FROM lab1_db.rfcf_vw_forniture_"""+commodity) \
            .withColumn('mercato', f.when(f.col('mercato') == 'tutelato', 'regolato').otherwise(f.col('mercato')))

    df_ricavi_tcr = spark.sql("""SELECT 
        fk_forn_fatt,
        origine_dato,
        componente_tcr
        FROM lab1_db.rfcf_vw_ricavi_"""+commodity+""" \
        WHERE motivo_scarto in ("Valido","")""") \
            .filter(f.col('sist_fat') != 'ND')

    df_mapping_tcr = spark.sql("""SELECT
        map_componente,
        map_tipologia_prezzo, 
        map_flag_comp_ricavi
        FROM lab1_db.rfcf_vw_mapping_tcr_""" + commodity ) \
            .filter((f.col('map_flag_comp_ricavi') == 'S') | (f.col('map_componente').isin('CONSUMO'))).drop('map_flag_comp_ricavi')

    cond_map = [df_ricavi_tcr.componente_tcr == df_mapping_tcr.map_componente]
    df_ricavi_tcr = df_ricavi_tcr.join(df_mapping_tcr, cond_map).drop('map_componente')

    df_ricavi_tcr = df_ricavi_tcr.withColumn('componente_tcr_FV', f.col('map_tipologia_prezzo')) \
        .drop('map_tipologia_prezzo')

    cond_join = ['fk_forn_fatt']
    info_tcr = df_forniture_tcr.join(df_ricavi_tcr, cond_join)
    return info_tcr

####################################################################################################

def transform(df,anno,df_fattura):
  """Metodo con le trasformazioni sui campi.

  Input:  df        --> dataframe di partenza

  Output: forn      --> dataframe con Componente_TCR / Cadenza / Gruppo trasformati
          """
  test_case = {
	'BP':'bolletta piatta',
  'CED':'cedibili',
  'CED_BP':'cedibili bolletta piatta',
  'DOM':'domiciliati',
  'DOM_BP':'domiciliati bolletta piatta'
    }
  test_case = OrderedDict(test_case)
  test_case.move_to_end('BP')
  
  df = df.withColumn('commodity_tcr',f.col('commodity')).withColumn('commodity',f.when(~ f.col('COD_GRUPPO_FATT').contains('ORF'),
          f.when(f.substring('COD_GRUPPO_FATT', 0, 2) == 10, 'gas').otherwise('luce')) \
            .otherwise(f.when(f.col('COD_GRUPPO_FATT').contains('POW'),'luce').otherwise('gas')))
    
  df = df.withColumn('TIPOLOGIA', f.when(f.col('COD_GRUPPO_FATT').contains('ORF'), 'bolletta piatta').otherwise(
        f.coalesce(*[f.when(f.upper(f.col('COD_GRUPPO_FATT')).like('%'+key), f.lit(value)) for key, value in
          test_case.items()],f.lit('normali')))) \
            .withColumn('mercato', f.when((f.col('TIPOLOGIA').contains('piatta')) | (f.col('commodity')=='luce'),'libero').otherwise(f.col('mercato')))   


  df = df.withColumn('Gruppo', f.when(f.col('COD_GRUPPO_FATT').contains('ORF'), 'M').otherwise(f.substring('COD_GRUPPO_FATT', 6, 1))) \
        .withColumn('cadenza',f.when(f.col('COD_GRUPPO_FATT').contains('ORF'), 'mensile').otherwise(f.when((f.col('COMMODITY') == 'gas') & (f.col('CADE_DESCRIZIONE').contains('MENSILE')) &
          ((f.col('CADE_DESCRIZIONE').contains('INVERN')) | (f.col('CADE_DESCRIZIONE').contains('ESTIV'))), \
            f.lower(f.substring_index('CADE_DESCRIZIONE', ' ', 2)[0:11])) \
              .otherwise(f.lower(f.substring_index('CADE_DESCRIZIONE', ' ', 1))))) \
                .drop('CADE_DESCRIZIONE')
					  
  df_fatturato = df.filter(f.col('origine_dato') == 'Fatturato')
  
  df_fatturato = df_fatturato.select(f.col('fk_testata'),f.col('fk_forn_fatt')).dropDuplicates()

  df_fattura_data_emiss = df_fattura.join(df_fatturato, 'fk_testata')
  df_fattura_data_emiss = df_fattura_data_emiss.groupBy('fk_forn_fatt').agg(f.max(f.col('DAT_EMISSIONE_BOL')).alias('MAX_DAT_EMISSIONE'))

  df = df.filter(f.col('origine_dato') != 'Fatturato')
  df = df.join(df_fattura_data_emiss, 'fk_forn_fatt', 'left').withColumn('MAX_DAT_EMISSIONE', f.when(f.col('MAX_DAT_EMISSIONE').isNull(), \
                                                                                                    datetime(anno-1,12,31)).otherwise(f.col('MAX_DAT_EMISSIONE')))
  return df

def test(df_emissione_speciale_rateo):
  """Metodo per testare le date competenza corrette
  Input:  df_emissione_speciale_rateo --> dataframe di input
  Output: test                        --> dataframe di output
          """
  test = df_emissione_speciale_rateo.withColumn('flag',f.month(f.col('data_fatturazione'))-f.month(f.col('MAX_DAT_EMISSIONE')))
  test = test.withColumn('filter',f.rank().over(Window.partitionBy(f.col('fk_forn_fatt')).orderBy('data_fatturazione')))
  test = test.filter(f.col('filter')==1).dropDuplicates().drop('filter','flag')
  test = test.withColumn('start_Date_Fatt', f.col('data_inizio_competenza')) \
    .withColumn('end_Date_Fatt', f.col('data_fine_competenza'))

  return test


def preparation_letture(spark, par, day_end):
  """Metodo che prepara le letture
  Input:  spark   --> sessione spark
          par     --> classe con parametri di anagrafica
  Output: df_lett --> dataframe con letture
            """
  day_start = day_end + relativedelta(months=-12)
  df_tipologie_letture = spark.table('lab1_db.rfcf_decodifiche') \
    .filter(f.col('id_run').cast('int') == par.id_run_fw).filter(f.col('tipo') == 'VALIDITA_TIPO_LOGICO_LETT') \
      .select(f.col('id').alias('DSC_TIPO_LOGICO_LETT'))
  df_stato_letture = spark.table('lab1_db.rfcf_decodifiche') \
    .filter(f.col('id_run').cast('int') == par.id_run_fw).filter(f.col('tipo') == 'STATO_LETTURE') \
      .selectExpr('id METERING', 'id_2 STATO_LETTURA', 'valore VALIDITA_LETTURA')
  df_stato_letture = df_stato_letture.filter(f.col('METERING') == 'NETA')
  df_stato_letture = df_stato_letture.filter(f.col('VALIDITA_LETTURA') == 'S')

  df_letture_puntuali = spark.sql("""SELECT 
    codice_pdf,
    data_ltu,
    stato_lettura,
    data_inizio_comp,
    data_fine_comp,
    consumo,
    effettiva_stimata  
    FROM edl_ods.vw_fcf_misura_puntuale""").filter(f.col('data_ltu') > day_start).filter(f.col('data_ltu') <= day_end) \
  .withColumn('dsc_tipo_logico_lett', f.lit('Puntuali'))
  #      WHERE year(data_ltu) = 2020""").withColumn('dsc_tipo_logico_lett', f.lit('Puntuali'))

  df_letture_pwr = spark.sql("""SELECT 
    codice_pdf,
    data_ltu,
    dsc_tipo_logico_lett,
    stato_lettura,
    data_inizio_comp,
    data_fine_comp,
    consumo,
    effettiva_stimata 
    FROM edl_ods.vw_fcf_lettura_pwr""").filter(f.col('data_ltu') > day_start).filter(f.col('data_ltu') <= day_end)
  #      WHERE data_ltu > cast("""+ str(day) + """ AS TIMESTAMP)""")

  df_letture_gas = spark.sql("""SELECT
    codice_pdf,
    data_ltu,
    dsc_tipo_logico_lett,
    stato_lettura,
    data_inizio_comp,
    data_fine_comp,
    consumo_calcolato consumo,
    effettiva_stimata 
    FROM edl_ods.vw_fcf_lettura_gas""").filter(f.col('data_ltu') > day_start).filter(f.col('data_ltu') <= day_end)
  #      WHERE year(data_ltu) = 2020""")

  df_letture = df_letture_pwr.union(df_letture_gas).unionByName(df_letture_puntuali) \
    .withColumnRenamed('stato_lettura', 'STATO_LETTURA')
  df_letture = df_letture.withColumn('effettiva_stimata', f.coalesce(f.col('effettiva_stimata'), f.lit('E')))
  df_letture = df_letture.filter(f.col('effettiva_stimata')=='E')
  df_letture = df_letture.withColumn('deltaGG', f.datediff('data_fine_comp', 'data_inizio_comp') + 1)
  df_letture = df_letture.filter(~ ((f.col('deltaGG') == 0) & (f.col('consumo') == 0))).drop('deltaGG') \
    .withColumnRenamed('dsc_tipo_logico_lett', 'DSC_TIPO_LOGICO_LETT')
  
  cond_tipo_lett = ['DSC_TIPO_LOGICO_LETT']
  df_letture = df_letture.join(df_tipologie_letture, cond_tipo_lett, 'left_anti')
  
  cond_stato_lettura = ['STATO_LETTURA']
  df_lett = df_letture.join(df_stato_letture, cond_stato_lettura)
  df_lett = df_lett.withColumn('Tipo_lettura',
                               f.when(f.col('DSC_TIPO_LOGICO_LETT').contains('AUTOLETT'), 'AUTOLETTURA') \
                               .otherwise('LETTURA'))
  return df_lett
################################## OPEN ##############################

def calcoli(open_df):
  """Metodo che ridistribuisce le percentuali dei punti open per cluster
  Input:  open_df  --> dataframe con punti open
  Output: res     --> dataframe risultante
          """
  df = tv.clusterize(open_df)
  
  df = df.withColumn('commodity',f.coalesce(f.col('commodity'), f.when(f.col('commodity')=='EE','luce').otherwise('gas'))) \
    .withColumn('cadenza', f.coalesce(f.col('cadenza'), f.lit('bimestrale'))).withColumn('Gruppo', f.coalesce(f.col('Gruppo'), f.lit('B'))) \
      .withColumn('mercato', f.coalesce(f.col('mercato'), f.when(f.col('mercato')=='Tutelato','regolato').otherwise('libero'))) \
        .withColumn('TIPOLOGIA', f.coalesce(f.col('TIPOLOGIA'), f.lit('cedibili')))
  
  df = df.withColumn('macrosegmento',f.coalesce(f.col('macrosegmento'),f.when(f.col('pod_pdr').like('RET%'),
    f.substring_index(f.col('pod_pdr'),'#',-1)).otherwise('ND')))
  
  df = df.filter(f.col('TIPOLOGIA') == 'normali')
  
  tot = df.groupBy(['macrosegmento','mercato','commodity']).count() \
    .withColumnRenamed('count','TOT')
  
  parz = df.groupBy(['macrosegmento','mercato','commodity','cadenza','tipologia','gruppo']).count() \
    .withColumnRenamed('count','PARZ')
    
  res = parz.join(tot,['macrosegmento','mercato','commodity'])
  
  res = res.withColumn('percentuale', f.col('PARZ')/f.col('TOT')).drop('PARZ','TOT')
  
  return res

def elaborazione_open(df,anno):
  """Metodo che prepara i dati delle tabelle degli open
  Input:  df   --> dataframe con dati open
          anno --> anno da filtrare
  Output: df   --> dataframe di output
          """
  
  df = df.withColumn('prodotto_des', f.when(f.col('prodotto_des')=='Power','luce').otherwise('gas'))
  df = df.filter(f.substring(f.col('tempo'), 6,4) == anno) \
    .withColumn('tempo_anno', f.substring(f.col('tempo'),6,4).cast('int')) \
    .withColumn('tempo_mese',f.substring(f.col('tempo'),11,2).cast('int')).drop('tempo')
  df = df.filter(f.col('stato_des') == 'Da Contrattare')
  df = df.withColumn('mercato_des', f.when(f.col('mercato_des').contains('Libero'),'libero').otherwise('regolato'))
  df = df.withColumn('segmento_des', f.when(f.col('segmento_des')=='Condomini','CONDOMINIO') \
    .when(f.col('segmento_des')=='Pubblica Amministrazione','PUBBLICA AMMINISTRAZIONE') \
      .when(f.col('segmento_des')=='Microbusiness','MICROBUSINESS') \
        .when(f.col('segmento_des')=='Multisito','MULTISITO') \
          .when(f.col('segmento_des')=='Autotrazione','AUTOTRAZIONE') \
            .when(f.col('segmento_des')=='Residenziali','RESIDENZIALI') \
              .when(f.col('segmento_des')=='Imprese','IMPRESE'))
  return df

#####################################################################################

def elaborazione_forniture(spark,data_snapshot,data_decorrenza):
  """Metodo che lega il macrosegmento alle forniture
  Input:  spark           --> sessione spark
          data_snapshot   --> classe con le date snapshot
          data_decorrenza --> data da cui prendere i dati
  Output: df              --> dataframe di output
          """
  
  df_forniture_neta = spark.sql("""SELECT
      forn_old_codice_fornitura,
      cade_descrizione,
      cod_gruppo_fatt,
      if(spwkf_descrizione = 'ATTIVATA', 1, (if(spwkf_descrizione in ('ATTIVA COMMERCIALMENTE','IN CORSO DI CESSAZIONE'), 2, 3))) as rank,
      data_snapshot
      FROM edl_neta.vw_neta_fcf_fornitura""").filter(f.col('data_snapshot') == data_snapshot['vw_neta_fcf_fornitura']).drop('data_snapshot')
  
  key_cols = ['forn_old_codice_fornitura']
  w_key = Window.partitionBy(key_cols)
  df_forniture_neta = df_forniture_neta.withColumn('to_mantain', f.min('rank').over(w_key)).filter(f.col('rank')==f.col('to_mantain'))
  df_forniture_neta = df_forniture_neta.dropDuplicates(key_cols).drop('to_mantain', 'rank')

  
  df_forniture_tcr_pwr = spark.sql("""SELECT fk_forn_fatt, cod_pratica_crm, commodity, lower(mercato) mercato, cod_cliente, data_decorrenza, pod_pdr
    FROM lab1_db.rfcf_vw_forniture_pwr""")

  df_forniture_tcr_gas = spark.sql("""SELECT fk_forn_fatt, cod_pratica_crm, commodity, lower(mercato) mercato, cod_cliente, data_decorrenza, pod_pdr
  FROM lab1_db.rfcf_vw_forniture_gas""")

  df_forniture_tcr = df_forniture_tcr_pwr.unionByName(df_forniture_tcr_gas).filter(f.col('data_decorrenza') >= data_decorrenza) \
    .withColumn('mercato', f.when(f.col('mercato')=='tutelato','regolato').otherwise(f.col('mercato')))

  df_ricavi_tcr_pwr = spark.sql("""SELECT fk_forn_fatt, saldo_acconto, componente_neta, componente_tcr, valore 
                                     FROM lab1_db.rfcf_vw_ricavi_pwr""")


  df_ricavi_tcr_gas = spark.sql("""SELECT fk_forn_fatt, saldo_acconto, componente_neta, componente_tcr, valore 
                                     FROM lab1_db.rfcf_vw_ricavi_gas""")

  df_ricavi_tcr = df_ricavi_tcr_pwr.union(df_ricavi_tcr_gas)

  cond_ricavi_forn_tcr = ['fk_forn_fatt']
  df = df_ricavi_tcr.join(df_forniture_tcr, cond_ricavi_forn_tcr)

  cond_neta_df = [df_forniture_neta.forn_old_codice_fornitura == df.cod_pratica_crm]
  df = df.join(df_forniture_neta, cond_neta_df,'left')


  df_soggetti = spark.sql("""SELECT * FROM edl_ods.vw_fcf_soggetti""")

  ds_soggetti = df_soggetti.select('data_snapshot').groupby().agg(f.max('data_snapshot')).collect()[0][0]

  df_soggetti = df_soggetti.filter(f.col('data_snapshot') == ds_soggetti).select('macrosegmento', 'cod_cliente')


  cond_soggetti = ['cod_cliente']
  df = df.join(df_soggetti, cond_soggetti,'left')
  
  return df

#################################################################

def preparation_scrittura_open(spark,calendario_agg,df_open):
  """Metodo per preparar gli intervalli di fatturazione spezzati per mese corretti
  Input:  spark                   --> sessione spark
          calendario_agg          --> dataframe con il calendario di fatturazione
          df_open                 --> dataframe con i dati open
  Output: df__open_calendarizzati --> dataframe con gli intervalli corretti
          """
  cond_calendario = ['COMMODITY','MERCATO','CADENZA','TIPOLOGIA','GRUPPO']
  df_open_calendarizzati = df_open.join(f.broadcast(calendario_agg), cond_calendario)

  filtro = (f.col('data_fine_competenza') >= f.col('startFattura')) \
      & (f.col('data_inizio_competenza') <= f.col('endFattura'))

  df_open_calendarizzati = df_open_calendarizzati.filter(filtro)

  df_open_calendarizzati = df_open_calendarizzati.withColumn('start_Date_Fatt', f.greatest(f.col('data_inizio_competenza').cast('timestamp'), \
                                                                           f.col('startFattura')))

  df_open_calendarizzati = df_open_calendarizzati.withColumn('end_Date_Fatt', f.least(f.col('data_fine_competenza').cast('timestamp'),
      f.col('endFattura')))

  df_open_calendarizzati = pro_die(df_open_calendarizzati, inizio_competenza='data_inizio_competenza', fine_competenza='data_fine_competenza',\
                             inizio_mese='start_Date_Fatt', fine_mese='end_Date_Fatt',val='VALORE_percentuale')

  return df_open_calendarizzati

def preparation_scrittura_open_punti(spark, calendario_agg, df_open):
  """Metodo per preparar gli intervalli di fatturazione spezzati per mese corretti
  Input:  spark                   --> sessione spark
          calendario_agg          --> dataframe con il calendario di fatturazione
          df_open                 --> dataframe con i dati open
  Output: df__open_calendarizzati --> dataframe con gli intervalli corretti
          """
  cond_calendario = ['COMMODITY','MERCATO','CADENZA','TIPOLOGIA','GRUPPO']
  df_open_calendarizzati = df_open.join(f.broadcast(calendario_agg), cond_calendario)

  filtro = (f.concat(f.year('data_fatturazione').cast("string"), f.format_string("%02d", f.month('data_fatturazione'))).cast('integer') > \
            f.concat(f.col('tempo_anno').cast("string"), f.format_string("%02d", f.col('tempo_mese'))).cast('integer'))

  df_open_calendarizzati = df_open_calendarizzati.filter(filtro)

  df_open_calendarizzati = df_open_calendarizzati.withColumn('start_Date_Fatt', f.greatest(f.col('data_inizio_competenza').cast('timestamp'), \
                                                                           f.col('startFattura')))

  df_open_calendarizzati = df_open_calendarizzati.withColumn('end_Date_Fatt', f.least(f.col('data_fine_competenza').cast('timestamp'),
      f.col('endFattura')))

  df_open_calendarizzati = pro_die(df_open_calendarizzati, inizio_competenza='start_Date_Fatt', fine_competenza='end_Date_Fatt',\
                             inizio_mese='start_Date_Fatt', fine_mese='end_Date_Fatt', val='VALORE_percentuale')

  return df_open_calendarizzati

#################################################################

def preparation_scrittura(spark,calendario_agg,df):
  """Metodo per preparar gli intervalli di fatturazione spezzati per mese corretti
  Input:  spark             --> sessione spark
          calendario_agg    --> dataframe con il calendario di fatturazione
          df                --> dataframe con i ricavi calendarizzati e le letture
  Output: df_calendarizzati --> dataframe con gli intervalli corretti
          """
  cond_calendario = ['fk_forn_fatt', 'FV']
  df_calendarizzati = df.join(calendario_agg, cond_calendario)

  filtro = (f.col('data_fine_competenza') >= f.col('startFattura')) \
      & (f.col('data_inizio_competenza') <= f.col('endFattura'))

  df_calendarizzati = df_calendarizzati.filter(filtro)

  df_calendarizzati = df_calendarizzati.withColumn('start_Date_Fatt', f.greatest(f.col('data_inizio_competenza').cast('timestamp'), \
                                                                           f.col('startFattura')))

  df_calendarizzati = df_calendarizzati.withColumn('end_Date_Fatt', f.least(f.col('data_fine_competenza').cast('timestamp'),
      f.col('endFattura')))

  df_calendarizzati = pro_die(df_calendarizzati, inizio_competenza='data_inizio_competenza', fine_competenza='data_fine_competenza',\
                             inizio_mese='start_Date_Fatt', fine_mese='end_Date_Fatt',val='VALORE')

  return df_calendarizzati

#################################################################

def tcr(spark,commodity,ds):
  """Metodo che prepara i dati di ricavo tcr per le letture
  Input:  spark      -->  sessione spark
          commodity  -->  commodity
          ds         -->  classe con date_snapshot
  Output: info_tcr   -->  dataframe output
          """
  df_forniture_tcr = spark.sql("""SELECT
    fk_forn_fatt,
    commodity,
    lower(mercato) mercato,
    cod_pratica_crm,
    codice_pdf
    FROM lab1_db.rfcf_vw_forniture_"""+commodity) \
      .withColumn('mercato',f.when(f.col('mercato')=='tutelato','regolato').otherwise(f.col('mercato')))

  df_ricavi_tcr = spark.sql("""SELECT 
    fk_testata,
    fk_forn_fatt,
    origine_dato,
    componente_tcr,
    valore
    FROM lab1_db.rfcf_vw_ricavi_"""+commodity+"""
    WHERE motivo_scarto in ("Valido","")""")
  df_mapping_tcr = spark.sql("""SELECT map_componente, map_tipologia_prezzo FROM lab1_db.rfcf_vw_mapping_tcr_"""+commodity )

  cond_map = [df_ricavi_tcr.componente_tcr == df_mapping_tcr.map_componente]
  df_ricavi_tcr = df_ricavi_tcr.join(df_mapping_tcr,cond_map).drop('map_componente')

  df_ricavi_tcr = df_ricavi_tcr.withColumn('componente_tcr_F/V', f.col('map_tipologia_prezzo')).drop('map_tipologia_prezzo')

  cond_join= ['fk_forn_fatt']
  info_tcr = df_forniture_tcr.join(df_ricavi_tcr, cond_join)
  return info_tcr


#################################################################


def tcr_commodity_anno_prec(spark,commodity,data):
    """Metodo per caricare forniture e ricvi del tcr per commodity.

    Input:  spark         --> sparksession
          commodity     --> stringa con commodity

    Output: info_tcr      --> dataframe di join tra forniture e ricavi tcr
          """
    df_forniture_tcr = spark.sql("""SELECT
    fk_forn_fatt,
    cod_pratica_crm,
    cod_pratica,
    commodity,
    lower(mercato) mercato,
    codice_pdf,
    tipo_regime, 
    cod_cliente,
    pod_pdr
    FROM lab1_db.rfcf_vw_forniture_"""+commodity)\
    .withColumn('mercato',f.when(f.col('mercato')=='tutelato','regolato').otherwise(f.col('mercato')))

    df_ricavi_tcr = spark.sql("""SELECT 
    fk_testata,
    fk_forn_fatt,
    data_inizio_competenza,
    data_fine_competenza,
    cast(anno_comp as int) as anno_comp,
    cast(mese_comp as int) as mese_comp,
    saldo_acconto,
    origine_dato,
    componente_tcr,
    valore,
    sist_fat,
    churn,
    case when sist_fat in ('Ret','Lodestar','Mediana') then tipo_calcolo else 'ND' end as tipo_calcolo
    FROM lab1_db.rfcf_vw_ricavi_"""+commodity+"""_anno_prec
    WHERE origine_dato in ('Rateo','Rettifica')  
    AND motivo_scarto in ('Valido','')""").filter(f.col('sist_fat')!='ND')


    df_mapping_tcr = spark.sql("""SELECT map_componente, map_tipologia_prezzo, map_flag_comp_ricavi FROM lab1_db.rfcf_vw_mapping_tcr_"""+commodity) \
    .filter((f.col('map_flag_comp_ricavi') == 'S') | (f.col('map_componente').isin('CONSUMO'))).drop('map_flag_comp_ricavi')      
      
    cond_map = [df_ricavi_tcr.componente_tcr == df_mapping_tcr.map_componente]
    df_ricavi_tcr = df_ricavi_tcr.join(df_mapping_tcr,cond_map).drop('map_componente')

    df_ricavi_tcr = df_ricavi_tcr.withColumn('componente_tcr_FV', f.col('map_tipologia_prezzo')).drop('map_tipologia_prezzo')

    rand_udf = f.udf(lambda: random.randint(0, 9), IntegerType()).asNondeterministic()
    df_ricavi_tcr = df_ricavi_tcr.withColumn('salt', rand_udf())

    df_salt = spark.range(0, 10)
    df_salt = df_salt.withColumnRenamed('id', 'salt').withColumn('salt', f.col('salt').cast('int'))

    df_forniture_tcr = df_forniture_tcr.crossJoin(df_salt)

    cond_join = ['fk_forn_fatt', 'salt']
    info_tcr = df_forniture_tcr.join(df_ricavi_tcr, cond_join).drop('salt')

    return info_tcr
  
def createDefaultMasterDict(df_ordineDefault, criteri):
    """

    :param df_ordineDefault: pandas dataframe, table lab1_db.rfcf_ordine_default_tioce that contains default order of priority
    :param colonnaNomeCriteri:
    :return:
    """
    master_dict_string = '{'

    for i in criteri:
        dict_tmp = df_ordineDefault[df_ordineDefault.campo == i].to_dict()
        dict_criterio = f"'{i}'" + ': {'
        for j in dict_tmp['valore'].keys():
            dict_criterio = dict_criterio + f"'{dict_tmp['valore'][j]}':{dict_tmp['ordine_valore'][j]},"

        master_dict_string = master_dict_string + dict_criterio[:-1] + '},'

    masterDict = eval(master_dict_string[:-1] + '}')

    return masterDict
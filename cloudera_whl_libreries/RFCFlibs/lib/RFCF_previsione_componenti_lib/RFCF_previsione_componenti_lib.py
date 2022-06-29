from collections import OrderedDict
import pyspark.sql.functions as f
from pyspark.sql import Window
from pyspark.sql.types import *
from datetime import datetime
from dateutil.relativedelta import relativedelta
import calendar as cl

from udfs import *
from lib import RFCF_calendario_lib as c

def sum_col(df, col):
  """ Funzione che restituisce la somma dei valori di una colonna numerica 
  
  Input:  df      -->   dataframe
          col     -->   colonna numerica
          
  Output: double  -->   valore somma
  
  """
  return df.select(f.sum(col)).collect()[0][0]


def dataframe(spark,componente,anno,date_snapshot, mese_fine_cons):
  """ Funzione che recupera dati dal competenziato (BR+BM+XE), dall'anno emissione precedente in avanti, per componente data
  
  Input:  spark         -->   sessione spark stanziata
          componente    -->   stringa con componente analizzata
          anno          -->   anno attuale
          date_snapshot -->   data_snapshot da analizzare
          
  Output: df            -->   dataframe
  
  """
  # definisco anno_partenza in modo da avere sempre due anni di emesso 
  now = datetime.now()
  if mese_fine_cons == 0 and now.year == anno-1: 
    anno_partenza = anno-2
    # NOTA: non funziona in caso di budget anno x lanciato nell'anno x+1 (perché non trova nell'aggregato dell'emesso l'anno x-1)
  else:
    anno_partenza = anno-1
  
  stringa_where_map =  " WHERE map_componente in " + componente   
  stringa_where_comp = " WHERE cast(substring(annomese,1,4) as int) >= " + str(anno_partenza) +" and commodity <> 'LAVORI' and componente not like '%CONSUMO%'"
  campi_mapping = "map_tipo_riga as tipo_riga"
  campi_competenziato = "tipo_riga, commodity, sist_fat, cast(substring(annomese,1,4) as int) as anno, cast(substring(annomese,5,2) as int) as mese, valore_comp"
                                       
  query_mapping_pwr = " SELECT " + campi_mapping + " from lab1_db.rfcf_vw_mapping_voci_contabili_pwr " + stringa_where_map
  query_mapping_gas = " SELECT " + campi_mapping + " from lab1_db.rfcf_vw_mapping_voci_contabili_gas " + stringa_where_map
  query_competenziato = " SELECT " + campi_competenziato + " FROM lab1_db.rfcf_dett_fatt_comp " + stringa_where_comp
                                       
  df_mapping_tcr_pwr = spark.sql(query_mapping_pwr)
  df_mapping_tcr_gas = spark.sql(query_mapping_gas)
  
  df = spark.sql(query_competenziato).withColumn('sist_fat', f.when(f.col('sist_fat')=='3','NETA BR').when(f.col('sist_fat')=='8','NETA BM').otherwise('ND'))
  df_mapping_tcr = df_mapping_tcr_pwr.unionByName(df_mapping_tcr_gas).dropDuplicates()

  df = df.join(df_mapping_tcr, 'tipo_riga')   
  
  # aggiungo righe xe 
  
  stringa_where_map_xe =  " WHERE map_componente in " + componente   
  stringa_where_comp_xe = " WHERE commodity <> 'LAVORI' and tipo_riga not like '%CONSUMO%'"
  campi_mapping_xe = "map_tipo_riga as tipo_riga"
  campi_competenziato_xe = "codice_sotto_testata, tipo_riga, commodity, sist_fat, valore_comp"
  campi_sottotestata_xe = "codice_sottotestata as codice_sotto_testata, codice_fattura, edl_last_modify"
  campi_testata_xe = "num_bolletta as codice_fattura, cast(substring(dat_emissione_bol,1,4) as int) as anno, cast(substring(dat_emissione_bol,5,2) as int) as mese, edl_last_modify "
                                       
  df_mapping_xe_pwr = spark.sql("SELECT " + campi_mapping_xe + " from lab1_db.rfcf_mapping_xe_pwr " + stringa_where_map_xe)
  df_mapping_xe_gas = spark.sql("SELECT " + campi_mapping_xe + " from lab1_db.rfcf_mapping_xe_gas " + stringa_where_map_xe)
  df_mapping_xe = df_mapping_xe_pwr.unionByName(df_mapping_xe_gas).dropDuplicates()
  
  df_xe = spark.sql("SELECT " + campi_competenziato_xe + " FROM lab1_db.rfcf_dett_fatt_comp_xe " + stringa_where_comp_xe) 
  # lego la testata per recuperare anno e mese emissione
  w_let = Window.partitionBy(f.col('codice_sotto_testata')).orderBy(f.col('edl_last_modify').desc())
  sottotestata_xe = spark.sql("SELECT distinct " + campi_sottotestata_xe  + " from edl_int.br_tcr_sottotestatabollette") \
                         .withColumn('edl_last_modify', f.to_date('edl_last_modify', 'yyyy-MM-dd')).withColumn('edl_last_modify', f.to_timestamp('edl_last_modify', 'yyyy-MM-dd')) \
                         .withColumn('rank', f.rank().over(w_let)).filter(f.col('rank')==1).drop('edl_last_modify','rank')
                         #.withColumn('commodity', f.when(f.col('commodity')=='PWR','EE').otherwise(f.col('commodity')))
  w_let = Window.partitionBy(f.col('codice_fattura')).orderBy(f.col('edl_last_modify').desc())
  testata_xe = spark.sql("SELECT distinct " + campi_testata_xe + " from edl_int.br_tcr_testatabollette") \
                    .withColumn('edl_last_modify', f.to_date('edl_last_modify', 'yyyy-MM-dd')).withColumn('edl_last_modify', f.to_timestamp('edl_last_modify', 'yyyy-MM-dd')) \
                    .withColumn('rank', f.rank().over(w_let)).filter(f.col('rank')==1).drop('edl_last_modify','rank')
  df_xe = df_xe.join(sottotestata_xe,'codice_sotto_testata', 'left')
  df_xe = df_xe.join(testata_xe,'codice_fattura', 'left').filter(f.col('anno') >= anno_partenza).drop('codice_fattura','codice_sotto_testata')
  df_xe = df_xe.join(df_mapping_xe, 'tipo_riga')
  
  df = df.unionByName(df_xe)
  
  return df


def dataframe_new(spark,componente,anno,date_snapshot, mese_fine_cons):
  """ Funzione che recupera dati dal competenziato aggregato (BR+BM+XE), dall'anno emissione precedente in avanti, per componente data
  
  Input:  spark           -->   sessione spark stanziata
          componente      -->   stringa con componente analizzata
          anno            -->   anno attuale
          date_snapshot   -->   data_snapshot da analizzare
          mese_fine_cons  -->   mese fine consuntivo
          
  Output: df              -->   dataframe
  
  """
  # definisco anno_partenza in modo da avere sempre due anni di emesso 
  now = datetime.now()
  if mese_fine_cons == 0 and now.year == anno-1: 
    anno_partenza = anno-2
    # NOTA: non funziona in caso di budget anno x lanciato nell'anno x+1 (perché non trova nell'aggregato dell'emesso l'anno x-1)
  else:
    anno_partenza = anno-1
  
  stringa_where =  " WHERE componente_tcr in " + componente + "and year(data_emissione) >= " + str(anno_partenza) + " and componente_tcr not like '%CONSUMO%'"
  campi_emesso_aggregato = "commodity_tcr as commodity, sist_fat, year(data_emissione) as anno, month(data_emissione) as mese, valore_comp"
                                       
  df_emesso_aggregato_pwr = " SELECT " + campi_emesso_aggregato+ " FROM lab1_db.rfcf_fatturato_emesso_pwr " + stringa_where
  df_emesso_aggregato_gas = " SELECT " + campi_emesso_aggregato + " FROM lab1_db.rfcf_fatturato_emesso_gas " + stringa_where
                                       
  df_pwr = spark.sql(df_emesso_aggregato_pwr)
  df_gas = spark.sql(df_emesso_aggregato_gas)
  
  df = df_pwr.unionByName(df_gas)\
             .withColumn('sist_fat', f.when(f.col('sist_fat')=='3','NETA BR').when(f.col('sist_fat')=='8','NETA BM').otherwise('XE'))

  return df  


def previsione_pr_breve_att(spark, anno, id_run, date_snapshot, mese_fine_cons, target_annuale_retail, target_annuale_middle): 
  """ Funzione per la creazione della tabella lab1_db.rfcf_previsione_pr_breve_attributes contenente i target BM e BR,
      il fatturato della prescrizione breve per l'anno precedente e le relative percentuali mensili rispetto al totale
  
  Input:  spark                  -->   sessione spark stanziata
          anno                   -->   anno attuale
          id_run                 -->   id_run corrente
          date_snapshot          -->   data_snapshot da analizzare
          mese_fine_cons         -->   mese fine consuntivo
          target_annuale_retail  -->   target annuale BR
          target_annuale_middle  -->   target annuale BM
          
  Output: df_pr_breve            -->   dataframe
  
  """
  dataframe_pr_breve = dataframe_new(spark,"('PREB')",anno,date_snapshot,mese_fine_cons)
  dataframe_pr_breve = dataframe_pr_breve.withColumn('annomese', f.concat(f.col('anno'),f.lpad(f.col('mese'),2,'0')))
  
  # considero XE nella previsione solo dal 2021
  if anno < 2021:
    dataframe_pr_breve = dataframe_pr_breve.filter(f.col('sist_fat') != 'XE')
  
  if anno >= 2021: # a partire dalla previsione 2021 il sistema di fatturazione Middle è XE
    dataframe_pr_breve = dataframe_pr_breve.withColumn('sist_fat',f.when(f.col('sist_fat')=='NETA BM', f.lit('XE')).otherwise(f.col('sist_fat')))
  
  # emesso dell'anno precedente: se non è completo (caso budget) prendo i mesi mancanti/incompleti dall'anno prima
  now = datetime.now()
  if mese_fine_cons == 0 and now.year == anno-1: # emesso incompleto/mancante
    df_pr_breve_retail = dataframe_pr_breve.filter(f.col('sist_fat') == 'NETA BR')
    df_pr_breve_middle = dataframe_pr_breve.filter(f.col('sist_fat').isin('NETA BM', 'XE'))
    max_annomese_retail = df_pr_breve_retail.select('annomese').rdd.max()[0]
    mese_max_emesso_retail = str(max_annomese_retail)[-2:]
    max_annomese_middle = df_pr_breve_middle.select('annomese').rdd.max()[0]
    mese_max_emesso_middle = str(max_annomese_middle)[-2:]
    df_pr_breve_retail = df_pr_breve_retail.filter((f.col('annomese') < max_annomese_retail) & (f.col('annomese') >= str(anno-2)+str(mese_max_emesso_retail)))
    df_pr_breve_middle = df_pr_breve_middle.filter((f.col('annomese') < max_annomese_middle) & (f.col('annomese') >= str(anno-2)+str(mese_max_emesso_middle)))
    df_pr_breve = df_pr_breve_retail.unionByName(df_pr_breve_middle)\
                                    .withColumn('anno', f.lit(anno-1))
  else:
    df_pr_breve = dataframe_pr_breve.filter(f.col('anno')== anno-1) 
  
  df_pr_breve = df_pr_breve.groupBy('anno','mese','commodity','sist_fat').sum('valore_comp').withColumnRenamed('sum(valore_comp)', 'PREB_anno_prec')
  
  # aggiungo un sist_fat aggregato
  df_pr_breve = df_pr_breve.withColumn('sist_fat_agg', f.when(f.col('sist_fat')=='NETA BR', f.lit('BR')).otherwise(f.lit('BM/XE')))
  
  df_pr_breve = df_pr_breve.withColumn('id_run', f.lit(str(id_run)))                                                                          
  df_pr_breve = df_pr_breve.withColumnRenamed('anno','anno_prec')                                    
  df_pr_breve = df_pr_breve.withColumn('anno_att', f.lit(str(anno)))
  
  # calcolo percentuali (su totali raggruppati per sist_fat_agg)
  tot = df_pr_breve.groupBy('sist_fat_agg').sum('PREB_anno_prec') \
                   .withColumnRenamed('sum(PREB_anno_prec)','PREB_tot_anno_prec_sistfat')  
  df_pr_breve = df_pr_breve.join(tot,'sist_fat_agg')
  df_pr_breve = df_pr_breve.withColumn('importo_percentuale', f.col('PREB_anno_prec')/f.col('PREB_tot_anno_prec_sistfat')) 
  
  df_pr_breve = df_pr_breve.withColumn('target_br_PREB_anno_att', f.lit(-1 * target_annuale_retail)) \
                           .withColumn('target_bm_PREB_anno_att', f.lit(-1 * target_annuale_middle))
    
  return df_pr_breve


def punti_anno_attuale(spark, anno, mese_fine_cons, df_punti_temp):
  """ Funzione per il conteggio dei punti distinti per l'anno attuale, raggruppati per mese, commodity e sistema di fatturazione
  
  Input:  spark           -->   sessione spark stanziata
          anno            -->   anno attuale
          mese_fine_cons  -->   mese fine consuntivo
          df_punti_temp   -->   dataframe delle forniture gas e pwr (da tcr)
          
  Output: df_pti          -->   dataframe
  
  """
  if mese_fine_cons == 0: # run su budget
    fine_mese_cons = datetime(anno -1, 12 , cl.monthrange(anno -1, 12)[1])
    
  else:
    fine_mese_cons = datetime(anno, int(mese_fine_cons), cl.monthrange(anno,int(mese_fine_cons))[1])
  
  # nei mesi di forecast i punti attivi considerati sono tutti quelli attivi nell'ultimo giorno di consuntivo
  df_punti_forecast = df_punti_temp.filter((f.col('cod_stato_pratica')!='ANNULLATO') & (f.col('data_decorrenza') <= fine_mese_cons) & (f.col('data_cessazione') >= fine_mese_cons))
  if anno >= 2021:
    df_punti_forecast = df_punti_forecast.withColumn('sist_fat',f.when(f.col('sist_fat')=='NETA BM', f.lit('XE')).otherwise(f.col('sist_fat')))
  df_punti_forecast = df_punti_forecast.groupBy('commodity','sist_fat').agg(f.countDistinct(f.col('fk_forn_fatt')))
  df_punti_forecast = df_punti_forecast.withColumnRenamed('count(DISTINCT fk_forn_fatt)', 'pti_anno_att')
  
  mesi = [i for i in range(13)][1:13]   
  for mese in mesi:
    if mese <= mese_fine_cons:
      inizio_mese = datetime(anno, int(mese), 1) 
      fine_mese = datetime(anno, int(mese), cl.monthrange(anno,int(mese))[1]) 
      df_punti_tmp = df_punti_temp.filter((f.col('cod_stato_pratica')!='ANNULLATO') & (f.col('data_decorrenza') <= fine_mese) & (f.col('data_cessazione') >= inizio_mese))
      if anno >= 2021:
        df_punti_tmp = df_punti_tmp.withColumn('sist_fat',f.when(f.col('sist_fat')=='NETA BM', f.lit('XE')).otherwise(f.col('sist_fat')))
      df_punti_tmp = df_punti_tmp.groupBy('commodity','sist_fat').agg(f.countDistinct(f.col('fk_forn_fatt')))
      df_punti_tmp = df_punti_tmp.withColumn('mese', f.lit(mese))
      df_punti_tmp = df_punti_tmp.withColumnRenamed('count(DISTINCT fk_forn_fatt)', 'pti_anno_att')
    else: 
      df_punti_tmp = df_punti_forecast
      df_punti_tmp = df_punti_tmp.withColumn('mese', f.lit(mese))
      
    if mese != mesi[0]:
      df_punti = df_punti.union(df_punti_tmp)
    else:
      df_punti = df_punti_tmp
  
  # aggiungo punti open (leggo da edl_wap e non da lab1_db.rfcf_open_punti perché in quest'ultimo c'è il non cumulato)
  DR_ds = spark.sql("""SELECT cast(max(ingestion_date) as timestamp) FROM edl_wap.ot_wap_punti_open_da_bpc_estrazione_dr_all_ver""").collect()[0][0]
  campi_DR = "prodotto_des, tempo, data_type_des, stato_des, mercato_des, segmento_des, importo"
  query_DR = " SELECT " + campi_DR + " FROM edl_wap.ot_wap_punti_open_da_bpc_estrazione_dr_all_ver "                                   
  DR = spark.sql(query_DR).filter(f.col('ingestion_date')==str(DR_ds)).filter(f.col('data_type_des').contains('PdR/PoD EoP')) 
  
  DR = c.elaborazione_open(DR,anno)
  DR = DR.withColumn('prodotto_des', f.when(f.col('prodotto_des')=='gas','GAS').otherwise('EE')).withColumnRenamed('prodotto_des','commodity_dr')
  if anno >= 2021:
    DR = DR.withColumn('sist_fat_dr',f.when(f.col('segmento_des').isin('RESIDENZIALI','MICROBUSINESS'),'NETA BR').otherwise('XE'))
  else:
    DR = DR.withColumn('sist_fat_dr',f.when(f.col('segmento_des').isin('RESIDENZIALI','MICROBUSINESS'),'NETA BR').otherwise('NETA BM'))
  
  DR_punti = DR.groupBy('commodity_dr', 'tempo_mese', 'sist_fat_dr').agg(f.sum(f.col('importo')).cast('int').alias('sum(importo)')) 

  cond = [DR_punti.tempo_mese==df_punti.mese, DR_punti.commodity_dr==df_punti.commodity, DR_punti.sist_fat_dr==df_punti.sist_fat]  
  df_pti = df_punti.join(DR_punti, cond, how = 'left').drop('tempo_mese', 'commodity_dr','sist_fat_dr')
  df_pti = df_pti.withColumn('punti_anno_att', f.coalesce(f.col('pti_anno_att'),f.lit(0))+f.coalesce(f.col('sum(importo)'),f.lit(0))) 
  df_pti = df_pti.drop('sum(importo)', 'pti_anno_att')
  
  return df_pti


def punti_anno_precedente(anno, df_punti_temp):
  """ Funzione per il conteggio dei punti distinti dell'anno precedente, raggruppati per mese, commodity e sistema di fatturazione
  
  Input:  anno           -->   anno precedente
          df_punti_temp  -->   dataframe delle forniture gas e pwr (da tcr)
          
  Output: dfr_punti      -->   dataframe
  
  """
  mesi = [i for i in range(13)][1:13]  
  for mese in mesi:
    inizio_mese = datetime(anno, int(mese), 1)
    fine_mese = datetime(anno, int(mese), cl.monthrange(anno,int(mese))[1]) 
    dfr_punti_tmp = df_punti_temp.filter((f.col('cod_stato_pratica')!='ANNULLATO') & (f.col('data_decorrenza') <= fine_mese) & (f.col('data_cessazione') >= inizio_mese))      
    if anno+1 >= 2021:
      dfr_punti_tmp = dfr_punti_tmp.withColumn('sist_fat',f.when(f.col('sist_fat')=='NETA BM', f.lit('XE')).otherwise(f.col('sist_fat')))
    dfr_punti_tmp = dfr_punti_tmp.groupBy('commodity','sist_fat').agg(f.countDistinct(f.col('fk_forn_fatt')))
    dfr_punti_tmp = dfr_punti_tmp.withColumn('mese', f.lit(mese))
    dfr_punti_tmp = dfr_punti_tmp.withColumnRenamed('count(DISTINCT fk_forn_fatt)', 'punti_anno_prec')
      
    if mese != mesi[0]:
      dfr_punti = dfr_punti.union(dfr_punti_tmp)
    else:
      dfr_punti = dfr_punti_tmp
      
  return dfr_punti


def previsione_bollo_att(spark, anno, id_run, mese_fine_cons, date_snapshot):
  """ Funzione per la creazione della tabella lab1_db.rfcf_previsione_bollo_attributes contenente il fatturato della 
      componente bollo per l'anno precedente e il conteggio mensile dei punti per l'anno attuale e precendente
  
  Input:  spark            -->   sessione spark stanziata
          anno             -->   anno attuale
          id_run           -->   id_run corrente
          mese_fine_cons   -->   mese fine consuntivo
          date_snapshot    -->   data_snapshot da analizzare
          
  Output: df_bollo         -->   dataframe
  
  """
  dataframe_bollo = dataframe_new(spark,"('IBOL')",anno,date_snapshot,mese_fine_cons)
  dataframe_bollo = dataframe_bollo.withColumn('annomese', f.concat(f.col('anno'),f.lpad(f.col('mese'),2,'0')))
  
  # considero XE nella previsione solo dal 2021
  if anno < 2021:
    dataframe_bollo = dataframe_bollo.filter(f.col('sist_fat') != 'XE')
    
  if anno >= 2021:
    dataframe_bollo = dataframe_bollo.withColumn('sist_fat',f.when(f.col('sist_fat')=='NETA BM', f.lit('XE')).otherwise(f.col('sist_fat')))
  
  # emesso dell'anno precedente: se non è completo (caso budget) prendo i mesi mancanti/incompleti dall'anno prima
  now = datetime.now()
  if mese_fine_cons == 0 and now.year == anno-1: # emesso incompleto/mancante
    df_bollo_retail = dataframe_bollo.filter(f.col('sist_fat') == 'NETA BR')
    df_bollo_middle = dataframe_bollo.filter(f.col('sist_fat').isin('NETA BM', 'XE'))
    max_annomese_retail = df_bollo_retail.select('annomese').rdd.max()[0]
    mese_max_emesso_retail = str(max_annomese_retail)[-2:]
    max_annomese_middle = df_bollo_middle.select('annomese').rdd.max()[0]
    mese_max_emesso_middle = str(max_annomese_middle)[-2:]
    df_bollo_retail = df_bollo_retail.filter((f.col('annomese') < max_annomese_retail) & (f.col('annomese') >= str(anno-2)+str(mese_max_emesso_retail)))
    df_bollo_middle = df_bollo_middle.filter((f.col('annomese') < max_annomese_middle) & (f.col('annomese') >= str(anno-2)+str(mese_max_emesso_middle)))
    df_bollo = df_bollo_retail.unionByName(df_bollo_middle)\
                              .drop('annomese')\
                              .withColumn('anno', f.lit(anno-1))
  else:
    df_bollo = dataframe_bollo.filter(f.col('anno')== anno-1) 
  
  df_bollo = df_bollo.groupBy('anno','mese','commodity','sist_fat').sum('valore_comp').withColumnRenamed('sum(valore_comp)', 'IBOL_anno_prec')
  
  df_bollo = df_bollo.withColumn('id_run', f.lit(str(id_run)))
  df_bollo = df_bollo.withColumn('anno_att', f.lit(str(anno)))
  df_bollo = df_bollo.withColumnRenamed('anno','anno_prec')

  campi_forn = "commodity, sist_fat, data_decorrenza, data_cessazione, cod_stato_pratica, fk_forn_fatt"
  # Nota: forniture tcr conterranno già xe
                                       
  query_forn_pwr = " SELECT " + campi_forn + " from lab1_db.rfcf_vw_forniture_pwr" 
  query_forn_gas = " SELECT " + campi_forn + " from lab1_db.rfcf_vw_forniture_gas"
 
  df_punti_pwr_temp = spark.sql(query_forn_pwr).filter(f.col('sist_fat')!='ND')
  df_punti_gas_temp = spark.sql(query_forn_gas).filter(f.col('sist_fat')!='ND')
  df_punti_temp = df_punti_gas_temp.union(df_punti_pwr_temp).dropDuplicates()
  
  df_punti_anno_prec = punti_anno_precedente(anno-1, df_punti_temp)
  df_punti_anno = punti_anno_attuale(spark, anno, mese_fine_cons, df_punti_temp).drop('sum(importo)', 'pti_anno_att')
  df_bollo = df_bollo.join(df_punti_anno_prec, ['mese','commodity','sist_fat'], 'left')   
  df_bollo = df_bollo.join(df_punti_anno, ['mese','commodity','sist_fat'], 'left')
  
  return df_bollo


def previsione_indennizzo_att(spark, anno, id_run, date_snapshot, mese_fine_cons, target_annuale_retail, target_annuale_middle): 
  """ Funzione per la creazione della tabella lab1_db.rfcf_previsione_indennizzo_attributes contenente i target BM e BR,
      il fatturato della componente indennizzo per l'anno precedente e le relative percentuali mensili rispetto al totale
  
  Input:  spark                  -->   sessione spark stanziata
          anno                   -->   anno attuale
          id_run                 -->   id_run corrente
          date_snapshot          -->   data_snapshot da analizzare
          mese_fine_cons         -->   mese_fine_consuntivo
          target_annuale_retail  -->   target annuale BR
          target_annuale_middle  -->   target annuale BM
          
  Output: df_indennizzo          -->   dataframe
  
  """
  dataframe_indennizzo = dataframe_new(spark,"('INDV')",anno,date_snapshot,mese_fine_cons)
  dataframe_indennizzo = dataframe_indennizzo.withColumn('annomese', f.concat(f.col('anno'),f.lpad(f.col('mese'),2,'0')))
  
  # considero XE nella previsione solo dal 2021
  if anno < 2021:
    dataframe_indennizzo = dataframe_indennizzo.filter(f.col('sist_fat') != 'XE')
  
  if anno >= 2021:
    dataframe_indennizzo = dataframe_indennizzo.withColumn('sist_fat',f.when(f.col('sist_fat')=='NETA BM', f.lit('XE')).otherwise(f.col('sist_fat')))
  
  # emesso dell'anno precedente: se non è completo (caso budget) prendo i mesi mancanti/incompleti dall'anno prima
  now = datetime.now()
  if mese_fine_cons == 0 and now.year == anno-1: # emesso incompleto/mancante
    df_indennizzo_retail = dataframe_indennizzo.filter(f.col('sist_fat') == 'NETA BR')
    df_indennizzo_middle = dataframe_indennizzo.filter(f.col('sist_fat').isin('NETA BM', 'XE'))
    max_annomese_retail = df_indennizzo_retail.select('annomese').rdd.max()[0]
    mese_max_emesso_retail = str(max_annomese_retail)[-2:]
    max_annomese_middle = df_indennizzo_middle.select('annomese').rdd.max()[0]
    mese_max_emesso_middle = str(max_annomese_middle)[-2:]
    df_indennizzo_retail = df_indennizzo_retail.filter((f.col('annomese') < max_annomese_retail) & (f.col('annomese') >= str(anno-2)+str(mese_max_emesso_retail)))
    df_indennizzo_middle = df_indennizzo_middle.filter((f.col('annomese') < max_annomese_middle) & (f.col('annomese') >= str(anno-2)+str(mese_max_emesso_middle)))
    df_indennizzo = df_indennizzo_retail.unionByName(df_indennizzo_middle)\
                              .withColumn('anno', f.lit(anno-1))
  else:
    df_indennizzo = dataframe_indennizzo.filter(f.col('anno')== anno-1) 
  
  df_indennizzo = df_indennizzo.groupBy('anno','mese','commodity','sist_fat').sum('valore_comp') \
    .withColumnRenamed('sum(valore_comp)', 'INDV_anno_prec')
    
  # aggiungo un sist_fat aggregato
  df_indennizzo = df_indennizzo.withColumn('sist_fat_agg', f.when(f.col('sist_fat')=='NETA BR', f.lit('BR')).otherwise(f.lit('BM/XE')))
  
  df_indennizzo = df_indennizzo.withColumn('id_run', f.lit(str(id_run)))
  df_indennizzo = df_indennizzo.withColumnRenamed('anno','anno_prec')
  df_indennizzo = df_indennizzo.withColumn('anno_att', f.lit(str(anno)))
  
  # calcolo percentuali (su totali raggruppati per sist_fat)
  tot = df_indennizzo.groupBy('sist_fat_agg').sum('INDV_anno_prec') \
                   .withColumnRenamed('sum(INDV_anno_prec)','INDV_tot_anno_prec_sistfat')  
  df_indennizzo = df_indennizzo.join(tot,'sist_fat_agg')
  df_indennizzo = df_indennizzo.withColumn('importo_percentuale', f.col('INDV_anno_prec')/f.col('INDV_tot_anno_prec_sistfat')) 
  
  df_indennizzo = df_indennizzo.withColumn('target_br_INDV_anno_att', f.lit(-1 * target_annuale_retail)) \
                               .withColumn('target_bm_INDV_anno_att', f.lit(-1 * target_annuale_middle))
  
  return df_indennizzo


def fatturato_no_componenti_anno_attuale(spark,anno,date_snapshot): 
  """ Funzione per il calcolo del fatturato (previsivo) totale delle sole componenti di ricavo relativo all'anno attuale,
      per mese, commodity e sistema di fatturazione
  
  Input:  spark                  -->   sessione spark stanziata
          anno                   -->   anno attuale 
          date_snapshot          -->   data_snapshot da analizzare
          
  Output: df_fatturato_anno_att  -->   dataframe
  
  """
  # rateo e forecast (no Ret)
  df_rateo_forecast = spark.sql("SELECT \
    commodity_tcr as commodity, month(data_fatturazione) as mese, sist_fat, sum(coalesce(churn,1)*valore_comp) as fatturato \
    FROM lab1_db.rfcf_calendario_nes \
    where year(data_fatturazione)= " + str(anno) + " and commodity_tcr <> 'LAVORI' \
    and componente_tcr not in ('CONSUMO','DELTA_CONSUMO','DELTA','DELTA_RICAVI','DELTA_ACCISE','DELTA_IVA') \
    and sist_fat not in ('Ret','Lodestar','Mediana') \
    group by commodity_tcr, month(data_fatturazione), sist_fat" )
  # rateo e forecast Ret
  df_rateo_forecast_ret = spark.sql("SELECT \
    commodity_tcr as commodity, month(data_fatturazione) as mese, macrosegmento, coalesce(churn,1)*valore_comp as fatturato \
    FROM lab1_db.rfcf_calendario_nes \
    where year(data_fatturazione)= " + str(anno) + " and commodity_tcr <> 'LAVORI' \
    and componente_tcr not in ('CONSUMO','DELTA_CONSUMO','DELTA','DELTA_RICAVI','DELTA_ACCISE','DELTA_IVA') \
    and sist_fat in ('Ret','Lodestar','Mediana')") 
  if anno >= 2021:
    df_rateo_forecast_ret = df_rateo_forecast_ret.withColumn('sist_fat',f.when(f.col('macrosegmento').isin('RESIDENZIALI','MICROBUSINESS'),'NETA BR').otherwise('XE')).drop('macrosegmento')
  else:
    df_rateo_forecast_ret = df_rateo_forecast_ret.withColumn('sist_fat',f.when(f.col('macrosegmento').isin('RESIDENZIALI','MICROBUSINESS'),'NETA BR').otherwise('NETA BM')).drop('macrosegmento')
  df_rateo_forecast_ret = df_rateo_forecast_ret.groupBy('mese','commodity','sist_fat').sum('fatturato').withColumnRenamed('sum(fatturato)','fatturato')
  df_rateo_forecast = df_rateo_forecast.unionByName(df_rateo_forecast_ret)
  
  # fatturato BR/BM (no Ret)
  df_fatturato = spark.sql("SELECT \
    commodity, month(data_emissione) as mese, sist_fat, sum(valore_comp) as fatturato \
    FROM lab1_db.rfcf_fatturato \
    where flg_ricavo = 'S' and year(data_emissione)= " + str(anno) + " and commodity <> 'LAVORI' and componente_tcr not like '%CONSUMO%' \
    and sist_fat not in ('Ret','Lodestar','Mediana') \
    group by commodity, month(data_emissione), sist_fat" )
  # NOTA: no macrosegmento e tipo_regime in lab1_db.rfcf_fatturato, eventualmente prendere Ret da aggregato. 
  # Parte Ret del fatturato (fatturato in generale) non impattante dato che i mesi consuntivo sono sostituiti dall'emesso (valutare di troncare previsone ai soli mesi forecast)

  # fatturato XE
  df_fatturato_XE = spark.sql("SELECT \
    commodity, month(data_emissione) as mese, sist_fat, sum(valore_comp) as fatturato \
    FROM lab1_db.rfcf_fatturato_xe \
    where flg_ricavo = 'S' and year(data_emissione)= " + str(anno) + " and commodity <> 'LAVORI' and componente_tcr not like '%CONSUMO%' \
    group by commodity, month(data_emissione), sist_fat" )
  
  # aggiungo ricavi open 
  CE = spark.sql("SELECT \
     commodity, month(data_fatturazione) as mese, sist_fat, sum(valore_percentuale) as fatturato \
     FROM lab1_db.rfcf_open_consumi \
     group by commodity, month(data_fatturazione), sist_fat ")
  CE = CE.withColumn('commodity', f.when(f.col('commodity')=='gas','GAS').otherwise('EE')) 
  
  
  df_fatturato_anno_att = df_rateo_forecast.unionByName(df_fatturato).unionByName(df_fatturato_XE).unionByName(CE)
  df_fatturato_anno_att = df_fatturato_anno_att.withColumn('fatturato', f.coalesce(f.col('fatturato'),f.lit(0)))
  if anno >= 2021:
    df_fatturato_anno_att = df_fatturato_anno_att.withColumn('sist_fat',f.when(f.col('sist_fat')=='NETA BM', f.lit('XE')).otherwise(f.col('sist_fat')))
  df_fatturato_anno_att = df_fatturato_anno_att.groupBy('mese','commodity','sist_fat').sum('fatturato').withColumnRenamed('sum(fatturato)','fatturato_anno_att')
  
  return df_fatturato_anno_att


def componenti_ricavo(spark,date_snapshot):
  """ Funzione che recupera dal mapping tcr i tipo_riga associati alle componenti di ricavo
  
  Input:  spark           -->   sessione spark stanziata
          date_snapshot   -->   data_snapshot da analizzare
          
  Output: df_comp_ricavo  -->   dataframe
  
  """

  df_comp_ricavo_pwr = spark.sql("SELECT \
    a.map_tipo_riga as tipo_riga \
    FROM lab1_db.rfcf_vw_mapping_voci_contabili_pwr a \
    join lab1_db.rfcf_vw_mapping_tcr_pwr b on (a.map_componente = b.map_componente) \
    WHERE b.map_flag_comp_ricavi = 'S' " )
  df_comp_ricavo_gas = spark.sql("SELECT \
    a.map_tipo_riga as tipo_riga \
    FROM lab1_db.rfcf_vw_mapping_voci_contabili_gas a \
    join lab1_db.rfcf_vw_mapping_tcr_gas b on (a.map_componente = b.map_componente) \
    WHERE b.map_flag_comp_ricavi = 'S' " )
  
  df_comp_ricavo = df_comp_ricavo_pwr.unionByName(df_comp_ricavo_gas).dropDuplicates()
    
  return df_comp_ricavo


def componenti_ricavo_xe(spark,date_snapshot):
  """ Funzione che, passando per il mapping tcr, recupera dal mapping xe i tipo_riga associati alle componenti di ricavo
  
  Input:  spark           -->   sessione spark stanziata
          date_snapshot   -->   data_snapshot da analizzare
          
  Output: df_comp_ricavo_xe  -->   dataframe
  
  """
  campi_mapping_xe = "map_tipo_riga as tipo_riga, map_componente "
  df_mapping_xe_pwr = spark.sql("SELECT " + campi_mapping_xe + " from lab1_db.rfcf_mapping_xe_pwr ")
  df_mapping_xe_gas = spark.sql("SELECT " + campi_mapping_xe + " from lab1_db.rfcf_mapping_xe_gas ")
  df_mapping_xe = df_mapping_xe_pwr.unionByName(df_mapping_xe_gas).dropDuplicates()
  
  campi_mapping_tcr = "map_flag_comp_ricavi, map_componente"                                
  query_mapping_pwr = " SELECT " + campi_mapping_tcr + " from lab1_db.rfcf_vw_mapping_tcr_pwr where map_flag_comp_ricavi = 'S'"
  query_mapping_gas = " SELECT " + campi_mapping_tcr + " from lab1_db.rfcf_vw_mapping_tcr_gas where map_flag_comp_ricavi = 'S'"                                    
  df_mapping_tcr_pwr = spark.sql(query_mapping_pwr)
  df_mapping_tcr_gas = spark.sql(query_mapping_gas)
  df_mapping_tcr = df_mapping_tcr_pwr.unionByName(df_mapping_tcr_gas).drop('map_flag_comp_ricavi').dropDuplicates()
  
  df_comp_ricavo_xe = df_mapping_xe.join(df_mapping_tcr, 'map_componente').drop('map_componente')
  
  return df_comp_ricavo_xe

  
def fatturato_no_componenti_anno_precedente(spark, anno, df_fatt_anno_att, date_snapshot):
  """ Funzione per il calcolo del fatturato totale delle sole componenti di ricavo relativo all'anno precedente,
      per mese, commodity e sistema di fatturazione
  
  Input:  spark             -->   sessione spark stanziata
          anno              -->   anno precedente 
          df_fatt_anno_att  -->   output della funzione fatturato_no_componenti_anno_attuale
          date_snapshot     -->   data_snapshot da analizzare
          
  Output: df_fatturato      -->   dataframe
  
  """
  df_tipo_riga_ricavi = componenti_ricavo(spark,date_snapshot) # tipo_riga associati a componenti di ricavo dal mapping tcr
  
  campi_competenziato = "tipo_riga, commodity, sist_fat, cast(substring(annomese,1,4) as int) as anno, cast(substring(annomese,5,2) as int) as mese, valore_comp"
  stringa_where_comp = " WHERE cast(substring(annomese,1,4) as int) = " + str(anno) +" and commodity <> 'LAVORI' and componente not like '%CONSUMO%'"
  query_competenziato = " SELECT " + campi_competenziato + " FROM lab1_db.rfcf_dett_fatt_comp " + stringa_where_comp
  df = spark.sql(query_competenziato)
  df = df.withColumn('sist_fat', f.when(f.col('sist_fat')=='3','NETA BR').when(f.col('sist_fat')=='8','NETA BM').otherwise('ND')) 
  if anno+1 >= 2021:
    df = df.withColumn('sist_fat',f.when(f.col('sist_fat')=='NETA BM', f.lit('XE')).otherwise(f.col('sist_fat')))
    
  df = df.join(df_tipo_riga_ricavi, 'tipo_riga').drop('tipo_riga')
  
  #df_fatt_anno_prec = df.groupBy('mese','commodity','sist_fat').sum('valore_comp').withColumnRenamed('sum(valore_comp)','fatturato_anno_prec')
  
  
  # aggiungo fatturato xe 
  df_tipo_riga_ricavi_xe = componenti_ricavo_xe(spark,date_snapshot) # tipo_riga associati a componenti di ricavo dal mapping xe
  
  campi_competenziato_xe = "codice_sotto_testata, tipo_riga, commodity, sist_fat, valore_comp"
  stringa_where_comp_xe = " WHERE commodity <> 'LAVORI' and tipo_riga not like '%CONSUMO%'"
  df_xe = spark.sql("SELECT " + campi_competenziato_xe + " FROM lab1_db.rfcf_dett_fatt_comp_xe " + stringa_where_comp_xe) 
  
  df_xe = df_xe.join(df_tipo_riga_ricavi_xe, 'tipo_riga').drop('tipo_riga')

  # recupero anno e mese emissione dalla testata xe
  campi_sottotestata_xe = "codice_sottotestata as codice_sotto_testata, codice_fattura, edl_last_modify"
  campi_testata_xe = "num_bolletta as codice_fattura, cast(substring(dat_emissione_bol,1,4) as int) as anno, cast(substring(dat_emissione_bol,5,2) as int) as mese, edl_last_modify "
  
  w_let = Window.partitionBy(f.col('codice_sotto_testata')).orderBy(f.col('edl_last_modify').desc())
  sottotestata_xe = spark.sql("SELECT distinct " + campi_sottotestata_xe  + " from edl_int.br_tcr_sottotestatabollette") \
                         .withColumn('edl_last_modify', f.to_date('edl_last_modify', 'yyyy-MM-dd')).withColumn('edl_last_modify', f.to_timestamp('edl_last_modify', 'yyyy-MM-dd')) \
                         .withColumn('rank', f.rank().over(w_let)).filter(f.col('rank')==1).drop('edl_last_modify','rank')
                         #.withColumn('commodity', f.when(f.col('commodity')=='PWR','EE').otherwise(f.col('commodity')))
  w_let = Window.partitionBy(f.col('codice_fattura')).orderBy(f.col('edl_last_modify').desc())
  testata_xe = spark.sql("SELECT distinct " + campi_testata_xe + " from edl_int.br_tcr_testatabollette") \
                    .withColumn('edl_last_modify', f.to_date('edl_last_modify', 'yyyy-MM-dd')).withColumn('edl_last_modify', f.to_timestamp('edl_last_modify', 'yyyy-MM-dd')) \
                    .withColumn('rank', f.rank().over(w_let)).filter(f.col('rank')==1).drop('edl_last_modify','rank')
  df_xe = df_xe.join(sottotestata_xe,'codice_sotto_testata', 'left')
  df_xe = df_xe.join(testata_xe,'codice_fattura', 'left').filter(f.col('anno') == anno).drop('codice_fattura','codice_sotto_testata')
  
  #df_fatt_anno_prec_xe = df_xe.groupBy('mese','commodity','sist_fat').sum('valore_comp').withColumnRenamed('sum(valore_comp)','fatturato_anno_prec')
 
  df_fatturato = df.unionByName(df_xe)
  df_fatturato = df_fatturato.groupBy('mese','commodity','sist_fat').sum('valore_comp').withColumnRenamed('sum(valore_comp)','fatturato_anno_prec')
  
  df_fatturato = df_fatturato.join(df_fatt_anno_att, ['mese','commodity','sist_fat'])
  
  df_fatturato = df_fatturato.withColumn('anno_att', f.lit(anno +1))
  df_fatturato = df_fatturato.withColumn('anno_prec', f.lit(anno)) #.withColumn('mese', f.col('mese').cast('string'))
   
  return df_fatturato


def fatturato_no_componenti_anno_precedente_new(spark, anno, mese_fine_cons, df_fatt_anno_att, date_snapshot):
  """ Funzione per il calcolo del fatturato totale delle sole componenti di ricavo relativo all'anno precedente,
      per mese, commodity e sistema di fatturazione
  
  Input:  spark             -->   sessione spark stanziata
          anno              -->   anno precedente 
          mese_fine_cons    -->   mese_fine_consuntivo
          df_fatt_anno_att  -->   output della funzione fatturato_no_componenti_anno_attuale
          date_snapshot     -->   data_snapshot da analizzare
          
  Output: df_fatturato      -->   dataframe
  
  """
  # calcolo le componenti di ricavo per ogni commodity (sia retail che middle)
  comp_ricavo_pwr = spark.sql("""SELECT distinct map_componente as componente_tcr FROM lab1_db.rfcf_vw_mapping_tcr_pwr WHERE map_flag_comp_ricavi = 'S' """)
  comp_ricavo_gas = spark.sql("""SELECT distinct map_componente as componente_tcr FROM lab1_db.rfcf_vw_mapping_tcr_gas WHERE map_flag_comp_ricavi = 'S' """)                             
  comp_ric_all = comp_ricavo_pwr.unionByName(comp_ricavo_gas).dropDuplicates()

  # definisco anno_partenza in modo da avere sempre due anni di emesso 
  now = datetime.now()
  if mese_fine_cons == 0 and now.year == anno:
    anno_partenza = anno-1
  else:
    anno_partenza = anno
    
  stringa_where =  " WHERE year(data_emissione) >= " + str(anno_partenza) + " and componente_tcr not like '%CONSUMO%'"
  campi_emesso_aggregato = "componente_tcr, commodity_tcr as commodity, sist_fat, year(data_emissione) as anno, month(data_emissione) as mese, valore_comp"
                                       
  df_emesso_aggregato_pwr = spark.sql(" SELECT " + campi_emesso_aggregato+ " FROM lab1_db.rfcf_fatturato_emesso_pwr " + stringa_where)
  df_emesso_aggregato_gas = spark.sql(" SELECT " + campi_emesso_aggregato + " FROM lab1_db.rfcf_fatturato_emesso_gas " + stringa_where)

  # emesso delle sole componenti di ricavo
  df_em_pwr = df_emesso_aggregato_pwr.join(comp_ricavo_pwr ,'componente_tcr')
  df_em_gas = df_emesso_aggregato_gas.join(comp_ricavo_gas ,'componente_tcr')

  df_fatturato = df_em_pwr.unionByName(df_em_gas).withColumn('annomese', f.concat(f.col('anno'),f.lpad(f.col('mese'),2,'0')))\
                          .withColumn('sist_fat', f.when(f.col('sist_fat')=='3','NETA BR').when(f.col('sist_fat')=='8','NETA BM').otherwise('XE'))
  if anno+1 < 2021:
    df_fatturato = df_fatturato.filter(f.col('sist_fat') != 'XE')
  
  if anno+1 >= 2021: # a partire dalla previsione 2021 il sistema di fatturazione Middle è XE
    df_fatturato = df_fatturato.withColumn('sist_fat',f.when(f.col('sist_fat')=='NETA BM', f.lit('XE')).otherwise(f.col('sist_fat')))
  
  # emesso dell'anno precedente: se non è completo (caso budget) prendo i mesi mancanti/incompleti dall'anno prima
  now = datetime.now()
  if mese_fine_cons == 0 and now.year == anno: # emesso incompleto/mancante
    df_fatturato_retail = df_fatturato.filter(f.col('sist_fat') == 'NETA BR')
    df_fatturato_middle = df_fatturato.filter(f.col('sist_fat').isin('NETA BM', 'XE'))
    max_annomese_retail = df_fatturato_retail.select('annomese').rdd.max()[0]
    mese_max_emesso_retail = str(max_annomese_retail)[-2:]
    max_annomese_middle = df_fatturato_middle.select('annomese').rdd.max()[0]
    mese_max_emesso_middle = str(max_annomese_middle)[-2:]
    df_fatturato_retail = df_fatturato_retail.filter((f.col('annomese') < max_annomese_retail) & (f.col('annomese') >= str(anno-1)+str(mese_max_emesso_retail)))
    df_fatturato_middle = df_fatturato_middle.filter((f.col('annomese') < max_annomese_middle) & (f.col('annomese') >= str(anno-1)+str(mese_max_emesso_middle)))
    df_fatturato = df_fatturato_retail.unionByName(df_fatturato_middle)\
                              .withColumn('anno', f.lit(anno))
      
  else:
    df_fatturato = df_fatturato.filter(f.col('anno')== anno)  
  
  df_fatturato = df_fatturato.groupBy('mese','commodity','sist_fat').sum('valore_comp').withColumnRenamed('sum(valore_comp)','fatturato_anno_prec')
  
  df_fatturato = df_fatturato.join(df_fatt_anno_att, ['mese','commodity','sist_fat'])
  
  df_fatturato = df_fatturato.withColumn('anno_att', f.lit(anno +1))
  df_fatturato = df_fatturato.withColumn('anno_prec', f.lit(anno)) #.withColumn('mese', f.col('mese').cast('string'))
  
  return df_fatturato


def previsione_comp_no_ricavo_att(spark, anno, id_run, mese_fine_cons, lista_componenti, date_snapshot):
  """ Funzione per la creazione della tabella lab1_db.rfcf_previsione_comp_no_ric_attributes che per mese, commodity e 
      sistema di fatturazione contiene il fatturato delle componenti DEPC, CMOR, IMOR e SPSO per l'anno precedente,
      il fatturato totale delle sole componenti di ricavo dell'anno attuale e del precendente
  
  Input:  spark            -->   sessione spark stanziata
          anno             -->   anno attuale
          id_run           -->   id_run corrente
          mese_fine_cons   -->   mese_fine_consuntivo
          lista_componenti -->   lista delle quattro componenti 
          date_snapshot    -->   data_snapshot da analizzare
          
  Output: df               -->   dataframe
  
  """
  df_fatt_anno_att = fatturato_no_componenti_anno_attuale(spark, anno, date_snapshot)
  df = fatturato_no_componenti_anno_precedente_new(spark, anno-1, mese_fine_cons, df_fatt_anno_att, date_snapshot)
  
  for i in lista_componenti:
    comp = '("' +i+ '")'
    df_comp_anno_prec = dataframe_new(spark, comp, anno, date_snapshot, mese_fine_cons)
    df_comp_anno_prec = df_comp_anno_prec.withColumn('annomese', f.concat(f.col('anno'),f.lpad(f.col('mese'),2,'0')))
    
    # considero XE nella previsione solo dal 2021
    if anno < 2021:
      df_comp_anno_prec = df_comp_anno_prec.filter(f.col('sist_fat') != 'XE')
   
    if anno >= 2021:
      df_comp_anno_prec = df_comp_anno_prec.withColumn('sist_fat',f.when(f.col('sist_fat')=='NETA BM', f.lit('XE')).otherwise(f.col('sist_fat')))
      
    # emesso dell'anno precedente: se non è completo (caso budget) prendo i mesi mancanti/incompleti dall'anno prima
    now = datetime.now()
    if mese_fine_cons == 0 and now.year == anno-1: # emesso incompleto/mancante
      df_comp_anno_prec_retail = df_comp_anno_prec.filter(f.col('sist_fat') == 'NETA BR')
      df_comp_anno_prec_middle = df_comp_anno_prec.filter(f.col('sist_fat').isin('NETA BM', 'XE'))
      max_annomese_retail = df_comp_anno_prec_retail.select('annomese').rdd.max()[0]
      mese_max_emesso_retail = str(max_annomese_retail)[-2:]
      max_annomese_middle = df_comp_anno_prec_middle.select('annomese').rdd.max()[0]
      mese_max_emesso_middle = str(max_annomese_middle)[-2:]
      df_comp_anno_prec_retail = df_comp_anno_prec_retail.filter((f.col('annomese') < max_annomese_retail) & (f.col('annomese') >= str(anno-2)+str(mese_max_emesso_retail)))
      df_comp_anno_prec_middle = df_comp_anno_prec_middle.filter((f.col('annomese') < max_annomese_middle) & (f.col('annomese') >= str(anno-2)+str(mese_max_emesso_middle)))
      df_comp_anno_prec = df_comp_anno_prec_retail.unionByName(df_comp_anno_prec_middle)\
                              .withColumn('anno', f.lit(anno-1))
    else:
      df_comp_anno_prec = df_comp_anno_prec.filter(f.col('anno')== anno-1)       
      
    df_comp_anno_prec = df_comp_anno_prec.groupBy('mese','commodity','sist_fat').sum('valore_comp').withColumnRenamed('sum(valore_comp)',i + "_anno_prec")
    
    df = df.join(df_comp_anno_prec, ['mese', 'commodity','sist_fat'], how='left')
  df = df.withColumn('id_run', f.lit(str(id_run)))
  
  return df


def previsione_comp_no_ricavo(spark, anno, id_run, lista_componenti):
  """ Funzione per la previsione di fatturato delle componenti non di ricavo DEPC, CMOR, IMOR, SPSO, IBOL, INDD, PREB e per i SOPS.
      La previsione è ottenuta applicando le formule individuate durante la fase di studio del modello FCF
  
  Input:  spark             -->   sessione spark stanziata
          anno              -->   anno attuale
          id_run            -->   id_run corrente
          lista_componenti  -->   lista delle componenti oggetto di lab1_db.rfcf_previsione_comp_no_ric_attributes
          
  Output: df_all            -->   dataframe
  
  """
  aggiunta_mese_udf = f.udf(lambda x,y: aggiunta_mese(x,y),TimestampType())
  df_attributes = spark.sql("""SELECT * FROM lab1_db.rfcf_previsione_comp_no_ric_attributes""")
  df_gas = df_attributes.filter(f.col('commodity') == 'GAS')
  df_pwr = df_attributes.filter(f.col('commodity') == 'EE')
  
  commodity_df = [df_gas, df_pwr]
  for df in commodity_df: 
    for i in lista_componenti:
      try:  
        if i == 'DEPC':
          driver_retail = sum_col(df.filter(f.col('sist_fat')=='NETA BR'), i+'_anno_prec')/sum_col(df.filter(f.col('sist_fat')=='NETA BR'),'fatturato_anno_prec')
          driver_middle = sum_col(df.filter(f.col('sist_fat').isin('NETA BM','XE')), i+'_anno_prec')/sum_col(df.filter(f.col('sist_fat').isin('NETA BM','XE')),'fatturato_anno_prec')
          df_more = df.withColumn('driver_'+i, f.when(f.col('sist_fat')=='NETA BR',f.lit(driver_retail)).otherwise(f.lit(driver_middle)))
        elif (i == 'SPSO' and anno == 2020):  
          df_SPSM_tmp = df.filter(f.col('mese')>= 7)   
          driver_retail = sum_col(df_SPSM_tmp.filter(f.col('sist_fat')=='NETA BR'), i+'_anno_prec')/sum_col(df_SPSM_tmp.filter(f.col('sist_fat')=='NETA BR'),'fatturato_anno_prec')
          driver_middle = sum_col(df_SPSM_tmp.filter(f.col('sist_fat').isin('NETA BM','XE')), i+'_anno_prec')/sum_col(df_SPSM_tmp.filter(f.col('sist_fat').isin('NETA BM','XE')),'fatturato_anno_prec')
          df_more = df.withColumn('driver_'+i, f.when(f.col('sist_fat')=='NETA BR',f.lit(driver_retail)).otherwise(f.lit(driver_middle)))
        else:
          df_more = df.withColumn('driver_'+i, f.col(i+'_anno_prec') / f.col('fatturato_anno_prec'))
      except TypeError:
          df_more = df.withColumn('driver_'+i, f.lit(None))
        
      if df_more.select('driver_'+i) != None : 
        df_more = df_more.withColumn('prev_'+i, f.col('driver_'+i) * f.col('fatturato_anno_att'))
      else: 
        df_more = df_more.withColumn('prev_'+i, f.lit(None))

      if i != lista_componenti[0]:  
        prev_tmp = df_more.select(f.col('mese'),f.col('prev_'+i),f.col('commodity'),f.col('sist_fat'))
        prev_tmp = prev_tmp.withColumnRenamed('prev_'+i,'valore').withColumn('componente_tcr', f.lit(i)).withColumn('fk_forn_fatt', f.lit('FIT#'+i))
        df_prev = df_prev.unionByName(prev_tmp)
      else:
        df_prev = df_more.select(f.col('mese'),f.col('prev_'+i),f.col('commodity'),f.col('sist_fat'))
        df_prev = df_prev.withColumnRenamed('prev_'+i,'valore').withColumn('componente_tcr', f.lit(i)).withColumn('fk_forn_fatt', f.lit('FIT#'+i))
  
    if (df != commodity_df[0]):
        df_all = df_all.unionByName(df_prev)
    else:
        df_all = df_prev
    
    # CMOR BR GAS 2019 da gen a mag non presente, per il 2020 mettiamo la media del CMOR 2019 giu-dic
    if anno == 2020:
      cond = ((f.col('componente_tcr')=='CMOR')&(f.col('commodity')=='GAS')&(f.col('sist_fat')=='NETA BR'))
      cond_mesi = [1,2,3,4,5]
      df_all = df_all.withColumn('valore', f.when((cond&(f.col('mese').isin(cond_mesi))), 114803).otherwise(f.col('valore')) )
        
    
  bollo_attributes = spark.sql("""SELECT 
   cast(mese as int) mese, commodity, sist_fat, IBOL_anno_prec, punti_anno_att, punti_anno_prec
   FROM lab1_db.rfcf_previsione_bollo_attributes""")
  
  df_bollo = bollo_attributes.withColumn('valore', (f.col('IBOL_anno_prec')/f.col('punti_anno_prec')) * f.col('punti_anno_att'))
  df_bollo = df_bollo.drop('IBOL_anno_prec','punti_anno_att','punti_anno_prec').withColumn('componente_tcr', f.lit('IBOL')).withColumn('fk_forn_fatt', f.lit('FIT#IBOL'))
    
  df_all = df_all.unionByName(df_bollo)
  
  
  indennizzo_attributes = spark.sql("""SELECT 
   cast(mese as int) mese, commodity, sist_fat, importo_percentuale, target_br_INDV_anno_att, target_bm_INDV_anno_att
   FROM lab1_db.rfcf_previsione_indennizzo_attributes""")
  
  # previsione e aggiunta di alcuni campi
  df_indennizzo = indennizzo_attributes.withColumn('valore', f.when(f.col('sist_fat')=='NETA BR',f.col('importo_percentuale') * f.col('target_br_INDV_anno_att')) \
                                                              .otherwise(f.col('importo_percentuale') * f.col('target_bm_INDV_anno_att')))
  df_indennizzo = df_indennizzo.drop('importo_percentuale','target_br_INDV_anno_att','target_bm_INDV_anno_att').withColumn('componente_tcr', f.lit('INDV')).withColumn('fk_forn_fatt', f.lit('FIT#INDV'))
  
  df_all = df_all.unionByName(df_indennizzo)


  df_service_asis = spark.sql("""SELECT 
   cast(mese as int) mese, commodity, sist_fat, tipo, fatturato_last12, fatture_last12, stima_fatture_anno_att, coeff_corr
   FROM lab1_db.rfcf_previsione_SOPS_ASIS_attributes""")
  
  df_service_asis = df_service_asis.withColumn('valore', (f.col('fatturato_last12')/f.col('fatture_last12'))*f.col('stima_fatture_anno_att')*f.col('coeff_corr'))
  df_service_asis = df_service_asis.groupBy('mese','tipo','commodity','sist_fat').sum('valore').withColumnRenamed('sum(valore)','valore')
  df_service_asis = df_service_asis.withColumnRenamed('tipo', 'componente_tcr').withColumn('fk_forn_fatt', f.lit('FIT#SOPS')).drop('tipo')
  
  df_all = df_all.unionByName(df_service_asis)
  
  
  df_service_tobe = spark.sql("""SELECT 
   anno, cast(mese as int) mese, commodity, sist_fat, servizio, tipo, trend_mensile, cast(prezzo as int), delta
   FROM lab1_db.rfcf_previsione_SOPS_TOBE_attributes""")
  
  df_service_tobe = df_service_tobe.withColumn('valore', f.col('trend_mensile')*f.col('prezzo'))
  
  df_service_tobe = df_service_tobe.withColumn('data_tmp', f.to_timestamp(f.to_date(f.concat_ws('-', 'anno', 'mese', f.lit(1))))).withColumn('data_tmp', aggiunta_mese_udf('data_tmp','delta')).filter(f.year(f.col('data_tmp')) == anno).drop('anno_tmp')  
  df_service_tobe = df_service_tobe.withColumn('mese', f.month(f.col('data_tmp')))
  
  df_service_tobe = df_service_tobe.groupBy('mese','commodity','tipo','sist_fat').sum('valore').withColumnRenamed('sum(valore)','valore')
  
  df_service_tobe = df_service_tobe.withColumn('componente_tcr', f.col('tipo')).withColumn('fk_forn_fatt', f.lit('FIT#SOPS')).drop('tipo')
  
  df_all = df_all.unionByName(df_service_tobe)  
  
  
  # piani di rientro
  df_service_tobe2 = spark.sql("""SELECT 
   anno, cast(mese as int) mese, sist_fat, servizio, tipo, trend_mensile, cast(prezzo_baseline as int) prezzo_baseline, cast(prezzo_incremental as int) prezzo_incremental, delta
   FROM lab1_db.rfcf_previsione_SOPS_TOBE2_attributes""")
  
  df_service_tobe2 = df_service_tobe2.withColumn('valore', f.col('trend_mensile')*(f.col('prezzo_baseline')+f.col('prezzo_incremental')))
  
  df_service_tobe2 = df_service_tobe2.withColumn('data_tmp', f.to_timestamp(f.to_date(f.concat_ws('-', 'anno', 'mese', f.lit(1))))).withColumn('data_tmp', aggiunta_mese_udf('data_tmp','delta')).filter(f.year(f.col('data_tmp')) == anno).drop('anno_tmp')  
  df_service_tobe2 = df_service_tobe2.withColumn('mese', f.month(f.col('data_tmp')))
  
  ######################
  # calcolo percentuale punti gas e pwr per ogni mese, da usare per divisione della commodity CROSS
  perc_punti_gas_pwr = spark.sql("""SELECT 
  commodity, 
  mese, 
  punti_anno_att 
  FROM lab1_db.rfcf_previsione_bollo_attributes
  WHERE sist_fat = 'NETA BR' """)
  perc_punti_gas_pwr = perc_punti_gas_pwr.withColumn('punti_tot_commodity', f.sum('punti_anno_att').over(Window.partitionBy('mese')))
  perc_punti_gas_pwr = perc_punti_gas_pwr.withColumn('perc_punti_commodity', f.col('punti_anno_att')/f.col('punti_tot_commodity')) \
                                         .drop('punti_anno_att','punti_tot_commodity')

  # divisione commodity CROSS
  df_service_tobe2 = df_service_tobe2.join(perc_punti_gas_pwr,'mese','left')
  df_service_tobe2 = df_service_tobe2.withColumnRenamed('valore','valore_mese') 
  df_service_tobe2 = df_service_tobe2.withColumn('valore', f.round(f.col('valore_mese')*f.col('perc_punti_commodity')).cast('int'))\
                                     .drop('valore_mese','punti_anno_att')
  #######################
  
  df_service_tobe2 = df_service_tobe2.groupBy('mese','commodity','tipo','sist_fat').sum('valore').withColumnRenamed('sum(valore)','valore')
  
  df_service_tobe2 = df_service_tobe2.withColumn('componente_tcr', f.col('tipo')).withColumn('fk_forn_fatt', f.lit('FIT#SOPS')).drop('tipo')
  
  df_all = df_all.unionByName(df_service_tobe2)
  
  
  pr_breve_attributes = spark.sql("""SELECT 
   cast(mese as int) mese, commodity, sist_fat, importo_percentuale, target_br_PREB_anno_att, target_bm_PREB_anno_att
   FROM lab1_db.rfcf_previsione_pr_breve_attributes""")
  
  pr_breve = pr_breve_attributes.withColumn('valore', f.when(f.col('sist_fat')=='NETA BR',f.col('importo_percentuale') * f.col('target_br_PREB_anno_att')) \
                                                              .otherwise(f.col('importo_percentuale') * f.col('target_bm_PREB_anno_att')))
  pr_breve = pr_breve.drop('importo_percentuale','target_br_PREB_anno_att','target_bm_PREB_anno_att').withColumn('componente_tcr', f.lit('PREB')).withColumn('fk_forn_fatt', f.lit('FIT#PREB'))
  
  df_all = df_all.unionByName(pr_breve)
  
  
  
  df_all = df_all.withColumnRenamed('mese','mese_emissione').withColumn('anno_emissione', f.lit(str(anno))).withColumn('id_run', f.lit(str(id_run)))
  df_all = df_all.withColumn('fk_testata',f.concat('fk_forn_fatt',f.lit('#'),'anno_emissione',f.lpad(f.col('mese_emissione'),2,'0'))).withColumn('saldo_acconto', f.lit('AC')).withColumn('origine_dato', f.lit('FCF')) \
                   .withColumn('churn', f.lit('1')).withColumn('flg_ricavi', f.lit('N')).withColumn('id_cluster', f.lit('-1'))
  df_all = df_all.withColumn('data_inizio_competenza', f.to_timestamp(f.concat_ws('-', 'anno_emissione', 'mese_emissione', f.lit(1))) ) \
                   .withColumn('data_fine_competenza', f.to_timestamp(f.last_day('data_inizio_competenza')))
  
  return df_all

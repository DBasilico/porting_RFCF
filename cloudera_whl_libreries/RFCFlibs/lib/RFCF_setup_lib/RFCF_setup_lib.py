from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from datetime import datetime
from pyspark.sql import Window
import calendar as cl
from dateutil.relativedelta import relativedelta

from shared_lib import run_par

def overwrite_table(spark, df_in, tab_out):
  
  # Salvo il nuovo contenuto in una tabella temporanea
  df_in.write.saveAsTable(tab_out+"_tmp2",mode='append')

  # Elimino il vecchio contenuto
  spark.sql('TRUNCATE TABLE ' + tab_out )
  
  # Ricarico il nuovo contenuto
  tmp = spark.sql("SELECT * FROM " + tab_out + "_tmp2") 
  
  # Store
  tmp.write.format('hive').mode('append').saveAsTable(tab_out)

  # Drop della tabella temporanea
  spark.sql('DROP TABLE ' + tab_out+ '_tmp2' ) 

#def data_snapshot(spark,nomi, df):
#    # Recupero id_run_fw e lo casto a stringa per la select successiva
#    id_run_fw = df.select('id_run_fw').collect()[0][0]
#    id_run_fw = str(id_run_fw)
#
#    # Eseguo un ciclo per ogni nome_tabella contenuto nella lista dei nomi
#    i = 0
#    while i < len(nomi):
#
#        # Carico le data_snapshot relativi alla tabella e all'id_run_fw in input
#
#        if nomi[i][0:6] == 'vw_tcr':
#
#            # Carico le data_snapshot relativi alla tabella e all'id_run_fw in input
#            df_query = spark.sql(
#                "SELECT max(data_snapshot) as data_snapshot FROM edl_tcr." + nomi[i] + " where id_run = " + id_run_fw)
#
#        else:
#
#            # Carico le data_snapshot relativi alla tabella e all'id_run_fw in input
#            df_query = spark.sql("SELECT max(data_snapshot) as data_snapshot FROM edl_neta." + nomi[i])
#
#        # Casto ad intero
#        df_query = df_query.withColumn('data_snapshot', f.col('data_snapshot').cast('int'))
#
#        # Ricavo il massimo
#        df_query = df_query.groupBy().max('data_snapshot').withColumnRenamed('max(data_snapshot)', 'data_snapshot')
#
#        # Aggiungo nome_tabella
#        df_query = df_query.withColumn('nome_tabella', f.lit(nomi[i]))
#
#        if i == 0:
#
#            # Creo Tabella Finale in cui concatenerò le varie data snapshot per tabella
#            df_snapshot = df_query
#
#        else:
#
#            # Concateno le varie data snapshot per tabella
#            df_snapshot = df_snapshot.union(df_query)
#
#        i = i + 1
#
#    return df_snapshot


def empty_df(df):
    try:
        if (df.count() > 0):
            val_empty = False
        else:
            val_empty = True

    except:
        val_empty = True

    return val_empty

def data_snap_ods(spark, src, table, lim_sup_cons=datetime.now().strftime('%Y%m%d')):
    db = spark.sql('SHOW CREATE TABLE edl_{}.{}'.format(src, table)).withColumn('db', f.substring_index(f.col('createtab_stmt'), 'FROM ', -1)) \
        .select('db').collect()[0][0]

    ds = spark.sql("SHOW PARTITIONS " + db) \
        .withColumn('ds', f.regexp_replace('partition', 'data_snapshot=', '').cast('integer')) \
        .filter(f.col('ds') <= lim_sup_cons) \
        .select(f.max('ds').alias('data_snapshot')).collect()[0][0]

    return ds
  
def compute_macrosegmento(spark, df_forniture, data_snapshot='', cond_join='cod_cliente'):
    """
    Funzione per calcolare il macrosegmento delle forniture TCR attraverso la tabella edl_ods.vw_fcf_soggetti
    :param df_forniture: spark dataframe della tabella forniture tcr
    :param data_snapshot: string, se non specificata prenderà l'ultima data_snapshot
    :param cond_join: condizione di join tra la tabella di forniture e la soggetti, il default è impostato sul campo cod_cliente
    :param spark: spark session attiva, impostata di default
    :return: dataframe delle forniture dato in input con aggiunta la colonne macrosegmento
    """
    if data_snapshot == '':
        data_snapshot = spark.table('edl_ods.vw_fcf_soggetti').select(f.max('data_snapshot').alias('ds')).first()['ds']

    query_soggetti = "SELECT distinct cod_cliente, macrosegmento FROM edl_ods.vw_fcf_soggetti where data_snapshot = {}".format(data_snapshot)
    soggetti = spark.sql(query_soggetti).withColumn('ordine', f.lit(2))

    query_soggetti_xe = "SELECT distinct concat('XE#',cod_cliente) as cod_cliente, macrosegmento FROM edl_int.vw_xe_tcr_tcr_soggetti_lr"
    soggetti_xe = spark.sql(query_soggetti_xe).withColumn('ordine', f.lit(1))
    soggetti_xe = soggetti_xe.withColumn('macrosegmento', f.when(f.col('macrosegmento') == 'Clienti Condominio', 'CONDOMINIO') \
                                         .when(f.col('macrosegmento').like('Clienti Pubblica%'), 'PUBBLICA AMMINISTRAZIONE') \
                                         .when(f.col('macrosegmento') == 'Clienti Microbusiness', 'MICROBUSINESS') \
                                         .when(f.col('macrosegmento') == 'Clienti Multisito', 'MULTISITO') \
                                         .when(f.col('macrosegmento') == 'Clienti Autotrazione', 'AUTOTRAZIONE') \
                                         .when(f.col('macrosegmento').isin('Clienti Residenziali', 'Clienti Residenziale'), 'RESIDENZIALI') \
                                         .when(f.col('macrosegmento') == 'Clienti Impresa', 'IMPRESE') \
                                         .otherwise(f.col('macrosegmento')))

    soggetti_all = soggetti.unionByName(soggetti_xe).orderBy('ordine').coalesce(1).dropDuplicates(subset=['cod_cliente']).drop('ordine')

    return df_forniture.join(soggetti_all, cond_join, 'left')
  
def lettura_forn_tcr_tioce(spark, commodity):
  
  commodity_tcr = {'pwr': 'EE', 'gas': 'GAS'}
  
  forniture = spark.table(f'lab1_db.rfcf_vw_forniture_{commodity}') \
    .filter(f.col('cod_stato_pratica') != 'ANNULLATO')

  forniture = forniture.select('fk_forn_fatt',
                               'cod_pratica_crm', 
                               'codice_conto_cliente', 
                               'codice_conto_cliente_numerico', 
                               'cod_cliente', 
                               f.lower(f.col('mercato')).alias('mercato'),
                               'pod_pdr',
                               f.col('cluster').cast('integer').alias('frequenza'),
                               f.lit(commodity_tcr[commodity]).alias('commodity_tcr')).distinct()
  
  ricavi = spark.sql(f"""SELECT DISTINCT 
    fk_forn_fatt,
    sist_fat
    FROM lab1_db.rfcf_vw_ricavi_{commodity} WHERE motivo_scarto in ("Valido","")""") \
    .filter(f.col('sist_fat') == 'NETA BR')
    
  forniture = ricavi.join(forniture, 'fk_forn_fatt', 'left')
  
  return forniture
  
def get_forniture_neta(spark, data_snapshot=''):
    """
    Funzione per leggera la tabella edl_neta.vw_neta_fcf_fornitura con il subset di informazioni necessarie per fornitura
    :param data_snapshot: string, se non specificata prenderà l'ultima data_snapshot
    :param spark: spark session attiva
    :return: dataframe con i campi [id_conto_cliente, id_fornitura, forn_old_codice_fornitura, codice_conto, cade_descrizione, commodity, tipo_mercato]
    """

    if data_snapshot == '':
        data_snapshot = spark.table('edl_neta.vw_neta_fcf_fornitura').select(f.max('data_snapshot').alias('ds')).first()['ds']

    df = spark.sql("""SELECT
      id_conto_cliente,
      id_fornitura,
      forn_old_codice_fornitura,
      codice_conto,
      cod_gruppo_fatt as cade_descrizione,
      if(spwkf_descrizione = 'ATTIVATA', 1, (if(spwkf_descrizione in ('ATTIVA COMMERCIALMENTE','IN CORSO DI CESSAZIONE'), 2, 3))) as rank,
      commodity,
      tipo_mercato,
      data_snapshot
      FROM edl_neta.vw_neta_fcf_fornitura""").filter(f.col('data_snapshot') == data_snapshot).drop('data_snapshot')

    w_key = Window.partitionBy('forn_old_codice_fornitura')
    df = df.withColumn('to_mantain', f.min('rank').over(w_key)).filter(f.col('rank') == f.col('to_mantain'))
    df = df.drop('to_mantain', 'rank').dropDuplicates(['forn_old_codice_fornitura'])

    return df
  
def compute_info_neta(df_forn_neta):
    """
    Funzione per calcolare i campi ['TIPO_SERVIZIO', 'TIPOLOGIA_CLIENTE', 'TIPO_MERCATO', 'CODICE_FAMIGLIA_EMISSIONE'] per la tabelle edl_neta.vw_neta_fcf_fornitura
    :param df_forn_neta: dataframe delle forniture neta
    :return: dataframe con la computazione delle colonne tipo_servizio, tipologia_cliente, tipo_mercato, codice_famiglia_emissione
    """
    df_forn_neta = df_forn_neta.withColumn('commodity', f.when(f.col('commodity').contains('GAS'), 'GAS') \
                                           .when((f.col('commodity').contains('ENERGIA ELETTRICA')) | (f.col('commodity') == 'EE'), 'EE').otherwise('ND'))

    df_forn_neta = df_forn_neta.withColumn('nCommodity', f.size(f.collect_set('commodity').over(Window.partitionBy('codice_conto')))) \
        .withColumn('nForn', f.size(f.collect_set('forn_old_codice_fornitura').over(Window.partitionBy('codice_conto'))))

    df_forn_neta = df_forn_neta.withColumn('tipo_servizio', f.when(f.col('nCommodity') > 1, 'DUAL').otherwise(f.col('commodity'))) \
        .withColumn('tipologia_cliente', f.when(f.col('nForn') > 1, f.when(f.col('nCommodity') > 1, 'DUAL').otherwise('MULTI')).otherwise('MONO'))

    df_forn_neta = df_forn_neta.withColumn('tipo_mercato', f.when(f.col('tipo_mercato').like('%LIBERO%'), 'LIBERO') \
                                           .when(f.col('tipo_mercato').like('%VINCOLATO%'), 'REGOLATO').otherwise('ND'))

    df_forn_neta = df_forn_neta.withColumn('tipo_mercato', f.when(f.col('tipologia_cliente') == 'DUAL', 'LIBERO').otherwise(f.col('tipo_mercato')))

    df_forn_neta = df_forn_neta.withColumn('codice_famiglia_emissione', f.when(f.col('tipo_servizio') == 'DUAL', 'D') \
                                           .when(f.col('tipo_servizio') == 'EE', 'P') \
                                           .otherwise(f.when(f.col('tipo_mercato') == 'LIBERO', 'GL') \
                                                      .otherwise('GR')))
    df_forn_neta = df_forn_neta.withColumn('cadenza',f.when(f.col('cade_descrizione') == 'MENSILI', 'MENSILE') \
                                                 .when(f.col('cade_descrizione') == 'QUADRIM_Q1', 'QUADRIMESTRALE_Q1') \
                                                 .when(f.col('cade_descrizione') == 'QUADRIM_Q2', 'QUADRIMESTRALE_Q2') \
                                                 .when(f.col('cade_descrizione') == 'QUADRIM_Q3', 'QUADRIMESTRALE_Q3') \
                                                 .when(f.col('cade_descrizione') == 'QUADRIM_Q4', 'QUADRIMESTRALE_Q4') \
                                                 .when(f.col('cade_descrizione') == 'BIMESTRALI_P', 'BIMESTRALE_PARI') \
                                                 .when(f.col('cade_descrizione') == 'BIMESTRALI_D', 'BIMESTRALE_DISPARI')
                                                 .otherwise('ND'))
    
    df_forn_neta = df_forn_neta.withColumn('cadenza',f.when(f.col('sist_fat') == 'XE', 'MENSILE') \
                                             .otherwise(f.col('cadenza')))
                                                 
    df_forn_neta = df_forn_neta.withColumn('cadenza_tcr',f.when(f.col('cadenza') == 'MENSILE', 'MENSILE') \
                                                     .when(f.col('cadenza').isin('QUADRIMESTRALE_Q1', 'QUADRIMESTRALE_Q2', 'QUADRIMESTRALE_Q3', 'QUADRIMESTRALE_Q4'), 'QUADRIMESTRALE') \
                                                     .when(f.col('cadenza').isin('BIMESTRALE_PARI', 'BIMESTRALE_DISPARI'), 'BIMESTRALE') \
                                                     .otherwise('ND'))#.drop()

    df_forn_neta = df_forn_neta.filter(~((f.col('commodity') == 'ND') & (f.col('cadenza') == 'ND'))).drop_duplicates()


    return df_forn_neta.drop('nCommodity', 'nForn', 'cade_descrizione')
  
def compute_cedibile_domiciliato(spark, df_forniture, cond_join='id_conto_cliente'):
    """
    Funzione per ottenere il campo cedibilita e domiciliazione per cliente, attributo di TIOCE, dalla tabella edl_ods.vw_dg_l1_dg_conti_cliente_bi
    :param df_forniture: spark dataframe della tabella edl_ods.vw_dg_l1_conto_cliente
    :param cond_join: condizione di join tra la tabella di forniture e la conto_cliente, il default è impostato sul campo id_conto_cliente
    :param spark: spark session attiva, impostata di default
    :return: dataframe delle forniture dato in input con aggiunta le colonne cedibile e domiciliato
    """
    df_conto_cliente_bi = spark.table('edl_ods.vw_dg_l1_dg_conti_cliente_bi') \
        .select('id_conto_cliente',
                'stato_dom_neta').distinct()

    df_conto_cliente_bi = df_conto_cliente_bi.withColumn('domiciliato', f.when(f.col('stato_dom_neta').isin('N', 'C'), 'N') \
                                                         .when(f.col('stato_dom_neta').isin('D', 'CD'), 'S') \
                                                         .otherwise('ND'))

    df_conto_cliente_bi = df_conto_cliente_bi.withColumn('cedibile', f.when(f.col('stato_dom_neta').isin('N', 'D'), 'N') \
                                                         .when(f.col('stato_dom_neta').isin('C', 'CD'), 'S') \
                                                         .otherwise('ND'))

    return df_forniture.join(df_conto_cliente_bi, cond_join, 'left')
  
  
def compute_spedizione(spark, df_forniture, dict_mapping, cond_join='codice_conto'):
    """
    Funzione per ottenere il campo tipo_spedizione per cliente, attributo di TIOCE, dalla tabella edl_ods.vw_dg_l1_conto_cliente
    :param df_forniture: spark dataframe della tabella edl_ods.vw_dg_l1_conto_cliente
    :param dict_mapping: dizionario di mapping del campo canale_inoltro con 'POSTALE' e 'DIGITALE'
    :param cond_join: condizione di join tra la tabella di forniture e la conto_cliente, il default è impostato sul campo codice_conto_cliente
    :param spark: spark session attiva, impostata di default
    :return: dataframe delle forniture dato in input con aggiunta la colonna tipo_spedizione
    """
    df_conto_cliente = spark.sql("""SELECT 
      cod_conto_cliente_numerico as codice_conto,
      canale_inoltro
      FROM edl_ods.vw_dg_l1_conto_cliente
      WHERE flag_ultimo_record_valido = 1""")
    
    df_conto_cliente = df_conto_cliente.withColumn('tipo_spedizione', f.coalesce(*[f.when(f.upper(f.col('canale_inoltro')) == key, f.lit(value)) for key, value in dict_mapping.items()], f.lit('ND')))

    print('Numero di clienti per cui il mapping del dizionario non ha funzionato: {}'.format(df_conto_cliente.filter(f.col('tipo_spedizione') == 'ND').select('codice_conto').distinct().count()))
    df_conto_cliente = df_conto_cliente.filter(f.col('tipo_spedizione') != 'ND')

    return df_forniture.join(df_conto_cliente, cond_join, 'left')
  
def compute_conto_cliente_completo(spark, df_forniture, data_snapshot='', cond_join='id_fornitura'):
    """
    Funzione per calcolare il campo conto_cliente_completo dei conti neta attraverso la edl_ods.vw_fcf_blocchi_fatturazione
    :param df_forniture: spark dataframe della tabella edl_neta.fcf_forniture
    :param data_snapshot: string, se non specificata prenderà l'ultima data_snapshot
    :param cond_join: condizione di join tra la tabella di forniture e la blocchi_fatturazione, il default è impostato sul campo id_fornitura
    :param spark: spark session attiva, impostata di default
    :return: dataframe delle forniture dato in input con aggiunta le colonne ['DATA_INIZIO_VALIDITA_BLOCCO', 'CONTO_CLIENTE_COMPLETO', 'PRESENZA_BLOCCO']
    """
    if data_snapshot == '':
        data_snapshot = spark.table('edl_ods.vw_fcf_blocchi_fatturazione').select(f.max('data_snapshot').alias('ds')).first()['ds']

    df_blocchi_fatt = spark.sql("""SELECT
          codice_fornitura id_fornitura,
          data_inizio_validita data_inizio_validita_blocco,
          data_snapshot
          FROM edl_ods.vw_fcf_blocchi_fatturazione""") \
        .filter(f.col('data_snapshot') == data_snapshot).drop('data_snapshot').distinct()

    df_blocchi_fatt = df_blocchi_fatt.withColumn('min_data_inizio_validita_blocco', f.min(f.col('data_inizio_validita_blocco')).over(Window.partitionBy('id_fornitura')))

    df_blocchi_fatt = df_blocchi_fatt.filter(f.col('min_data_inizio_validita_blocco') == f.col('data_inizio_validita_blocco')).drop('min_data_inizio_validita_blocco')

    df_forniture_upd = df_forniture.join(df_blocchi_fatt, cond_join, 'left')

    df_forniture_upd = df_forniture_upd.withColumn('nBlocchi', f.size(f.collect_set('data_inizio_validita_blocco').over(Window.partitionBy('codice_conto')))) \
        .withColumn('conto_cliente_completo', f.when(f.col('nBlocchi') > 0, 'N').otherwise('S')) \
        .withColumn('presenza_blocco', f.when(f.col('conto_cliente_completo') == 'S', 'NO BLOCCATO').otherwise('BLOCCATO PARZIALE'))

    return df_forniture_upd

def max_lett_tcr(spark, commodity):
  par = run_par(spark)
  ultima_data_consuntiva = datetime(par.anno_fine,par.mese_fine_cons,cl.monthrange(par.anno_fine,par.mese_fine_cons)[1])
  prima_data_previsiva = ultima_data_consuntiva + relativedelta(days=1)
  
  df_ricavi_anno_attuale = spark.table(f'lab1_db.rfcf_vw_ricavi_{commodity}') \
    .filter(f.col('origine_dato').isin('Fatturato', 'Rateo')) \
    .filter(f.col('componente_tcr') == 'CONSUMO') \
    .filter(f.col('saldo_acconto') == 'SA') \
    .groupBy(f.col('fk_forn_fatt')).agg(f.max(f.col('data_fine_competenza')).alias('max_lett_tcr'))
    
  df_ricavi_anno_precedente = spark.table(f'lab1_db.rfcf_vw_ricavi_{commodity}_anno_prec') \
    .filter(f.col('origine_dato').isin('Fatturato', 'Rateo')) \
    .filter(f.col('componente_tcr') == 'CONSUMO') \
    .filter(f.col('saldo_acconto') == 'SA') \
    .groupBy(f.col('fk_forn_fatt')).agg(f.max(f.col('data_fine_competenza')).alias('max_lett_tcr'))
    
  df_letture_tcr = df_ricavi_anno_attuale.unionByName(df_ricavi_anno_precedente)
  
  df_letture_tcr = df_letture_tcr.groupBy(f.col('fk_forn_fatt')).agg(f.max(f.col('max_lett_tcr')).alias('max_lett_tcr'))
  
  if df_letture_tcr.filter(f.col('max_lett_tcr')>=prima_data_previsiva).count() > 0:
    raise Exception('max_lett_tcr ha date successive al consuntivo')
    
  return df_letture_tcr

def lettura_forn_mens(spark, commodity):
    if commodity.lower() not in ['pwr', 'gas']:
        sys.exit('La commodity passata non è tra quelle aspettate: [pwr, gas]')

    dsnap = spark.table('edl_tcr.ot_tcr_fcf_forniture_' + commodity + '_on').select(f.max('data_snapshot').alias('ds')).first()['ds']

    if commodity == 'pwr':
        with open("./RFCF_emesso_anagrafica.txt", "a") as output:
            output.write('\n\n' + 'Giorno aggiornamento: ' + datetime.now().strftime('%Y-%m-%d') + " - data_snapshot forniture TCR Power:" + dsnap)
    else:
        with open("./RFCF_emesso_anagrafica.txt", "a") as output:
            output.write('\n' + 'Giorno aggiornamento: ' + datetime.now().strftime('%Y-%m-%d') + " - data_snapshot forniture TCR Gas:" + dsnap)

    forniture = spark.table('edl_tcr.vw_tcr_fcf_forniture_' + commodity) \
        .filter(f.col('data_snapshot') == dsnap) \
        .filter(f.col('pod_pdr') != 'NO CRM') \
        .filter(f.col('sist_fat').isin('NETA BR', 'XE')) \
        .withColumn('data_decorrenza_crm', f.coalesce(f.col('data_decorrenza_crm'), f.lit('2100-01-01'))) \
        .withColumn('max_data_decorrenza_crm', f.max('data_decorrenza_crm').over(Window.partitionBy('cod_pratica_crm', 'sist_fat', 'pod_pdr'))) \
        .filter(f.col('data_decorrenza_crm') == f.col('max_data_decorrenza_crm')) \
        .withColumn('mercato', f.when(f.col('mercato') == 'tutelato', 'regolato').otherwise(f.col('mercato'))) \
        .select('cod_pratica_crm',
                'commodity',
                'codice_conto_cliente',
                'codice_conto_cliente_numerico',
                'cod_cliente',
                'sist_fat',
                'pod_pdr',
                'mercato',
                'tipo_regime',
                'data_snapshot').distinct()
    return forniture
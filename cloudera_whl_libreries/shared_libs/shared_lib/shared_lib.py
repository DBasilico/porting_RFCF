from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from datetime import datetime, timedelta, date
import calendar as cl
from pyspark.sql import Window
import pandas as pd
from udfs import split_dateformat
from itertools import chain
import time
import os
import warnings

def comp(dateS, dateE, NUM_M):
    """Metodo per dividere l'intervallo di competenza in base al numero di mesi.

    Input:  dateS       --> colonna data inizio competenza
            dateE       --> colonna data fine competenza
            NUM_M       --> colonna intervallo mensile di competenza

    Output: StartDate   --> colonna data inizio competenza mensile
            EndDate     --> colonna data fine competenza mensile
            """

    StartDate = []
    EndDate = []
    for i in range(NUM_M):
        if i == 0:
            if (dateS.month == dateE.month and dateS.year == dateE.year):
                StartDate.append(dateS)
                EndDate.append(dateE)
            else:
                StartDate.append(dateS)
                dateS = dateS.replace(day=cl.monthrange(dateS.year, dateS.month)[1])
                EndDate.append(dateS)
                dateS = dateS + timedelta(1)
        elif i == NUM_M - 1:
            StartDate.append(dateS)
            EndDate.append(dateE)
        else:
            StartDate.append(dateS)
            dateS = dateS.replace(day=cl.monthrange(dateS.year, dateS.month)[1])
            EndDate.append(dateS)
            dateS = dateS + timedelta(1)
    return StartDate, EndDate


def pro_die(df, inizio_competenza, fine_competenza, inizio_mese, fine_mese, val='VALORE'):
    """Metodo che ricalcola il valore nel periodo di competenza.

    Input:  df                  --> dataframe di partenza
            inizio_competenza   --> colonna data inizio periodo
            fine_competenza     --> colonna data fine periodo
            inizio_mese         --> colonna inizio mese
            fine_mese           --> colonna fine mese
            val='VALORE'        --> colonna con il valore da competenziare

    Output: df                  --> dataframe con il valore ricalcolato mensilmente
            """
    df = df.withColumn('deltaGG', f.datediff(fine_competenza, inizio_competenza) + 1)
    df = df.filter(df.deltaGG > 0)
    df = df.withColumn('GiorniEffettivi', f.datediff(fine_mese, inizio_mese) + 1)
    df = df.filter(df.GiorniEffettivi > 0)
    df = df.withColumn("ANNO_COMP", f.year(f.col(inizio_mese)))
    df = df.withColumn("MESE_COMP", f.month(f.col(inizio_mese)))
    df = df.withColumn("ANNOMESE_COMP", f.concat(f.year(f.col(inizio_mese)).cast("string"),
                                                 f.format_string("%02d", f.month(f.col(inizio_mese)))))

    df = df.withColumn("VALORE_COMP", f.col(val).cast("float") / df.deltaGG * df.GiorniEffettivi)
    return df


class run_par:
    def __init__(self, spark):
        tmp = spark.table('lab1_db.rfcf_storico_run').filter(f.col('flg_run_corrente') == 'Y').collect()[0]
        self.id_run = tmp['id_run']
        self.id_run_fw = tmp['id_run_fw']
        self.anno_inizio = int(tmp['anno_inizio'])
        self.anno_fine = int(tmp['anno_fine'])
        self.id_run_cons = tmp['id_run_cons']
        self.mese_fine_cons = tmp['mese_fine_cons']
        self.max_emissione_cons = tmp['max_emissione_cons']
        self.nota = tmp['nota']
        self.stato = tmp['stato']
        self.flg_run_corrente = tmp['flg_run_corrente']
        self.data_run_inizio = tmp['data_run_inizio']
        self.data_run_fine = tmp['data_run_fine']
        self.anno_prec_tipologia = tmp['anno_prec_tipologia']
        self.ds_ric_gas_anno_prec = tmp['ds_ric_gas_anno_prec']
        self.ds_ric_pwr_anno_prec = tmp['ds_ric_pwr_anno_prec']
        tmp_cons = spark.table('lab1_db.rfcf_storico_run_cons').filter(f.col('id_run_cons') == tmp['id_run_cons']).select('data_run')
        if len(tmp_cons.head(1)) > 0:
          tmp_cons = tmp_cons.collect()[0]
          self.data_run = tmp_cons['data_run'].date()
        else:
          print('id_run_cons non presente --> parametro data_run di default')
          self.data_run = date(self.anno_inizio, self.mese_fine_cons, cl.monthrange(self.anno_inizio, self.mese_fine_cons)[1])



class data_snap:
    def __init__(self, spark, id_run):
        snap_table = spark.table('lab1_db.rfcf_run_snapshot').filter(f.col('id_run') == id_run)
        if snap_table.count() < 1:
            raise Exception('ID RUN non ha riscontro')
        if snap_table.groupBy('nome_tabella').count().agg({'count': 'max'}).collect()[0]['max(count)'] > 1:
            raise Exception('Ci sono tabelle doppie in rfcf_run_snapshot')
        self.data_snapshot = dict(snap_table.select('nome_tabella', 'data_snapshot').collect())


class data_snap_cashflow:

    @staticmethod
    def remove_directory(tabella):
        if len(tabella.split('.')) == 2:
            return tabella.split('.')[-1]
        else:
            return tabella

    def get_feature_table(self, tabella, feature):
        if len(tabella.split('.')) == 2:
            tabella = tabella.split('.')[-1]
        for line in self.tab_infos:
            if line['nome_tabella'] == tabella:
                return line[feature]
        return None

    def get_informations_pandas(self):
        df = pd.DataFrame(columns=self.tab_infos[0].asDict().keys())
        for line in self.tab_infos:
            line = line.asDict()
            df = df.append(line, ignore_index=True)
        df.set_index('nome_tabella', inplace=True)
        return df

    def get_informations(self):
        return self.tab_infos

    def __init__(self, spark, id_run):
        spark.sparkContext.addPyFile('/home/cdsw/FCF/shared_libs/udf/udfs.py')
        split_type = f.udf(lambda s: split_dateformat(s), returnType=StringType())
        divide_array_0 = f.udf(lambda arr: arr[0], returnType=StringType())
        divide_array_1 = f.udf(lambda arr: arr[1], returnType=StringType())
        snap_table = spark.table('lab1_db.rfcf_run_snapshot_cashflow').filter(f.col('id_run_cashflow') == id_run)
        if snap_table.count() < 1:
            raise Exception('data_snap_FCF2: ID RUN non ha riscontro')
        if snap_table.groupBy('nome_tabella').count().agg({'count': 'max'}).collect()[0]['max(count)'] > 1:
            raise Exception('Ci sono tabelle doppie in rfcf_run_snapshot')

        snap_table = snap_table.withColumn('vectorized_spit', split_type('formato')) \
            .withColumn('formato', divide_array_0('vectorized_spit')) \
            .withColumn('string_data_formato', divide_array_1('vectorized_spit')) \
            .drop('vectorized_spit')

        snap_table = snap_table.withColumn('data_snapshot', f.to_timestamp(f.col('data_snapshot'), 'yyyy-MM-dd HH:mm:ss'))

        self.tab_infos = snap_table.collect()

        ###  TIMESTAMP TYEPES  ###
        snap_table_timestamp = snap_table.filter(f.col('formato') == 'timestamp').drop('string_data_formato')

        ###  STRING TYEPES  ###
        snap_table_stringa = snap_table.filter(f.col('formato') == 'stringa')

        all_string_formats = [x.string_data_formato for x in snap_table_stringa.select('string_data_formato').distinct().collect()]

        str_format = all_string_formats[0]
        snap_table_stringa_NEW = snap_table_stringa.filter(f.col('string_data_formato') == str_format)
        snap_table_stringa_NEW = snap_table_stringa_NEW \
            .withColumn('data_snapshot', f.date_format(f.col('data_snapshot'), str_format)) \
            .withColumn('data_snapshot', f.col('data_snapshot').cast(StringType()))

        for str_format in all_string_formats[1:]:
            snap_table_stringa_TEMP = snap_table_stringa.filter(f.col('string_data_formato') == str_format)
            snap_table_stringa_TEMP = snap_table_stringa_TEMP \
                .withColumn('data_snapshot', f.date_format(f.col('data_snapshot'), str_format)) \
                .withColumn('data_snapshot', f.col('data_snapshot').cast(StringType()))
            snap_table_stringa_NEW = snap_table_stringa_NEW.unionByName(snap_table_stringa_TEMP)

        snap_table_stringa = snap_table_stringa_NEW
        del snap_table_stringa_NEW

        ###  INTEGER TYEPES  ###
        snap_table_integer = snap_table.filter(f.col('formato') == 'integer').drop('string_data_formato')
        snap_table_integer = snap_table_integer \
            .withColumn('data_snapshot', f.date_format(f.col('data_snapshot'), 'yyyyMMdd')) \
            .withColumn('data_snapshot', f.col('data_snapshot').cast(IntegerType()))

        ## Creiamo il data_snapshot ##
        self.data_snapshot = dict()
        for pair in snap_table_timestamp.select(['nome_tabella', 'data_snapshot']).collect():
            self.data_snapshot[pair.nome_tabella] = pair.data_snapshot
        for pair in snap_table_stringa.select(['nome_tabella', 'data_snapshot']).collect():
            self.data_snapshot[pair.nome_tabella] = pair.data_snapshot
        for pair in snap_table_integer.select(['nome_tabella', 'data_snapshot']).collect():
            self.data_snapshot[pair.nome_tabella] = pair.data_snapshot


class DecodificheError(Exception):
    'ERRORE: nelle decodifiche manca almeno un elemento'

    def __init__(self):
        self.message = 'ERRORE: nelle decodifiche manca almeno un elemento'
        super().__init__(self.message)


class controllo_decodifiche:

    def __init__(self, spark, ds_class):
        df_errori_columns = ['campo_mancante_decodifica',
                             'nome_tabella_controllata',
                             'nome_colonna_controllata',
                             'query_controllata',
                             'colonna_snapshot_controllata',
                             'snapshot_controllata',
                             'nome_tabella_decodifica',
                             'query_decodifica',
                             'nome_colonna_decodifica',
                             'data_del_check']

        self.__df_errori = pd.DataFrame(columns=df_errori_columns, dtype=str)
        self.ds_class = ds_class
        self.spark = spark

    def controlla(self, info):
        if info['query_controllata'] is not None:
            try:
                df_controllato = self.spark.sql(info['query_controllata'])
            except:
                raise Exception('TAB CONTROLLATA: la query non funziona')
        else:
            df_controllato = self.spark.table(info['nome_tabella_controllata'])

        # filtriamo il datasnapshot corrente
        nome_colonna_snapshot = self.ds_class.get_feature_table(info['nome_tabella_controllata'], 'nome_colonna')
        snapshot_corrente = self.ds_class.data_snapshot[
            self.ds_class.remove_directory(info['nome_tabella_controllata'])]
        df_controllato = df_controllato.filter(f.col(nome_colonna_snapshot) == snapshot_corrente)
        df_controllato = df_controllato.cache()

        if isinstance(snapshot_corrente, datetime):
            snapshot_corrente = snapshot_corrente.strftime("%d/%m/%Y, %H:%M:%S")

        # Un paio di check
        if df_controllato.count() == 0:
            raise Exception(f"TAB CONTROLLATA: {info['tab']} da controllare VUOTA")
        col_controllata = info['nome_colonna_controllata']
        if col_controllata not in df_controllato.columns:
            raise Exception(f"TAB CONTROLLATA: colonna {info['colonna']} non prensente in tabella")

        # Insieme degli elementi da controllare (Aggiustare con antijoin)
        SET_controllato = set([x[col_controllata] for x in df_controllato.select(col_controllata).distinct().collect()])

        if info['query_decodifica'] is not None:
            try:
                df_decodifiche = self.spark.sql(info['query_decodifica'])
            except:
                raise Exception('TAB DECODIFICA: la query non funziona')
        else:
            df_decodifiche = self.spark.table(info['nome_tabella_decodifica'])

        if info['nome_colonna_decodifica'] not in df_decodifiche.columns:
            raise Exception('TAB DECODIFICA: colonna con decodifica non trovata')

        df_decodifiche = df_decodifiche.select(info['nome_colonna_decodifica']).distinct()
        SET_decodifica = set([x[info['nome_colonna_decodifica']] for x in df_decodifiche.collect()])

        mancanti = SET_controllato - SET_decodifica

        if len(mancanti) > 0:
            for campo in mancanti:
                row_dict = {'campo_mancante_decodifica': str(campo),
                            'nome_tabella_controllata': info['nome_tabella_controllata'],
                            'query_controllata': info['query_controllata'],
                            'nome_colonna_controllata': info['nome_colonna_controllata'],
                            'colonna_snapshot_controllata': nome_colonna_snapshot,
                            'snapshot_controllata': snapshot_corrente,
                            'nome_tabella_decodifica': info['nome_tabella_decodifica'],
                            'query_decodifica': info['query_decodifica'],
                            'nome_colonna_decodifica': info['nome_colonna_decodifica'],
                            'data_del_check': datetime.strftime(datetime.now(), "%d/%m/%Y %H:%M:%S")}

                for kk in row_dict.keys():
                    row_dict[kk] = str(row_dict[kk])
                self.__df_errori = self.__df_errori.append(row_dict, ignore_index=True)

    def return_df_errori(self):
        return self.__df_errori

    def inizializza_tabella_errori(self, spark, tab_name='lab1_db.rfcf_check_decodifiche'):
        import colorama
        spark.sql(f"DROP TABLE {tab_name}")
        print('\033[41m' + '\033[36m' + f'Tabella {tab_name} DROPPATA')
        print('\033[0m')
        empty_tab_schema = StructType([StructField(col, StringType(), True) for col in self.__df_errori.columns])
        empty_tab = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=empty_tab_schema)
        empty_tab.write.format('hive').saveAsTable(tab_name)
        print('\033[42m' + '\033[30m' + f'Tabella {tab_name} INIZIALIZZATA')
        print('\033[0m')

    def return_tabella_errori(self, spark, tab_name='lab1_db.rfcf_check_decodifiche'):
        return spark.table(tab_name)

    def overwrite_tabella_errori(self, spark, tab_name='lab1_db.rfcf_check_decodifiche'):
        from colorama import Fore, Back, Style
        if self.__df_errori.empty:
            print(Back.GREEN + f'Nessun problema riscontrato')
            print(Style.RESET_ALL)
            return None
        sdf = spark.createDataFrame(self.__df_errori)
        print(Back.GREEN + f'Tabella {tab_name} APPESA')
        print(Style.RESET_ALL)
        sdf.write.mode('overwrite').format('hive').saveAsTable(tab_name)
        return sdf

    def trunca_tabella_errori(self, spark, tab_name='lab1_db.rfcf_check_decodifiche'):
        import colorama
        print('\033[41m' + '\033[36m' + f'Tabella {tab_name} TRONCATA')
        print(colorama.Style.RESET_ALL)
        spark.sql('TRUNCATE TABLE ' + tab_name)


def customUnion(df1, df2):
    """Funzione per concatenare due dataframe con diverso schema fillando le colonne di differenza con colonne vuote
    :param df1: spark.dataFrame, primo dataframe
    :param df2: spark.dataFrame, secondo dataframe
    """
    # Selezione le colonne di ogni df
    cols1 = df1.columns
    cols2 = df2.columns

    # Creo la lista delle colonne totali
    total_cols = sorted(cols1 + list(set(cols2) - set(cols1)))

    col_mancanti_1 = set(total_cols) - set(cols1)
    col_mancanti_2 = set(total_cols) - set(cols2)

    df1 = df1.select(['*', *(f.lit(None).alias(i) for i in col_mancanti_1)])
    df2 = df2.select(['*', *(f.lit(None).alias(i) for i in col_mancanti_2)])

    df = df1.unionByName(df2)

    return df


def overwrite_table(spark, table, df):
    """Funzione per sovrascrivere una tabella mantenendo lo schema.
    :param spark: sparkSession
    :param table: str, nome della tabella
    :param df: spark.dataFrame, dataframe con i dati da salvare
    """
    spark.sql(f"""truncate table {table}""")
    df.write.format('hive').saveAsTable(table, mode='append')


def sql_create(table, df, stored='parquet'):
    schema = df.dtypes
    ss = f"""CREATE TABLE IF NOT EXISTS {table}\n(\n"""
    for i in schema:
        if i[1] == 'long':
            i[1] = 'bigint'

        if i != schema[-1]:
            ss += f"""\t{i[0]} {i[1].upper()},\n"""
        else:
            ss += f"""\t{i[0]} {i[1].upper()}\n"""

    ss += f""")\nSTORED AS {stored.upper()}"""

    return ss


def window_function_ods(partition, order):
    w = Window.partitionBy(partition).orderBy(order.desc())
    return w


class TracciaTempo:
    def __init__(self):
        self.start_time = time.time()

    def tempo_trascorso(self):
        tempo = (time.time() - self.start_time)
        print(f'{tempo} secondi')
        print(f'{tempo / 60} minuti')
        print(f'{tempo / 3600} ore')

    def tempo_trascorso_secondi(self):
        tempo = (time.time() - self.start_time)
        return tempo


class run_par_cashflow(dict):
    def __init__(self, spark):
        configurazione = spark.table('lab1_db.rfcf_configurazione_start_run_cashflow').collect()[0].asDict()
        if configurazione['puntamento_id_run']>0 and configurazione['vista_forecast_cashflow']=='default':
          raise Exception('ERRORE: "puntamento_id_run">0 e "vista_forecast_cashflow"==default')

        if configurazione['puntamento_id_run']<0:
          par = spark.table('lab1_db.rfcf_storico_run')\
                     .filter(f.col('flg_run_corrente')=='Y').collect()[0].asDict()
        else:
          par = spark.table('lab1_db.rfcf_storico_run')\
                     .filter(f.col('id_run')==configurazione['puntamento_id_run']).collect()[0].asDict()

        par_cf = spark.table('lab1_db.rfcf_storico_run_cashflow')\
                      .filter(f.col('flg_run_corrente_cashflow') == 'Y').collect()[0].asDict()
        for c in par:
            self[c] = par[c]
        for c in par_cf:
            self[c] = par_cf[c]

    def __getattr__(self, name):
        return self[name]


def decodifica_colonna(df, col_name, map_dict, na_replace=None, mantieni_mancanti=True, default_mancanti=None):
    if not isinstance(col_name, f.Column):
        col_name = f.col(col_name)
    if na_replace is not None:
        df = df.fillna(na_replace, subset=[col_name])
  
    mapping_expr = f.create_map([f.lit(x) for x in chain(*map_dict.items())])
  
    if mantieni_mancanti:
        expr = f.when(~f.isnull(mapping_expr.getItem(col_name)), mapping_expr.getItem(col_name)).otherwise(col_name)
    else:
        expr = f.when(~f.isnull(mapping_expr.getItem(col_name)), mapping_expr.getItem(col_name)).otherwise(f.lit(default_mancanti))
  
    return df.withColumn(col_name._jc.toString(), expr)

  
class monitor_tempistiche:
  def __init__(self,nome_script=None):
    self.output_table_name = 'lab1_db.rfcf_tempistiche_cashflow'
    self.expected_columns = ['origine_run','fase_run','id_run','id_run_cashflow',
                             'nome_script','tempo_minuti','giorno_run']
    self.output_schema = StructType([StructField('origine_run', StringType(),True),
                                     StructField('fase_run', StringType(),True),
                                     StructField('id_run', IntegerType(),True),
                                     StructField('id_run_cashflow',IntegerType(),True),
                                     StructField('nome_script', StringType(),True),
                                     StructField('tempo_minuti', DoubleType(),True),
                                     StructField('giorno_run', TimestampType(),True)])
    self.start_time = datetime.now()
    self.nome_script = nome_script
  
  def add(self, spark, fase_run, par=None):
    df_output = pd.DataFrame()
    df_output.at[0,'fase_run'] = fase_run
    df_output.at[0,'tempo_minuti'] = (datetime.now()-self.start_time).seconds/60
    if par is None:
      par = run_par_cashflow(spark)
    df_output['giorno_run'] = self.start_time
    df_output['origine_run'] = os.environ.get('CDSW_ENGINE_TYPE')
    if os.environ.get('SCRIPT_NAME') is not None:
      df_output['nome_script'] = os.environ.get('SCRIPT_NAME')
    elif self.nome_script is not None:
      df_output['nome_script'] = self.nome_script
    else:
      df_output['nome_script'] = 'nome_script non presente'
    df_output['id_run'] = par.id_run
    df_output['id_run_cashflow'] = par.id_run_cashflow
    if set(df_output.columns) != set(self.expected_columns):
      raise Exception('Colonne tabella non coincidono con quelle attese')
    else:
      df_output = df_output[self.expected_columns]
    spark.createDataFrame(df_output, schema=self.output_schema)\
          .write.mode('append').format('parquet').saveAsTable(self.output_table_name)
  
  def truncate_table(spark):
    spark.sql(f'TRUNCATE TABLE {self.output_table_name}')
    

def rinomina_campi(spark, df, tabella):

    # tabella di mapping
    tabella_rinomina_campi = 'data.tcr_rinomina_ndp_to_dg'
    df_mappatura_colonne = spark.table(tabella_rinomina_campi).filter(f.col('Tabella') == tabella).select(f.col('CAMPO_NDP'), f.col('CAMPO_TCR'))

    # trasformo la tabella di mapping in un dizionario
    map_dict = df_mappatura_colonne.toPandas().set_index('CAMPO_NDP').to_dict()['CAMPO_TCR']

    # ciclo sugli elementi del dizionario per rinominare le colonne
    for nomeNDP, nomeTCR in map_dict.items():
        df = df.withColumnRenamed(nomeNDP, nomeTCR)

    return df
 
 
def codifica_campi(spark, flusso, df):
    df_codifica_campi = spark.table('data.tcr_flusso_campi_codifica').filter(f.col('Flusso')==flusso)
    codifica_campi = df_codifica_campi.collect()
    if len(codifica_campi) == 0:
        return df
    mantieni_mancanti = codifica_campi[0].mantieni_mancanti
    default_mancanti = codifica_campi[0].default_mancanti
    
    df_infoDecodifica = spark.table('data.tcr_decodifica_valori')

    df_decodifiche = df_codifica_campi.drop('mantieni_mancanti','default_mancanti')\
                 .join(df_infoDecodifica, 
                       on=[df_codifica_campi.Codifica == df_infoDecodifica.Nome_Decodifica], 
                       how='left')
    colonneDaDecodificare = [x.Campo for x in df_decodifiche.select('Campo').distinct().collect()]

    for colonnaDaDecodificare in colonneDaDecodificare:
        map_dict = df_decodifiche.filter(f.col('Campo')==colonnaDaDecodificare).toPandas()
        if map_dict.ID.isna().any(): # <------ vuoi fare una righa all'interno della tabella che indica come mappare i nulli?
            na_replace = map_dict[map_dict.ID.isna()]
            if na_replace.shape[0] > 1:
                raise Exception('Esiste un valore None con doppia mappatura')
            else:
                na_replace = na_replace.Valore_TCR.values[0]
            map_dict.dropna(subset=['ID'],inplace=True)
        else:
            na_replace=None
    map_dict = map_dict.set_index('ID')['Valore_TCR'].to_dict()
    df = decodifica_colonna(df, col_name=colonnaDaDecodificare, map_dict=map_dict,
              na_replace=na_replace,
              mantieni_mancanti=mantieni_mancanti,
              default_mancanti=default_mancanti)
    return df
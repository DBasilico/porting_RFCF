from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f

def overwrite_table2(spark, table, df):
    """Funzione per sovrascrivere una tabella mantenendo lo schema.
    :param spark: sparkSession
    :param table: str, nome della tabella
    :param df: spark.dataFrame, dataframe con i dati da salvare
    """
    df.cache()
    print(f"le nuove righe sono: {df.count()}")
    spark.sql(f"truncate table {table}")
    spark.sql(f"REFRESH TABLE {table}")
    df.write.format('parquet').saveAsTable(table, mode='append')
    df.unpersist()

def overwrite_table(spark, df_in, tab_out, table_format='parquet'):
  
  # Salvo il nuovo contenuto in una tabella temporanea
  df_in.write.format(table_format).saveAsTable(tab_out+"_tmp2",mode='append')

  # Elimino il vecchio contenuto
  spark.sql('TRUNCATE TABLE ' + tab_out )
  
  # Ricarico il nuovo contenuto
  tmp = spark.sql("SELECT * FROM " + tab_out + "_tmp2") 
  
  # Store
  tmp.write.format(table_format).mode('append').saveAsTable(tab_out)

  # Drop della tabella temporanea
  spark.sql('DROP TABLE ' + tab_out+ '_tmp2' ) 

def data_snapshot(spark,nomi, df):
    # Recupero id_run_fw e lo casto a stringa per la select successiva
    id_run_fw = df.select('id_run_fw').collect()[0][0]
    id_run_fw = str(id_run_fw)

    # Eseguo un ciclo per ogni nome_tabella contenuto nella lista dei nomi
    i = 0
    while i < len(nomi):

        # Carico le data_snapshot relativi alla tabella e all'id_run_fw in input

        if nomi[i][0:6] == 'vw_tcr':

            # Carico le data_snapshot relativi alla tabella e all'id_run_fw in input
            df_query = spark.sql(
                "SELECT max(data_snapshot) as data_snapshot FROM edl_tcr." + nomi[i] + " where id_run = " + id_run_fw)

        else:
          check = spark.table("edl_neta." + nomi[i])
          if 'vw_neta_fcf_fatt_inca' != nomi[i]:
            # Carico le data_snapshot relativi alla tabella e all'id_run_fw in input
            df_query = spark.sql("SELECT max(data_snapshot) as data_snapshot FROM edl_neta." + nomi[i])

            df_query = df_query.withColumn('data_snapshot', f.col('data_snapshot').cast('int'))

            # Ricavo il massimo
            df_query = df_query.groupBy().max('data_snapshot').withColumnRenamed('max(data_snapshot)', 'data_snapshot')

            # Aggiungo nome_tabella
            df_query = df_query.withColumn('nome_tabella', f.lit(nomi[i]))

        if i == 0:

            # Creo Tabella Finale in cui concatenerò le varie data snapshot per tabella
            df_snapshot = df_query

        else:

            # Concateno le varie data snapshot per tabella
            df_snapshot = df_snapshot.union(df_query)

        i = i + 1

    return df_snapshot


def empty_df(df):
    try:
        if (df.count() > 0):
            val_empty = False
        else:
            val_empty = True

    except:
        val_empty = True

    return val_empty

from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from datetime import timedelta
import calendar as cl
import os
from pyspark.sql import Window
from datetime import datetime, timedelta
from pyspark.sql.types import *
from udfs import *

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
  
  
def pro_die(df, inizio_competenza, fine_competenza, inizio_mese, fine_mese,val='VALORE'):
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
        tmp = spark.table('lab1_db.crv_storico_run').filter(f.col('flg_run_corrente') == 'Y').collect()[0]
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

        
        
class data_snap:
    def __init__(self, spark,id_run):
      tmp = spark.table('lab1_db.crv_run_snapshot').filter(f.col('id_run') == id_run).select('nome_tabella','data_snapshot').collect()
      self.data_snapshot = dict(tmp)

#######################################################
      
def customUnion(df1, df2):
    
  # Selezione le colonne di ogni df
  cols1 = df1.columns
  cols2 = df2.columns
    
  # Creo la lista delle colonne totali
  total_cols = sorted(cols1 + list(set(cols2) - set(cols1)))
  
  col_mancanti_1 = set(total_cols)-set(cols1)
  col_mancanti_2 = set(total_cols)-set(cols2)
  
  df1 = df1.select(['*',*(f.lit(None).alias(i) for i in col_mancanti_1)])
  df2 = df2.select(['*',*(f.lit(None).alias(i) for i in col_mancanti_2)])
  
  df = df1.unionByName(df2)
  
  return df


def overwrite_table(spark,table,df):
  spark.sql(f"""truncate table {table}""")
  df.write.format('parquet').saveAsTable(table,mode='append')

  
def sql_create(table,df,stored='parquet'):
    schema = df.dtypes
    ss= f"""CREATE TABLE IF NOT EXISTS {table}\n(\n"""
    for i in schema:
      if i[1] == 'long':
        i[1] ='bigint'
        
      if i != schema[-1]:
        ss += f"""\t{i[0]} {i[1].upper()},\n"""
      else:
        ss += f"""\t{i[0]} {i[1].upper()}\n"""
    
    ss += f""")\nSTORED AS {stored.upper()}"""
    
    return ss

  
def window_function_ods(partition, order):
  w = Window.partitionBy(partition).orderBy(order.desc())
  return w
  

def comp_df(df,inizio_competenza,fine_competenza,nome_col_inizio='start_date',nome_col_fine='end_date'):
    """Metodo per dividere l'intervallo di competenza in mesi.

    Input:  df                            --> dataframe di partenza
            inizio_competenza             --> colonna data inizio competenza
            fine_competenza               --> colonna data fine competenza
            nome_col_inizio='start_date'   --> nome colonna StartDate di output
            nome_col_fine='end_date'       --> nome colonna EndDate di output

    Output: df                            --> dataframe con periodo di competenza esploso su più record
            """

    df = df.withColumn('NUM_M', (f.month(fine_competenza) + (12 - f.month(inizio_competenza) + 1) + (
              f.year(fine_competenza) - f.year(inizio_competenza) - 1) * 12))

    compt = f.udf(lambda x, y, z: comp(x, y, z), StructType([ \
        StructField(nome_col_inizio, ArrayType(TimestampType())), \
        StructField(nome_col_fine, ArrayType(TimestampType())) \
        ]))

    df = df.withColumn("Dates", compt(inizio_competenza, fine_competenza, "NUM_M"))

    df = df.withColumn("temp_col", f.arrays_zip("Dates." + nome_col_inizio, "Dates."+ nome_col_fine)) \
      .withColumn("temp_col", f.explode("temp_col")) \
      .withColumn(nome_col_inizio,f.col("temp_col.0")) \
      .withColumn(nome_col_fine,f.col("temp_col.1")) \
      .drop("temp_col").drop('Dates')
    return df
from datetime import datetime
from pyspark.sql.types import *
import pyspark.sql.functions as f

from shared_lib import comp, run_par


def rename_commodity(commodity):
    """Metodo per rinominare la commodity.

    Input:  commodity   --> stringa commodity

    Output: commodity   --> stringa commodity
            """

    if (commodity == 'ENERGIA ELETTRICA' or commodity == 'Energia Elettrica'):
        commodity = 'EE'
        return commodity
    else:
        commodity = 'GAS'
        return commodity

rename_commodity_udf = f.udf(lambda x: rename_commodity(x), StringType())
"""User Define Function
Input: commodity  --> colonna commodity

Output: commodity --> colonna commodity
        """



def distinct_pk(spark, startDateEmissione, endDateEmissione, commodity):
    """Metodo per identificare le key distinte per periodo di tempo e commodity.

    Input:  spark                 --> sparksession
            startDateEmissione   --> colonna data inizio periodo da analizzare
            endDateEmissione     --> colonna data fine periodo da analizzare
            commodity            --> colonna commodity

    Output: distinct_pk_month    --> call funzione
            """

    global df_voce, df_componente, df_causale, df_neta

    fm = "%Y-%m-%d %H:%M:%S"
    tempS = datetime.strptime(startDateEmissione, fm)
    tempE = datetime.strptime(endDateEmissione, fm)

    NUM_M = ((tempE.year - tempS.year) * 12) + (tempE.month - tempS.month + 1)
    col = comp(tempS, tempE, NUM_M)
    col = set(zip(col[0], col[1]))

    df_voce = spark.sql("SELECT id_voce, codice_voce, descrizione_voce, cod_sistema_provenienza as cod_sistema_provenienza_voce FROM edl_ods.vw_dg_l1_voce where flag_ultimo_record_valido = 1")
    df_componente = spark.sql("SELECT id_componente, codice_componente, componente, cod_sistema_provenienza as cod_sistema_provenienza_componente FROM edl_ods.vw_dg_l1_componente where flag_ultimo_record_valido = 1")
    df_neta = spark.sql("SELECT sist_fat as sist_fat_neta, sistema_nome  FROM lab1_db.rfcf_mapping_neta")

    if(commodity == 'EE'):
      commodity =" in ('ENERGIA ELETTRICA', 'Energia Elettrica')"
    elif(commodity == 'GAS'):
      commodity =" in ('GAS NATURALE', 'GAS METANO')"

    for i in col:
      distinct_pk_month(spark, commodity, i[0].strftime(fm), i[1].strftime(fm))



def distinct_pk_month(spark,commodity, startDateEmissione, endDateEmissione):
    """Metodo per scrivere le key distinte per mese e commodity.

    Input:  spark                 --> sparksession
            commodity            --> stringa commodity da analizzare
            startDateEmissione   --> data inizio periodo da analizzare
            endDateEmissione     --> data fine periodo da analizzare

    Output: rfcf_pk_dett_fatt    --> scrittura su DB delle key trovate per mese/commodity
            """
    param = run_par(spark)
    query = "select codice_voce, codice_componente, codice_causale, cod_sistema_provenienza as sist_fat, codice_quota_parte, commodity, data_emissione from edl_ods.vw_dg_l1_dettaglio_fattura \
          where data_emissione >= cast('"+ startDateEmissione +"'as timestamp) \
          and data_emissione <= cast('"+ endDateEmissione +"' as timestamp) \
          and commodity"+ commodity

    df_dettaglio_fattura = spark.sql(query)

    cond_voce = [df_dettaglio_fattura.codice_voce == df_voce.id_voce, df_dettaglio_fattura.sist_fat == df_voce.cod_sistema_provenienza_voce]
    cond_componente = [df_dettaglio_fattura.codice_componente == df_componente.id_componente, df_dettaglio_fattura.sist_fat == df_componente.cod_sistema_provenienza_componente]

    df_dettaglio_fattura = df_dettaglio_fattura.join(df_voce, cond_voce) \
      .join(df_componente, cond_componente) \
      .join(df_neta, df_neta.sist_fat_neta == df_dettaglio_fattura.sist_fat)

    df = df_dettaglio_fattura.select(f.concat(df_voce.codice_voce,f.lit('#'),df_dettaglio_fattura.codice_quota_parte,f.lit('#'),df_componente.codice_componente,f.lit('#'),df_neta.sistema_nome).alias("PK"),
                                     df_dettaglio_fattura.commodity,
                                     df_dettaglio_fattura.data_emissione,
                                     df_voce.codice_voce,
                                     df_voce.descrizione_voce,
                                     df_dettaglio_fattura.codice_quota_parte,
                                     df_componente.codice_componente,
                                     df_componente.componente,
                                     df_dettaglio_fattura.sist_fat,
                                     df_neta.sistema_nome)

    df = df.withColumn("ANNOMESE", f.concat(f.year(df.data_emissione),
                                          f.format_string("%02d", f.month(df.data_emissione))))
    df = df.drop(df.data_emissione).dropDuplicates(['PK']).withColumn('id_run', f.lit(param.id_run)).withColumnRenamed('pk','tipo_riga')
    df.write.format("parquet").saveAsTable("lab1_db.rfcf_tipo_riga_dett_fatt", mode='append')



def diff_pk(spark):
    """Metodo per scrivere le nuove key trovate.

    Input:  spark           --> sparksession

    Output: rfcf_diff_pk   --> scrittura su DB delle nuove key trovate
            """
    param = run_par(spark)

    actual_pk = spark.sql("SELECT * FROM lab1_db.vw_rfcf_pk_dett_fatt")
    expect_pk = spark.sql("SELECT * FROM lab1_db.rfcf_pk_mapping_tcr")
    actual_pk = actual_pk.withColumn("commodity", rename_commodity_udf(actual_pk.commodity))

    cond_join = [actual_pk.pk == expect_pk.pk, actual_pk.commodity == expect_pk.commodity]
    diff_pk = actual_pk.join(expect_pk, cond_join, 'left_anti')
    diff_pk = diff_pk.withColumn("id_run", f.lit(param.id_run)).withColumnRenamed('pk','tipo_riga')
    diff_pk.write.format("parquet").saveAsTable("lab1_db.rfcf_diff_tipo_riga", mode='append')

def check_if_col_has_null(df,sensible_cols):
  
  cols_with_error = []
  
  for i in sensible_cols:
    print('Controllo colonna {col} per valori nulli.'.format(col = i))
    if (df.filter(f.col(i).isNull()).count() != 0):
      cols_with_error.append(i)
  assert len(cols_with_error) == 0 , cols_with_error
  
def diff_check(output,test_output,key):
  diff = output.unionByName(test_output).subtract(test_output.intersect(output))

  assert diff.count() == 0, 'mismatch sulle seguenti '+key+'. {show}'.format(show=diff.select(key).distinct().collect())


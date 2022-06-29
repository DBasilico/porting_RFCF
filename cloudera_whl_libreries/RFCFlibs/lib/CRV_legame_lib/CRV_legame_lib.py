import pyspark.sql.functions as f
from pyspark.sql.types import *
from udfs import *

def calcolo_pesi(df_ret, df_nret, campi_obb, campi_fac):

    df_toKeep = df_ret.select(campi_obb).distinct()

    df_nret = df_nret.join(df_toKeep, campi_obb)

    toGb = campi_obb + campi_fac + ['idcampagna']

    df_tot = df_nret.groupBy(campi_obb) \
        .agg(f.sum('valore').alias('tot')).cache()

    df_tot = df_tot.rdd.toDF()

    df_agg_tmp = df_nret.groupBy(toGb).agg(f.sum('valore').alias('parz'))

    out_tmp = df_tot.join(df_agg_tmp, campi_obb)

    out_tmp = out_tmp.withColumn('percentuale', f.col('parz') / f.col('tot'))#.drop('parz', 'tot')

    return out_tmp
import pyspark.sql.functions as f
from pyspark.sql import window as w
from collections import OrderedDict
from pyspark.sql.types import *

from udfs import *

def comp_date_canone(df, inizio_competenza, fine_competenza, nome_col_inizio='start_date', nome_col_fine='end_date'):
    """Metodo per dividere l'intervallo del canone in mesi di competenza.

    Input:  df                            --> dataframe di partenza
            valore_in                     --> nome colonna del valore input
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


def rimoz_dupl_comp(df_canone, fk, mese, data_fatt):
    """" Funzione che elimina le righe corrispondenti a mesi duplicati a causa della competenziazione nella loro seconda data di emissione  (per non contarli due volte).

        Input:  df_canone       -->         dataframe con dati calendarizzati in input
                fk              -->         str o list type. colonna/e su cui eliminare i duplicati.
                mese            -->         str type. Campo che specifica il mese di competenza della riga per poter individuare i duplicati
                data_fatt       -->         str type. Campo contenente la data di emissione.

        Output: dupl            -->         dataframe con duplicati rimossi.
    """

    if type(fk)==str:
        win_dupl = w.Window.partitionBy([fk, mese]).orderBy(data_fatt).rowsBetween(w.Window.unboundedPreceding, w.Window.currentRow)
    elif type(fk)==list:
        fk.append(mese)
        win_dupl = w.Window.partitionBy(fk).orderBy(data_fatt).rowsBetween(w.Window.unboundedPreceding, w.Window.currentRow)
    else:
        raise Exception("fk must be a string or a list of columns names")

       # """elimino per le forniture competenziate le metà di mese rimaste nella successiva data di fatturazione"""
    dupl = df_canone.withColumn('rank_dupl', f.dense_rank().over(win_dupl)).filter(f.col('rank_dupl') == 1).drop('rank_dupl')
    return dupl


def canone_data_fatt(dupl, fk, mese, data_fatt):
    """" Funzione che conta il numero di mesi associati ad ogni data di emissioni per ogni chiave e li moltiplica per il valore mensile del canone (9).

        Input:  dupl            -->         dataframe con dati con duplicati per mese rimossi.
                fk              -->         str o list type. colonna/e su cui contare i mesi.
                mese            -->         str type. Campo che specifica il mese di competenza della riga per poter individuare i mesi.
                data_fatt       -->         str type. Campo contenente la data di emissione.

        Output: df_valore            -->    dataframe con valori assegnati.
    """

    if type(fk)==str:
        win_mesi = w.Window.partitionBy([fk, data_fatt]).rowsBetween(w.Window.unboundedPreceding,
                                                                     w.Window.unboundedFollowing)
    elif type(fk)==list:
        fk.append(mese)
        win_mesi = w.Window.partitionBy(fk).rowsBetween(w.Window.unboundedPreceding,
                                                                     w.Window.unboundedFollowing)
    else:
        raise Exception("fk must be a string or a list of columns names")



    """conto il numero di mesi e li moltiplico per 9"""
    count = dupl.withColumn('num_mesi', f.count(mese).over(win_mesi))
    df_valore = count.dropDuplicates([fk, data_fatt])
    df_valore = df_valore.withColumn('VALORE', f.col('num_mesi') * 9)
    df_valore = df_valore.drop('num_mesi')
    return df_valore


def clusterize(df):
  """" Funzione che organizza in cluster di calendarizzazione se presenti le colonne 'tipologia', 'gruppo' e 'mercato'.

      Input:  df     -->         dataframe con senza cluster.

      Output: df     -->         dataframe  clusterizzati.
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
  return df
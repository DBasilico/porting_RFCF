from pyspark.sql.types import *
import pyspark.sql.functions as f

def info_commodity_recovery(commodity):
    if (commodity == 'EE'):
        commodity = ['ENERGIA ELETTRICA', 'Energia Elettrica']
        suff = "pwr"
    elif (commodity == 'GAS'):
        commodity = ['GAS NATURALE', 'GAS METANO']
        suff = "gas"

    return commodity, suff

def info_commodity_recovery_xe(commodity):
    if (commodity == 'EE'):
        commodity = "PWR"
        suff = "pwr"
    elif (commodity == 'GAS'):
        commodity = "GAS"
        suff = "gas"

    return commodity, suff


def calcolo_iva(df, commodity):
  
  iva = df.select('cod_pratica_crm','fk_testata','sist_fat','data_inizio_competenza','data_fine_competenza','valore_comp','saldo_acconto','tipologia_fatturato','iva','annomese','data_emissione','data_scadenza')
  #     filtro le righe che possiedono iva 0
  iva = df.filter(iva.iva != 0)
  #     aggiungo colonna tmp con calcolo per riga dell'iva
  iva = iva.withColumn('valore_comp',f.col('valore_comp')*f.col('iva')/f.lit(100) )
  #     raggruppo per le colonne base
  iva = iva.groupBy(iva.cod_pratica_crm, iva.fk_testata,iva.sist_fat,iva.data_inizio_competenza,iva.data_fine_competenza,iva.saldo_acconto,\
                    iva.tipologia_fatturato, iva.annomese, iva.data_emissione,iva.data_scadenza)\
           .agg(f.sum(iva.valore_comp)).withColumnRenamed('sum(valore_comp)','valore_comp')\
           .withColumn('valore_comp',f.col('valore_comp').cast("double"))
  #     aggiungo colonne utili
  iva = iva.withColumn("anno_comp", f.year(iva.data_inizio_competenza))
  iva = iva.withColumn("mese_comp", f.month(iva.data_inizio_competenza))
  iva = iva.withColumn('componente_tcr', f.lit('IVA'))
  iva = iva.withColumn('cod_voc_tar', f.lit('IVA'))
  iva = iva.withColumn('flg_ricavo', f.lit('N'))
  iva = iva.withColumn('commodity', f.lit(commodity))
  
  return iva
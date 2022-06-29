from collections import OrderedDict
import pyspark.sql.functions as f
from pyspark.sql import window
from pyspark.sql.types import *
from datetime import datetime

from shared_lib import customUnion


def cod_dirette(codif_df, commodity):
    """Metodo per identificazione colonne da estrarre forniture per codifiche dirette.

    Input:  codif_df    --> dataframe elenco codifiche tariffario (sheet TipoCodifica)
            commodity   --> stringa con commodity

    Output: col_dirette --> lista colonne da estrarre da forniture
            """

    col_dirette = codif_df.filter((codif_df['TipoCodifica'] == 'Diretta') & (codif_df['SIST_FAT'] == 'NETA BR') & (codif_df['COMMODITY'].isin(commodity, 'ALL'))) \
        .select(codif_df['CampoCodifica']).dropDuplicates().collect()
    col_dirette = [*(i[0] for i in col_dirette)]
    return col_dirette

########################################################################################################

def cod_valori(forn_df , valori_df, commodity):
    """Metodo per applicazione transcodifiche valori a forniture.

    Input:  forn_df    --> dataframe forniture a cui applicare mapping
            valori_df  --> dataframe codifiche valori (sheet ValoriCodifica)
            commodity   --> stringa con commodity

    Output: forn_df    --> dataframe forniture con colonne aggiunte colonne transcodificate
            col_valori --> lista colonne decodificate valori
            """

    valori_dict = dict(valori_df.filter(valori_df['COMMODITY'].isin('ALL',commodity)) \
                .dropDuplicates(['TOG_VALORE', 'CAMPO_TCR_SRC', 'CAMPO_TCR_DEST']).filter(f.col('CAMPO_TCR_DEST') != '') \
                .select('TOG_VALORE', f.struct('CAMPO_TCR_SRC','CAMPO_TCR_DEST')).collect())

    col_valori = [*(i['CAMPO_TCR_DEST'] for i in valori_dict.values())]

    for i in valori_dict.keys():
        cond_valori = [valori_df['COMMODITY'].isin(forn_df['COMMODITY'], 'ALL')
            , valori_df['TOG_VALORE'] == i,
                       forn_df[valori_dict[i]['CAMPO_TCR_SRC']] == valori_df['ValoreTCR']]

        forn_df = forn_df.join(valori_df, cond_valori, 'leftouter') \
            .select([*(forn_df[i] for i in forn_df.columns), f.coalesce('ValoreAsse', f.lit('#--#'))
                    .alias(valori_dict[i]['CAMPO_TCR_DEST'])])
    return forn_df, col_valori

########################################################################################################

def cod_campi(campi_df, commodity):
    """Metodo per applicazione transcodifiche campi a forniture.

    Input:  campi_df  --> dataframe codifiche campi (sheet CampiCodifica)
            commodity   --> stringa con commodity

    Output: query      --> stringa con query da applicare a tabella forniture
            col_campi  --> lista colonne decodificate campi
            """

    campi_dict = dict(campi_df.filter(campi_df['COMMODITY'].isin(commodity, 'ALL')).dropDuplicates(['TOG_VALORE', 'CAMPO_TCR_DEST']) \
                      .select('CAMPO_TCR_DEST', 'TOG_VALORE').filter(f.col('CAMPO_TCR_DEST') != '').collect())
    col_campi = [*(i for i in campi_dict.keys())]

    query = "select *"
    for i in campi_dict.keys():
        # part to create if
        temp = campi_df.filter(campi_df['TOG_VALORE'] == campi_dict[i]).filter(campi_df['CAMPO_TCR_DEST'] == i)
        temp_matrix = temp.select('FORMULA_TCR', 'ValoreAsse').collect()
        dyn_if = [*(' if(' + i['FORMULA_TCR'] + " , '" + i['ValoreAsse'] + "'" for i in temp_matrix)]
        dyn_if = ',' + ','.join(dyn_if) + " ,'#--#'" + ')' * len(temp_matrix) + ' as ' + i
        query += dyn_if
    query += ' from tforn'
    return query, col_campi

########################################################################################################

def transcod_commodity(spark, commodity,cod,valori,campi,prov,dist,ds_dict, anno_inizio, anno_fine):
  """Metodo per applicazione transcodifiche campi a forniture.

  Input:    spark           --> sparksession
            commodity       --> stringa con commodity
            cod             --> dataframe codifiche tipo (sheet TipoCodifica)
            valori          --> dataframe codifiche valori (sheet ValoriCodifica)
            campi           --> dataframe codifiche campi (sheet CampiCodifica)
            prov            --> dataframe per recuperare i cod_regione e cod_provincia
            dist            --> dataframe dei distributori

  Output: forn    --> dataframe con le forniture associate ai tog e valori
          """

  if (commodity == 'EE'):
    forn = spark.sql(f"""SELECT * from lab1_db.rfcf_vw_forniture_pwr
    where COD_STATO_PRATICA <> 'ANNULLATO' and DATA_CESSAZIONE >= cast('{anno_inizio}-01-01' as TIMESTAMP) 
    and DATA_DECORRENZA <= cast('{anno_fine}-12-31' as TIMESTAMP)""") \
    .withColumn('INIZIO_VALIDITA', f.lit(str(anno_inizio)+'-01-01').cast('timestamp')).withColumn('FINE_VALIDITA', f.lit(str(anno_fine)+'-12-31').cast('timestamp'))
    cols = ['FK_FORN_FATT', 'SIST_FAT', 'COMMODITY', 'COD_PRATICA','INIZIO_VALIDITA','FINE_VALIDITA', 'COD_PRATICA_CRM', 'CODICE_OP',
            'CODICE_UNIVOCO_CE'] #'TENSIONE',
  elif commodity == 'GAS':
    forn = spark.sql(f"""SELECT * from lab1_db.rfcf_vw_forniture_gas
    where COD_STATO_PRATICA <> 'ANNULLATO' and DATA_CESSAZIONE >= cast('{anno_inizio}-01-01' as TIMESTAMP) 
    and DATA_DECORRENZA <= cast('{anno_fine}-12-31' as TIMESTAMP)""") \
    .withColumn('INIZIO_VALIDITA', f.lit(str(anno_inizio)+'-01-01').cast('timestamp')).withColumn('FINE_VALIDITA', f.lit(str(anno_fine)+'-12-31').cast('timestamp'))
    cols = ['FK_FORN_FATT', 'SIST_FAT', 'COMMODITY', 'COD_PRATICA', 'INIZIO_VALIDITA','FINE_VALIDITA', 'COD_PRATICA_CRM', 'CODICE_OP',
            'CODICE_UNIVOCO_CE']



  cond_dist = [dist.id_distributore == forn.cod_distributore]
  cond_prov = ['CODICE_ISTAT_ALFANUMERICO']

  forn = forn.join(prov, cond_prov, 'left')
  forn = forn.join(dist, cond_dist, 'left')
  col_dirette = cod_dirette(cod, commodity)

  cols.extend(col_dirette)

  forn, col_valori = cod_valori(forn, valori,commodity)
  cols.extend(col_valori)

  query, col_campi = cod_campi(campi, commodity)

  cols.extend(col_campi)
  cols = list(OrderedDict.fromkeys(cols))

  forn.createTempView('tforn')
  forn = spark.sql(query)
  spark.catalog.dropTempView("tforn")
  forn = forn.select(cols).dropDuplicates(['fk_forn_fatt'])
  return forn

########################################################################################################


def calcola_forniture_assi(cod, valori, campi, commodity, forn_transcod):
    """Metodo per il calcolo delle forniture e assi.

    Input:  cod         --> dataframe codifiche tipo (sheet TipoCodifica)
            valori      --> dataframe codifiche valori (sheet ValoriCodifiche)
            campi       --> dataframe codifiche campi (sheet CampiCodifiche)
            commodity   --> stringa commodity considerata
            forn_transc --> dataframe forniture transcodificate

    Output: df_cfa      --> dataframe
            """
    cod = cod.filter((cod['SIST_FAT'].isin('NETA BR', 'ALL')) & (cod['COMMODITY'].isin('ALL', commodity)) & (
                cod['TipoCodifica'] == 'Diretta')) \
        .withColumnRenamed('CampoCodifica', 'NOME_COLONNA') \
        .select('TOG_VALORE', 'NOME_COLONNA').dropDuplicates()

    valori = valori.filter((valori['SIST_FAT'].isin('NETA BR', 'ALL')) & (valori['COMMODITY'].isin('ALL', commodity)) & (
        valori['ValoreTCR'] != '')) \
        .withColumnRenamed('CAMPO_TCR_DEST', 'NOME_COLONNA') \
        .select( 'TOG_VALORE' , 'NOME_COLONNA').dropDuplicates()

    campi = campi.filter((campi['SIST_FAT'].isin('NETA BR', 'ALL')) & (campi['COMMODITY'].isin('ALL', commodity)) & (
        campi['CAMPO_TCR_DEST'] != '')) \
        .withColumnRenamed('CAMPO_TCR_DEST', 'NOME_COLONNA') \
        .select('TOG_VALORE', 'NOME_COLONNA').dropDuplicates()

    elencoCampi = cod.union(valori).union(campi).collect()
    elencoCampi = [[i,j] for i,j in elencoCampi]

    ss= 'stack('+str(len(elencoCampi))
    for k,v in elencoCampi :
        tmp = ", '" +k+ "' ," + v 
        ss += tmp
    ss += ') as (TOG_VALORE, ASSE_VALORE)'
    

    df_cfa = forn_transcod.selectExpr('FK_FORN_FATT', 'cod_pratica', ss)
    df_cfa = df_cfa.withColumn('ASSE_VALORE', f.coalesce( f.col('ASSE_VALORE'), f.lit('#--#')  ))
    df_cfa = df_cfa.filter(df_cfa['ASSE_VALORE'] != '#--#')
    return df_cfa

########################################################################################################

def calcola_fornitura_voci(spark, forn_transc,commodity, anno_inizio, ds_dict):
    """Metodo per il calcolo del periodo di validità delle forniture per voce.

    Input:  spark       --> sparksession
            forn_transc --> dataframe forniture transcodificate
            commodity   --> stringa commodity considerata
            run_date    --> storicizzazione
            ds_dict     --> valori massima data snapshot tabelle

    Output: df_cfv      --> dataframe
            """
    run_date = datetime(anno_inizio,1,1)
    ds= ds_dict['vw_neta_fcf_rep_forn_vof']
    df_forn_vof = spark.sql("""SELECT codice_fornitura cod_pratica, codice_voce, piano_orario,
                fascia_oraria, data_rif_prezzi, data_rif_variabili,
                data_ini, data_ini_applicazione, data_fine
            FROM edl_neta.vw_neta_fcf_rep_forn_vof
            where data_snapshot = """ + ds)

    if(commodity == 'GAS'):
        df_voce = forn_transc.select('cod_pratica', 'COD_PRATICA_CRM', 'SIST_FAT', 'FK_FORN_FATT',
                                     'INIZIO_VALIDITA', 'FINE_VALIDITA', 'CODICE_OP', 'CODICE_UNIVOCO_CE')
    elif commodity == 'EE':
        df_voce = forn_transc.select('cod_pratica', 'COD_PRATICA_CRM', 'SIST_FAT', 'FK_FORN_FATT',
                                     'INIZIO_VALIDITA', 'FINE_VALIDITA', 'CODICE_OP', 'CODICE_UNIVOCO_CE')#, 'TENSIONE')

    df_forn_vof = df_forn_vof.withColumn('data_rif_prezzi', f.when(f.col('data_rif_prezzi').isNull(), \
                                                               f.lit(run_date)).otherwise(df_forn_vof.data_rif_prezzi))

    df_forn_vof = df_forn_vof.withColumn('data_rif_variabili', f.when(f.col('data_rif_variabili').isNull(), \
                                                                  f.lit(run_date)).otherwise(df_forn_vof.data_rif_variabili))

    df_forn_vof = df_forn_vof.withColumn('data_ini_voce_tmp',
                                     f.when(f.col('data_ini') > f.col('data_ini_applicazione'), \
                                            df_forn_vof.data_ini).otherwise(df_forn_vof.data_ini_applicazione))

    df_forn_vof = df_forn_vof.withColumnRenamed('data_fine', 'data_fine_voce_tmp')

    cond_voce = ['cod_pratica']

    df_cfv = df_forn_vof.join(df_voce, cond_voce)

    df_cfv = df_cfv.withColumn('data_ini_voce', f.when(df_cfv['data_ini_voce_tmp'] > df_cfv['INIZIO_VALIDITA'], \
                                                   df_cfv.data_ini_voce_tmp).otherwise(df_cfv.INIZIO_VALIDITA)).drop('data_ini_voce_tmp')

    df_cfv = df_cfv.withColumn('data_fine_voce', f.when(df_cfv['data_fine_voce_tmp'] > df_cfv['FINE_VALIDITA'], \
                                                    df_cfv.FINE_VALIDITA).otherwise(df_cfv.data_fine_voce_tmp)).drop('data_fine_voce_tmp')

    df_cfv = df_cfv.withColumn('check', f.datediff('data_fine_voce', 'data_ini_voce') + 1)
    df_cfv = df_cfv.filter(df_cfv.check > 0).drop('check')
    return df_cfv

########################################################################################################

def calcola_classi_assi(spark, ds_dict):
    """Metodo per il calcolo degli assi delle classi

    Input:  spark    --> sparksession
            ds_dict  --> valori massima data snapshot tabelle

    Output: df_cca  --> dataframe
            """
    ds = ds_dict['vw_neta_fcf_rep_clu']
    df = spark.sql("""SELECT * FROM edl_neta.vw_neta_fcf_rep_clu
                        where data_snapshot =""" + ds)

    colonne = ['clu_classe_fornitura_cod', 'is_default', 'new']


    df_cca = df.select(df['clu_classe_fornitura_cod'], df['is_default'], f.array( *(f.array(df[i], df[i + 1]) for i in range(4, 17, 2))  ).alias('new'))
    df_cca = df_cca.withColumn('new', f.array_except('new', f.array(f.array(f.lit(None), f.lit(None)))))
    df_cca = df_cca.withColumn('new', f.explode('new')) \
        .withColumn('tog_valore', f.col('new')[0]) \
        .withColumn('asse_valore', f.col('new')[1]) \
        .drop('new')


    df_cca = df_cca.filter(f.col('asse_valore').isNotNull())
    df_agg = df_cca.groupby('clu_classe_fornitura_cod', 'is_default').count().withColumnRenamed('count','RANK_TOG')
    df_cca = df_cca.join(df_agg, ['clu_classe_fornitura_cod', 'is_default'])

    df_cca = df_cca.withColumn('tog_valore', f.substring_index('tog_valore', '-', 1)) \
        .withColumnRenamed('clu_classe_fornitura_cod', 'codice_classe')
          
    ## gestione multipli valori per tog asse (fatto dopo rank perchè una sola caratteristica quindi rank != numero di righe)
    df_cca = df_cca.withColumn('asse_valore', f.explode(f.split('asse_valore', ',')))
    df_cca = df_cca.withColumn('asse_valore', f.trim(f.col('asse_valore'))).dropDuplicates(['codice_classe', 'tog_valore', 'asse_valore'])
    
    
    test = df_cca.withColumn('tog_valore', f.split(df_cca['tog_valore'], ', '))
    test = test.withColumn('new', f.arrays_zip('tog_valore')).withColumn('new', f.explode('new'))
    test = test.withColumn('tog_valore', f.col('new.tog_valore')).drop('new')

    df_cca = test.dropDuplicates(['codice_classe', 'tog_valore', 'asse_valore'])
    return df_cca

########################################################################################################

# CALCOLA PREZZI

def preparazione_classi(df_classi):
    """Preparazione dataframe contenente le classi di prezzi

        Input:  df_classi       -->  dataframe contenente le classi di prezzi

        Output: df_classi   -->  dataframe classi pronto per join
                """
    k_elem = ['QUOTA_PARTE', 'CODICE_CE', 'CODICE_CLASSE', 'PIANO_ORARIO', 'FASCIA_ORARIA', 'DATA_INIZIO_PREZZO',
              'CODICE_FORMULA']
    for i in k_elem:
        df_classi = df_classi.withColumn(i, f.coalesce(i, f.lit('')))


    df_classi = df_classi.withColumn('K_SCAGLIONE',
                                               f.concat('QUOTA_PARTE', f.lit('#'), 'CODICE_CE', f.lit('#'), 'CODICE_CLASSE',
                                                        f.lit('#'), 'PIANO_ORARIO', f.lit('#'), 'FASCIA_ORARIA', f.lit('#'),
                                                        'DATA_INIZIO_PREZZO', f.lit('#'), 'CODICE_FORMULA'))

    df_classi = df_classi.withColumn('DATA_INIZIO_PREZZO', df_classi['DATA_INIZIO_PREZZO'].cast('timestamp'))

    df_classi = df_classi.withColumnRenamed('SCAGLIONE', 'SCAGLIONE_FINE')

    windowsdf = window.Window.orderBy('K_SCAGLIONE', 'SCAGLIONE_FINE').partitionBy('K_SCAGLIONE')
    df_classi = df_classi.withColumn('SCAGLIONE_INIZIO',
                        f.when(df_classi['K_SCAGLIONE'] == f.lag('K_SCAGLIONE', 1, '').over(windowsdf), f.lag('SCAGLIONE_FINE', 1, 0).over(windowsdf)).otherwise(0))

    return  df_classi


########################################################################################################


def preparazione_voci(df_voci,df_map_tcr, DATA_INIZIO_RUN, DATA_FINE_RUN):
    """Preparazione dataframe contenente le voci

         Input:  df_voci            -->  dataframe contenente le voci di prezzi
                 df_map_tcr         -->  mapping tcr
                 DATA_INIZIO_RUN    -->  data inizio run
                 DATA_FINE_RUN      -->  data fine run

         Output: df_voci   -->  dataframe voci pronto per join
                 """
    df_voci = df_voci.filter((df_voci['DATA_FINE_PRODOTTO'] >= f.lit(DATA_INIZIO_RUN))
                                           & (df_voci['DATA_INIZIO_PRODOTTO'] <= f.lit(DATA_FINE_RUN)))
    df_voci = df_voci.join(df_map_tcr, 'TIPO_RIGA')
    df_voci = df_voci.filter(df_voci['COMPONENTE_MAPPING'].isin('ACC','IMER'))
    return df_voci

########################################################################################################


def calcola_prezzi(df_classi, df_voci, DATA_INIZIO_RUN, DATA_FINE_RUN):
    """Join dataframe classi e voci con filtri su date di validitò con solo componenti non di ricavo ACC e IMER

         Input:  df_classi     -->  dataframe contenente le classi di prezzi
                 df_voci       -->  dataframe contenente le voci di prezzi
                 df_map_tcr    -->  dataframe contenente mapping componenti TCR
                 DATA_INIZIO_RUN    -->  data inizio run
                 DATA_FINE_RUN      -->  data fine run

         Output: df_out   -->  dataframe con voci-classi filtrato su ACC e IMER
                 """
    join = ['QUOTA_PARTE']
    tmp_prezzi = df_voci.join(df_classi, join, 'leftouter')

    tmp_prezzi = tmp_prezzi.withColumn('K_QP', f.concat('CODICE_VOCE', f.lit('#'), 'CODICE_OP', f.lit('#'), 'PIANO_ORARIO', f.lit('#'), 'FASCIA_ORARIA'))

    expr_date = (tmp_prezzi['TIPO_DATA_SPONDA_LP'] == f.lit('V')) & \
                (tmp_prezzi['DATA_FINE_PREZZO'] < f.lit(DATA_INIZIO_RUN)) | \
                (tmp_prezzi['DATA_INIZIO_PREZZO'] > f.lit(DATA_FINE_RUN))
    df_out = tmp_prezzi.filter(~expr_date).filter(tmp_prezzi['DATA_INIZIO_PREZZO'] < tmp_prezzi['DATA_FINE_PREZZO'])


    return df_out

########################################################################################################

def calcola_forniture_classi(spark, commodity,ds_dict):
    """ Funzione che calcola tutte le possibili classi applicabili ad una fornitura per fare una prima scrematura

    Input:  spark           --> spark session
            commodity       --> variabile per la commodity
            ds_dict         --> valori massima data snapshot tabelle

    Output: df_forn_classi  --> dataframe forniture-classi
    """
    df_cfv = spark.table('lab1_db.rfcf_tariffario_forn_voci').filter(f.col('commodity')==commodity)
    df_classiVoce = spark.table('lab1_db.rfcf_tariffario_prezzi')
    df_classiVoce = df_classiVoce.filter(f.col('TIPO') == 'ASSI')
    df_classiVoce = df_classiVoce.dropDuplicates(['CODICE_OP', 'CODICE_VOCE', 'CODICE_CLASSE']).select(
        'CODICE_OP', 'CODICE_VOCE', 'CODICE_CLASSE')

    df_forn_classi = df_cfv.select('FK_FORN_FATT', 'COD_PRATICA', 'CODICE_OP', 'CODICE_VOCE').dropDuplicates()

    df_forn_classi = df_forn_classi.join(df_classiVoce, ['CODICE_OP', 'CODICE_VOCE'])

    ds= ds_dict['vw_neta_fcf_rep_clu']

    df_default = spark.table('edl_neta.vw_neta_fcf_rep_clu').filter(f.col('data_snapshot')== ds)
    df_default = df_default.select('clu_classe_fornitura_cod', 'is_default')
    df_default = df_default.withColumnRenamed('clu_classe_fornitura_cod', 'CODICE_CLASSE').withColumnRenamed(
        'is_default', 'CLASSE_DEFAULT')

    df_forn_classi = df_forn_classi.join(f.broadcast(df_default), 'CODICE_CLASSE')
    df_forn_classi = df_forn_classi.withColumn('COMMODITY', f.lit(commodity)).distinct()
    return df_forn_classi

########################################################################################################

def applica_classi(spark,commodity):
    """ Funzione che applica le classi alle forniture

        Input:  spark           --> spark session
                commodity       --> variabile per la commodity

        Output: df_out          --> dataframe con classi applicate
        """

    if commodity == 'EE':
        df_trans = spark.table('lab1_db.rfcf_tariffario_transcodifiche_pwr')
    elif commodity == 'GAS':
        df_trans = spark.table('lab1_db.rfcf_tariffario_transcodifiche_gas')

    df_forn_assi = spark.table('lab1_db.rfcf_tariffario_forn_assi').filter(f.col('COMMODITY') == commodity)\
                        .withColumn('ASSE_VALORE',f.when(f.col('TOG_VALORE')=='STRB_REG',f.lpad('ASSE_VALORE',2,'0') ).otherwise(f.col('ASSE_VALORE') ) )
    df_classi_assi = spark.table('lab1_db.rfcf_tariffario_classi_assi')
    df_forn_classi = spark.table('lab1_db.rfcf_tariffario_forn_classi').filter(f.col('COMMODITY') == commodity)

    df_out = df_forn_classi.filter(f.col('CLASSE_DEFAULT') == 'S').select('FK_FORN_FATT', 'COD_PRATICA', 'CODICE_CLASSE')

    df_trans = df_trans.select(f.col('FK_FORN_FATT'), f.col('COD_PRATICA'), f.col('COD_PRATICA').alias('CODICE_CLASSE'))
    df_out = df_out.unionByName(df_trans).distinct()


    df_out = df_out.join(df_classi_assi.select('CODICE_CLASSE', f.col('is_default').alias('CLASSE_DEFAULT'), 'RANK_TOG'), 'CODICE_CLASSE', 'left')
    df_out = df_out.withColumn('RANK_TOG', f.coalesce(f.col('RANK_TOG'), f.lit(0)))
    df_out = df_out.withColumn('CLASSE_DEFAULT', f.coalesce(f.col('CLASSE_DEFAULT'), f.lit('N')))

    df_forn_classi = df_forn_classi.select('FK_FORN_FATT', 'COD_PRATICA', 'CODICE_CLASSE')

    df_out2 = df_forn_classi.join(df_classi_assi.select('CODICE_CLASSE', 'TOG_VALORE', 'ASSE_VALORE', 'RANK_TOG',f.col('is_default')\
                                                        .alias('CLASSE_DEFAULT')), 'CODICE_CLASSE')

    cond_join = ['FK_FORN_FATT','TOG_VALORE', 'ASSE_VALORE' ]
    df_out2 = df_out2.join(df_forn_assi.select('FK_FORN_FATT', 'ASSE_VALORE', 'TOG_VALORE',f.lit(1).alias('CHECK_FORNITURE')), cond_join, 'left')


    df_tog_soddisfatti = df_out2.groupby('FK_FORN_FATT','CODICE_CLASSE').agg(f.count('CHECK_FORNITURE').alias('NUM_SODDISFATTI'))

    df_out2 = df_out2.join(df_tog_soddisfatti,['FK_FORN_FATT','CODICE_CLASSE'])
    df_out2 = df_out2.filter(f.col('NUM_SODDISFATTI') == f.col('RANK_TOG'))

    df_out = df_out.unionByName(df_out2.select('FK_FORN_FATT', 'COD_PRATICA', 'CODICE_CLASSE', 'RANK_TOG', 'CLASSE_DEFAULT')).distinct()
    return df_out

########################################################################################################

def applica_prezzi(spark, commodity,df_classi_applicate):
    """ Funzione che applica i prezzi alle classi delle forniture

        Input:  spark                   --> spark session
                commodity               --> variabile per la commodity
                df_classi_applicate     --> dataframe contenente le forniture con classi già applicate

        Output: df_out                  --> dataframe con prezzi applicati
        """

    df_forn_voci = spark.table('lab1_db.rfcf_tariffario_forn_voci').filter(f.col('commodity')==commodity)
    df_forn_voci = df_forn_voci.select( 'COD_PRATICA', 'FK_FORN_FATT', 'CODICE_OP',
                                        f.col('CODICE_UNIVOCO_CE').alias('CODICE_CE'),
                                        'CODICE_VOCE', 'PIANO_ORARIO', 'FASCIA_ORARIA',
                                        'DATA_RIF_PREZZI', 'DATA_RIF_VARIABILI', 'DATA_INI_VOCE',
                                        'DATA_FINE_VOCE')

    df_classi_applicate = df_classi_applicate.select( 'FK_FORN_FATT', 'CODICE_CLASSE', 'RANK_TOG')
    df_out_tmp = df_classi_applicate.join(df_forn_voci,'FK_FORN_FATT')

    df_prezzi = spark.table('lab1_db.rfcf_tariffario_prezzi')

    df_tmp = df_prezzi.select('CODICE_CLASSE',
         'CODICE_OP',
         'CODICE_VOCE',
         'PIANO_ORARIO',
         'FASCIA_ORARIA',
         'COMPONENTE_MAPPING',
		 'QUOTA_PARTE',
		 'QUOTA_PARTE_PADRE',
		 'PRODOTTO',
		 'LISTINO',
		 'COMPONENTE',
		 'TIPO_DATA_SPONDA_LP',
		 'VALORE',
		 'DATA_INIZIO_PREZZO',
		 'DATA_FINE_PREZZO',
		 'UDM',
		 'UDM_SCAGLIONE',
		 'CODICE_FORMULA',
		 'SCAGLIONE_INIZIO',
		 'SCAGLIONE_FINE',
		 'DATA_INIZIO_PRODOTTO',
		 'DATA_FINE_PRODOTTO')


    df_out = df_out_tmp.join(df_tmp,['CODICE_CLASSE', 'CODICE_OP', 'CODICE_VOCE', 'PIANO_ORARIO', 'FASCIA_ORARIA'])

    df_agg= df_out.groupby('FK_FORN_FATT', 'CODICE_VOCE', 'QUOTA_PARTE', 'COMPONENTE').agg(f.max('RANK_TOG').alias('RANK_TOG'))

    df_out = df_out.join(df_agg, ['FK_FORN_FATT', 'CODICE_VOCE', 'QUOTA_PARTE', 'COMPONENTE','RANK_TOG'])


    df_out = df_out.filter(f.col('VALORE').isNotNull() & (f.col('VALORE') != 0))\
            .filter(    ~ ((f.col('TIPO_DATA_SPONDA_LP') == 'C') &
                    (((f.col('DATA_RIF_PREZZI') < f.col('DATA_INIZIO_PREZZO')) | (f.col('DATA_RIF_PREZZI') > f.col('DATA_FINE_PREZZO'))))))


    df_out = df_out.select( 'FK_FORN_FATT',
                            'CODICE_OP',
                            'PIANO_ORARIO',
                            'FASCIA_ORARIA',
                            'DATA_INI_VOCE',
                            'DATA_FINE_VOCE',
                            'COMPONENTE_MAPPING',
                            'PRODOTTO',
                            'LISTINO',
                            'COMPONENTE',
                            'UDM',
                            'UDM_SCAGLIONE',
                            'SCAGLIONE_INIZIO',
                            'SCAGLIONE_FINE',
                            'DATA_INIZIO_PRODOTTO',
                            'DATA_FINE_PRODOTTO',
                            'VALORE',
                            'DATA_INIZIO_PREZZO',
                            'DATA_FINE_PREZZO'
                              )


    return df_out

#########################################################################################################

def calcola_tariffario(df_applica_prezzi, run_anno_inizio, run_anno_fine):
    """ Funzione che formatta l'output per scrivere sulla tabella finale

        Input:  df_applica_prezzi       --> dataframe contenente le forniture con prezzi già applicati
                run_anno_inizio         --> anno inizio run
                run_anno_fine           --> anno fine run

        Output: df_applica_prezzi       --> dataframe con campi corretti
        """

    df_applica_prezzi = df_applica_prezzi.withColumnRenamed('FASCIA_ORARIA', 'FASCIA').withColumn('FASCIA', f.lit('F0'))
    df_applica_prezzi = df_applica_prezzi.withColumnRenamed('COMPONENTE_MAPPING', 'COMPONENTE_TCR')
    df_applica_prezzi = df_applica_prezzi.withColumnRenamed('PRODOTTO', 'ID_PRODOTTO')
    df_applica_prezzi = df_applica_prezzi.withColumnRenamed('LISTINO', 'ID_LISTINO')
    df_applica_prezzi = df_applica_prezzi.withColumnRenamed('COMPONENTE', 'COMPONENTE_NETA')
    df_applica_prezzi = df_applica_prezzi.withColumnRenamed('COMPONENTE', 'COMPONENTE_NETA')
    df_applica_prezzi = df_applica_prezzi.withColumn('FORMULA', f.lit('N'))
    df_applica_prezzi = df_applica_prezzi.withColumnRenamed('UDM_SCAGLIONE', 'UNITA_MISURA_SCAGLIONE')
    df_applica_prezzi = df_applica_prezzi.withColumn('NUMERO_SCAGLIONE', f.lit('ND'))
    df_applica_prezzi = df_applica_prezzi.withColumnRenamed('SCAGLIONE_INIZIO', 'SCAGLIONE_DA')
    df_applica_prezzi = df_applica_prezzi.withColumnRenamed('SCAGLIONE_FINE', 'SCAGLIONE_A')
    df_applica_prezzi = df_applica_prezzi.withColumnRenamed('VALORE', 'PREZZO')

    df_applica_prezzi = df_applica_prezzi.withColumn('FLG_SCAGLIONE', f.when((f.col('SCAGLIONE_DA') == 0) &  (f.col('SCAGLIONE_A') == 9999999999), 'N')
                                                     .otherwise('S'))
    df_applica_prezzi = df_applica_prezzi.withColumn('UDM', f.when(f.col('UDM') == 'EURO/KWH','€/kWh').when(f.col('UDM') == 'EURO/MC','€/mc'))\
                        .withColumnRenamed('UDM', 'UNITA_MISURA')

    df_applica_prezzi = df_applica_prezzi.withColumn('DATA_INIZIO_PRZ', f.greatest('DATA_INIZIO_PRODOTTO', 'DATA_INI_VOCE','DATA_INIZIO_PREZZO', f.lit(datetime(run_anno_inizio,1,1))))
    df_applica_prezzi = df_applica_prezzi.withColumn('DATA_FINE_PRZ_TMP', f.least('DATA_FINE_PRODOTTO', 'DATA_FINE_VOCE','DATA_FINE_PREZZO', f.lit(datetime(run_anno_fine,12,31))))

    df_agg = df_applica_prezzi.groupBy('FK_FORN_FATT', 'COMPONENTE_TCR', 'COMPONENTE_NETA', 'SCAGLIONE_DA').agg(f.max('DATA_FINE_PRZ_TMP').alias('DATA_FINE_PRZ_TMP'))
    df_agg = df_agg.withColumn('DATA_FINE_PRZ', f.lit(datetime(run_anno_fine,12,31)))
    df_applica_prezzi = df_applica_prezzi.join(df_agg, ['FK_FORN_FATT', 'COMPONENTE_TCR', 'COMPONENTE_NETA', 'SCAGLIONE_DA', 'DATA_FINE_PRZ_TMP'], 'left')
    df_applica_prezzi = df_applica_prezzi.withColumn('DATA_FINE_PRZ', f.coalesce(f.col('DATA_FINE_PRZ'), f.col('DATA_FINE_PRZ_TMP'))).drop('DATA_FINE_PRZ_TMP')

    df_applica_prezzi = df_applica_prezzi.filter(~( f.col('DATA_FINE_PRZ')<f.col('DATA_INIZIO_PRZ')))

    return df_applica_prezzi



#########################################################################################################################
# IVA
#########################################################################################################################

# Tariffario IVA - soglia_aliquota
def soglia_aliquota(spark, ds):
    """Metodo per il calcolo della soglie ed aliquota.

    Input:  spark        --> sparksession
            ds           --> valori massima data snapshot tabelle

    Output: aliquota     --> dataframe aliquota
            soglie       --> dataframe soglie
"""
    # Campi interessati
    campi_aliq = "cod_aliquota_iva, aliquota_iva, cod_aliquota_iva_c as cod_aliquota_iva_2"
    campi_soglie = "cod_struttura_soglie, soglia, limite_qta, tipologia_soglia, cod_aliquota_iva"

    # Condizioni where
    cond_aliq = "data_snapshot = '" + ds.data_snapshot['vw_neta_fcf_codici_iva'] + "'"
    cond_soglie = "data_snapshot = '" + ds.data_snapshot['vw_neta_fcf_soglie_rel_cod_iva'] + "'"

    # Inserisco le query
    query_aliq = "SELECT " + campi_aliq + " from edl_neta.vw_neta_fcf_codici_iva where " + cond_aliq
    query_soglie = "SELECT " + campi_soglie + " from edl_neta.vw_neta_fcf_soglie_rel_cod_iva where " + cond_soglie

    # Carico la tabella aliquota
    aliquota = spark.sql(query_aliq)

    # Sistemo aliquote iva e sogna che hanno padre-figlio
    i = 2
    while i <= 10:
        # Campi interesstati
        campi_padre_figlio = "cod_aliquota_iva as cod_aliquota_iva_" + str(i) + \
                             ", aliquota_iva as aliquota_iva_" + str(i) + \
                             ", cod_aliquota_iva_c as cod_aliquota_iva_" + str(i + 1)

        # Inserisco query
        query_padre_figlio = "SELECT " + campi_padre_figlio + " from edl_neta.vw_neta_fcf_codici_iva where " + cond_aliq

        # Aliquota Tmp - join padre figlio
        aliquota_tmp = aliquota.join(spark.sql(query_padre_figlio), 'cod_aliquota_iva_' + str(i), 'left')

        # Aggiorno aliquota
        aliquota = aliquota_tmp.withColumn('aliquota_iva', f.when(f.col('aliquota_iva_' + str(i)).isNull(),
                                                                  f.col('aliquota_iva')).otherwise(
            f.col('aliquota_iva_' + str(i))))

        i = i + 1

    # Aliquota - Step Finale
    aliquota = aliquota.select('cod_aliquota_iva', 'aliquota_iva')

    # Carico la tabella soglie iva
    soglie = spark.sql(query_soglie)

    # Mantengo le sole soglie che mi portano ad un'aliquota
    soglie = soglie.join(aliquota, 'cod_aliquota_iva')

    # Genero gli scaglione
    soglie = soglie.withColumn('scaglione_a',
                               f.when(f.col('limite_qta').isNull(), 9999999999).otherwise(f.col('limite_qta')))
    windowsdf = window.Window.orderBy('cod_struttura_soglie', 'scaglione_a').partitionBy('cod_struttura_soglie')
    soglie = soglie.withColumn('scaglione_da', \
                               f.when(soglie['cod_struttura_soglie'] == f.lag('cod_struttura_soglie', 1, '').over(
                                   windowsdf), \
                                      f.lag('scaglione_a', 1, 0).over(windowsdf)).otherwise(0))

    return aliquota, soglie


# Tariffario IVA - prezzi_componenti
def prezzi_componenti(spark,aliquota, soglie,ds):
    """Metodo per il calcolo prezzi delle componenti.

        Input:  spark        --> sparksession
                aliquota     --> dataframe aliquota
                soglie       --> dataframe soglie
                ds           --> valori massima data snapshot tabelle

        Output: soglie_comp  --> dataframe prezzi componenti

    """

    # Campi interessati
    campi_soglie_comp = "codice_voce, quota_parte, componente, cod_aliquota_iva_p, cod_aliquota_iva_s, cod_struttura_soglie, cod_struttura_soglie_sec"
    campi_map_vc = "map_componente as componente_tcr, map_tipo_riga as cod_voc_tar "
    campi_map_comp = "map_componente as componente_tcr, map_tipologia_prezzo"

    # Condizioni where
    cond_soglie_comp = "data_snapshot = " + ds.data_snapshot['vw_neta_fcf_rep_pro_vof_comp']

    # Inserisco le query
    query_soglie_comp = "SELECT distinct " + campi_soglie_comp + " from edl_neta.vw_neta_fcf_rep_pro_vof_comp where " + cond_soglie_comp
    query_map_vc_pwr = "SELECT " + campi_map_vc + " from lab1_db.rfcf_vw_mapping_voci_contabili_pwr" 
    query_map_vc_gas = "SELECT " + campi_map_vc + " from lab1_db.rfcf_vw_mapping_voci_contabili_gas"
    query_map_comp_pwr = "SELECT " + campi_map_comp + " from lab1_db.rfcf_vw_mapping_tcr_pwr"
    query_map_comp_gas = "SELECT " + campi_map_comp + " from lab1_db.rfcf_vw_mapping_tcr_gas"

    # Carico ed unisco i mapping voci contabili pwr e gas
    map_vc = spark.sql(query_map_vc_pwr)
    map_vc = map_vc.union(spark.sql(query_map_vc_gas))

    # Carico soglie componenti
    tmp = spark.sql(query_soglie_comp)
    tmp = tmp.withColumn('cod_voc_tar', f.concat(tmp.codice_voce, f.lit('#'), tmp.quota_parte, \
                                                 f.lit('#'), tmp.componente, f.lit("#NETA BR")))

    # Aggancio il mapping e mantengo solo le cose mappate
    tmp = tmp.join(map_vc, 'cod_voc_tar', 'left')
    tmp = tmp.dropna(subset='componente_tcr')  # DA FARE? parlandone con Marco si

    # Flag primari e secondari
    flg = soglie.select('cod_struttura_soglie', 'cod_aliquota_iva') \
        .withColumn('flg_soglia_primaria', f.lit('Y')) \
        .withColumn('flg_soglia_secondaria', f.lit('Y')) \
        .withColumn('flg_aliquota_primaria', f.lit('Y')) \
        .withColumn('flg_aliquota_secondaria', f.lit('Y'))

    # Aggancio i flg
    tmp = tmp.join(flg.select('cod_struttura_soglie', 'flg_soglia_primaria').dropDuplicates(), 'cod_struttura_soglie',
                   'left')
    tmp = tmp.join(flg.select('cod_struttura_soglie', 'flg_soglia_secondaria').withColumnRenamed('cod_struttura_soglie',
                                                                                                 'cod_struttura_soglie_sec').dropDuplicates() \
                   , 'cod_struttura_soglie_sec', 'left')
    tmp = tmp.join(flg.select('cod_aliquota_iva', 'flg_aliquota_primaria').withColumnRenamed('cod_aliquota_iva',
                                                                                             'cod_aliquota_iva_p').dropDuplicates() \
                   , 'cod_aliquota_iva_p', 'left')
    tmp = tmp.join(flg.select('cod_aliquota_iva', 'flg_aliquota_secondaria').withColumnRenamed('cod_aliquota_iva',
                                                                                               'cod_aliquota_iva_s').dropDuplicates() \
                   , 'cod_aliquota_iva_s', 'left')

    # Recupero le soglie per le componenti
    #    flg primario
    soglie_comp = tmp.filter(tmp.flg_soglia_primaria == 'Y').select('codice_voce', 'componente_tcr', 'cod_voc_tar',
                                                                    'cod_struttura_soglie').dropDuplicates()
    soglie_comp = soglie_comp.join(
        soglie.select('cod_struttura_soglie', 'tipologia_soglia', 'scaglione_da', 'scaglione_a', 'aliquota_iva'),
        'cod_struttura_soglie')
    #    flg_secondario
    soglie_comp_tmp = tmp.filter(tmp.flg_soglia_secondaria == 'Y').select('codice_voce', 'componente_tcr',
                                                                          'cod_voc_tar', 'cod_struttura_soglie_sec') \
        .withColumnRenamed('cod_struttura_soglie_sec', 'cod_struttura_soglie').dropDuplicates()
    soglie_comp_tmp = soglie_comp_tmp.join(
        soglie.select('cod_struttura_soglie', 'tipologia_soglia', 'scaglione_da', 'scaglione_a', 'aliquota_iva'),
        'cod_struttura_soglie')
    #    unisco primario e secondario
    soglie_comp = soglie_comp.union(soglie_comp_tmp)

    # Recupero le aliquota per le componenti
    #    flg primario
    soglie_ali = tmp.filter(
        (tmp.flg_aliquota_primaria == 'Y') & (tmp.flg_soglia_primaria != 'Y') & (tmp.flg_soglia_secondaria != 'Y')) \
        .select('codice_voce', 'componente_tcr', 'cod_voc_tar', 'cod_aliquota_iva_p') \
        .withColumnRenamed('cod_aliquota_iva_p', 'cod_aliquota_iva').dropDuplicates()
    #    flg_secondario
    soglie_ali = soglie_ali.union(tmp.filter(
        (tmp.flg_aliquota_primaria == 'Y') & (tmp.flg_soglia_primaria != 'Y') & (tmp.flg_soglia_secondaria != 'Y')) \
                                  .select('codice_voce', 'componente_tcr', 'cod_voc_tar', 'cod_aliquota_iva_s') \
                                  .withColumnRenamed('cod_aliquota_iva_s', 'cod_aliquota_iva').dropDuplicates())
    # unisco primario e secondario
    soglie_ali = soglie_ali.join(aliquota.select('cod_aliquota_iva', 'aliquota_iva'), 'cod_aliquota_iva') \
        .withColumn('cod_struttura_soglie', f.lit('ALIQUOTA')) \
        .withColumn('tipologia_soglia', f.lit('ALIQUOTA')) \
        .withColumn('scaglione_da', f.lit(0)) \
        .withColumn('scaglione_a', f.lit(9999999999))
    soglie_ali = soglie_ali.drop('cod_aliquota_iva')

    # Unione
    soglie_comp = customUnion(soglie_comp, soglie_ali)

    # Aggancio il flg_prezzo_medio
    map_comp = spark.sql(query_map_comp_pwr)
    map_comp = map_comp.union(spark.sql(query_map_comp_gas))
    soglie_comp = soglie_comp.join(map_comp, 'componente_tcr', 'left')

    return soglie_comp


# Tariffario IVA - voci_iva
def voci_iva(spark,aliquota, soglie_comp, storico,ds):
    """Metodo per il calcolo iva.

        Input:  spark        --> sparksession
                aliquota     --> dataframe aliquota
                soglie       --> dataframe soglie comp
                storico      --> dati anagrafica run
                ds           --> valori massima data snapshot tabelle

        Output: tariffario   --> dataframe iva

    """
    
    # Recupero informazioni relative al run corrente
    if storico.mese_fine_cons == 0:
      run_date = storico.anno_inizio - 1

    else:
      run_date = storico.anno_inizio  

    # Campi interessati
    campi_forn = "fornitura_cod as codice_fornitura, cod_aliquota_iva, forn_old_codice_fornitura as COD_PRATICA_CRM, spwkf_descrizione"

    # Condizioni where
    cond_forn = "data_snapshot = " + ds.data_snapshot['vw_neta_fcf_fornitura']

    # Inserisco le query
    query_forn = "SELECT distinct " + campi_forn + " from edl_neta.vw_neta_fcf_fornitura where " + cond_forn

    # Richiamo le forniture neta
    forn = spark.sql(query_forn)
    
    forn = forn.withColumn('rank', f.when(f.col('spwkf_descrizione')=='ATTIVATA', 1) \
      .when(f.col('spwkf_descrizione').isin('ATTIVA COMMERCIALMENTE','IN CORSO DI CESSAZIONE'), 2) \
        .otherwise(3))
    
    key_cols = ['COD_PRATICA_CRM']
    w_key = window.Window.partitionBy(key_cols)
    forn = forn.withColumn('to_mantain', f.min('rank').over(w_key)).filter(f.col('rank')==f.col('to_mantain'))
    forn = forn.dropDuplicates(key_cols).drop('to_mantain', 'rank')

    # Aggancio aliquota iva della fornitura
    tariffario = forn.join(
        aliquota.select('cod_aliquota_iva', 'aliquota_iva').withColumnRenamed('aliquota_iva', 'aliquota_iva_fornitura'),
        'cod_aliquota_iva')

    # Richiamo le fornitura voce, per entrambe le commodity
    df_trans = spark.table('lab1_db.rfcf_tariffario_transcodifiche_pwr')
    forn_voci = calcola_fornitura_voci(spark, df_trans, 'EE', run_date, ds.data_snapshot).select('FK_FORN_FATT', 'COD_PRATICA_CRM',
                                                                               'codice_voce') \
        .withColumn('commodity', f.lit('EE')).dropDuplicates()
    df_trans = spark.table('lab1_db.rfcf_tariffario_transcodifiche_gas')
    forn_voci = forn_voci.union(
        calcola_fornitura_voci(spark, df_trans, 'GAS', run_date,ds.data_snapshot).select('FK_FORN_FATT', 'COD_PRATICA_CRM',
                                                                        'codice_voce') \
        .withColumn('commodity', f.lit('GAS')).dropDuplicates())

    # Aggancio il codice_voce alle forniture
    tariffario = tariffario.join(forn_voci, 'COD_PRATICA_CRM', 'left')
    tariffario = tariffario.withColumnRenamed('FK_FORN_FATT', 'fk_forn_fatt').drop('COD_PRATICA_CRM')

    # Aggancio le soglie per componente, tenendo solo i codici voce mappati
    tariffario = tariffario.join(soglie_comp, 'codice_voce', 'left')

    return tariffario



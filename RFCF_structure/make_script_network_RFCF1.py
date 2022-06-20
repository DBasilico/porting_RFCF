import networkx as nx
import pandas as pd

def add_script(G, script_name):
    if not G.has_node(script_name):
        G.add_node(script_name)
    else:
        raise Exception('Script corrente gia` aggiunto')


def add_input_table(G, script_name, table_name, db_name):
    if not G.has_node(script_name):
        raise Exception('Nome script non presente')
    G.add_edge(table_name, script_name, db=db_name)


def add_output_table(G, script_name, table_name, db_name):
    if not G.has_node(script_name):
        raise Exception('Nome script non presente')
    G.add_edge(script_name, table_name, db=db_name)


def in_nodes(G, node):
    ret = list()
    for e in G.edges():
        if e[1] == node:
            ret.append(e)
    return ret


def out_nodes(G, node):
    return list(nx.neighbors(G, node))


G = nx.DiGraph()
all_scripts = list()

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/setup/RFCF_01_start_run.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_configurazione_start_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_storico_run',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_run_snapshot',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/setup/RFCF_00_preparazione_dati_ingestion2.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_tcr.vw_tcr_fcf_ricavi_gas',
                db_name='edl_tcr')
add_input_table(G, script_name=script,
                table_name='edl_tcr.vw_tcr_fcf_ricavi_pwr',
                db_name='edl_tcr')
add_input_table(G, script_name=script,
                table_name='edl_tcr.vw_tcr_fcf_forniture_gas',
                db_name='edl_tcr')
add_input_table(G, script_name=script,
                table_name='edl_tcr.vw_tcr_fcf_forniture_pwr',
                db_name='edl_tcr')
add_input_table(G, script_name=script,
                table_name='edl_tcr.vw_tcr_fcf_mapping_voci_contabili_gas',
                db_name='edl_tcr')
add_input_table(G, script_name=script,
                table_name='edl_tcr.vw_tcr_fcf_mapping_voci_contabili_pwr',
                db_name='edl_tcr')
add_input_table(G, script_name=script,
                table_name='edl_tcr.vw_tcr_fcf_mapping_tcr_gas',
                db_name='edl_tcr')
add_input_table(G, script_name=script,
                table_name='edl_tcr.vw_tcr_fcf_mapping_tcr_pwr',
                db_name='edl_tcr')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_rettifiche_fcf',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_vw_ricavi_gas',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_vw_ricavi_pwr',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_vw_ricavi_gas_anno_prec',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_vw_ricavi_pwr_anno_prec',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_vw_forniture_gas',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_vw_forniture_pwr',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_vw_mapping_voci_contabili_gas',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_vw_mapping_voci_contabili_pwr',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_vw_mapping_tcr_gas',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_vw_mapping_tcr_pwr',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/setup/RFCF_03_config_iniziale.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_int.vw_xe_tcr_tcr_soggetti_lr',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_neta.vw_neta_fcf_fornitura',
                db_name='edl_neta')
add_input_table(G, script_name=script,
                table_name='edl_ods.vw_fcf_soggetti',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='edl_ods.vw_dg_l1_anagrafica_domiciliazioni',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='edl_ods.vw_dg_l1_conto_cliente',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='edl_ods.vw_dg_l1_conti_cessione_br',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_config_anagrafica_comp',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_attributi_forniture',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/fatturato/RFCF_Fatturato_01_progressivo_consumo_gas.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_gas',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_progressivo_consumo_gas',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/fatturato/RFCF_Fatturato_02_fatturato.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_ods.vw_dg_l1_sottotestata',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='edl_ods.vw_dg_l1_fattura',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_pwr',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_fatturato',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/fatturato/RFCF_Fatturato_03_fatturato_xe.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_sottotestatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_testatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp_xe',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_pwr',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_fatturato_xe',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/tariffario_NETA_BR/RFCF_tariffario_01_transcodifiche.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_ods.vw_dg_l1_distributori',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_codifiche_tariffario',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_decodifiche',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_pwr',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_transcodifiche_gas',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_transcodifiche_pwr',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/tariffario_NETA_BR/RFCF_tariffario_02_calcola_prezzi.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_neta.vw_neta_fcf_rep_cotp_prz_clu',
                db_name='edl_neta')
add_input_table(G, script_name=script,
                table_name='edl_neta.vw_neta_fcf_rep_pro_vof_comp',
                db_name='edl_neta')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_pwr',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_prezzi',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/tariffario_NETA_BR/RFCF_tariffario_03_classi_assi.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_neta.vw_neta_fcf_rep_clu',
                db_name='edl_neta')
add_input_table(G, script_name=script,
                table_name='edl_neta.vw_neta_fcf_rep_forn_vof',
                db_name='edl_neta')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_codifiche_tariffario',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_transcodifiche_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_transcodifiche_pwr',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_forn_assi',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_classi_assi',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_forn_voci',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/tariffario_NETA_BR/RFCF_tariffario_04_calcola_forniture_classi.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_forn_voci',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_prezzi',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_forn_classi',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/tariffario_NETA_BR/RFCF_tariffario_05_applica_classi.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_transcodifiche_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_transcodifiche_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_forn_assi',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_classi_assi',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_forn_classi',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_forn_voci',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_prezzi',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_calcolo_tariffario',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/tariffario_NETA_BR/RFCF_tariffario_06_iva_NETA_BR.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_neta.vw_neta_fcf_rep_pro_vof',
                db_name='edl_neta')
add_input_table(G, script_name=script,
                table_name='edl_neta.vw_neta_fcf_soglie_rel_cod_iva',
                db_name='edl_neta')
add_input_table(G, script_name=script,
                table_name='edl_neta.vw_neta_fcf_codici_iva',
                db_name='edl_neta')
add_input_table(G, script_name=script,
                table_name='edl_neta.vw_neta_fcf_fornitura',
                db_name='edl_neta')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_pwr',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_iva',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/tariffario_XE/RFCF_XE_06_tar_xe_FCF.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_xe',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_xe_indicizzati_fw',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_xe_fcf',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/tariffario_XE/RFCF_XE_07_tar_xe_FCF_iva.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_xe.vw_xe_dbo_tbinstallationinfo',
                db_name='edl_xe')
add_input_table(G, script_name=script,
                table_name='edl_xe.vw_xe_gas_tbinstallationinfo',
                db_name='edl_xe')
add_input_table(G, script_name=script,
                table_name='edl_xe.vw_xe_gas_tbitems',
                db_name='edl_xe')
add_input_table(G, script_name=script,
                table_name='edl_xe.vw_xe_dbo_tbitems',
                db_name='edl_xe')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_xe_fcf',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_codifiche_tariffario',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_pwr',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_xe_iva',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/importi/RFCF_importi_01_accise_gas.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calcolo_tariffario',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_progressivo_consumo_gas',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_importi_accise_gas',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/importi/RFCF_importi_02_accise_pwr.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calcolo_tariffario',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_pwr',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_importi_accise_pwr',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/importi/RFCF_importi_03_iva_gas.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calcolo_tariffario',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_accise_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_iva',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_progressivo_consumo_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_forzatura_iva',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_importi_iva_gas',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/importi/RFCF_importi_04_iva_pwr.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calcolo_tariffario',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_accise_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_iva',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_importi_iva_pwr',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/importi/RFCF_importi_08_xe_accise_gas.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_xe.vw_xe_gas_tbinstallationexciseconf',
                db_name='edl_xe')
add_input_table(G, script_name=script,
                table_name='edl_int.vw_xe_tcr_tcr_forniture_lr',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_xe_fcf',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_progressivo_consumo_gas',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_importi_xe_accise_gas',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/importi/RFCF_importi_09_xe_accise_pwr.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_xe.vw_xe_dbo_tbinstallationexciseconf',
                db_name='edl_xe')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_xe_fcf',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_pwr',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_importi_xe_accise_pwr',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/importi/RFCF_importi_10_xe_iva_gas.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_xe_iva',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_xe_accise_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_progressivo_consumo_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_forzatura_iva',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_importi_xe_iva_gas',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/importi/RFCF_importi_11_xe_iva_pwr.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_xe_iva',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_xe_accise_pwr',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_importi_xe_iva_pwr',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/importi/RFCF_importi_07_accise_iva_rettifiche.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_pwr',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_importi_ret_aggr_accise_iva',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/setup/RFCF_04_info_saldi_letture.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_testatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_sottotestatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp_xe',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_pwr',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_saldi_letture',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/setup/RFCF_05_info_saldi_letture_iva_accise.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_testatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_sottotestatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp_xe',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_accise_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_accise_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_iva_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_iva_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_xe_accise_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_xe_accise_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_xe_iva_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_xe_iva_pwr',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_saldi_letture',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/calendario/RFCF_calendario_00_preparazione_calendario.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_wap.ot_wap_calendario_fatturazione_mensile_all_ver',
                db_name='edl_wap')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_ordine_default_tioce',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_calendario_agg_tioce',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/calendario/RFCF_calendario_00_preparazione_calendario_correzione.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_agg_tioce',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_calendario_agg_tioce',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/calendario/RFCF_calendario_01_join_anagrafica_calendario.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_agg_tioce',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_attributi_forniture',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_anagrafiche_tioce_escluse',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_anagrafica_join_calendario',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/calendario/RFCF_calendario_02_calendario_agg_forn_tioce.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_anagrafica_join_calendario',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_ordine_default_tioce',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_attributi_forniture',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_agg_forn_tioce_ranked',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_agg_tioce',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_calendario_agg_forn_tioce_ranked',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_calendario_agg_forn_tioce',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_calendario_agg_forn_tioce_anagrafiche_in_eccesso',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/calendario/RFCF_calendario_03_calendario_agg_forn_xe.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_xe_forn',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_xe_date',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_saldi_letture',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_calendario_agg_forn_xe',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/calendario/RFCF_calendario_04_calendario_nes_tioce.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_neta.vw_neta_fcf_fornitura',
                db_name='edl_neta')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_attributi_forniture',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_xe_forn',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_agg',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_agg_forn_tioce',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_saldi_letture',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_calendario_nes',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/calendario/RFCF_calendario_05_calendario_nes_xe.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_neta.vw_neta_fcf_fornitura',
                db_name='edl_neta')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_attributi_forniture',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_xe_forn',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_agg',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_saldi_letture',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_agg_forn_xe',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_calendario_nes',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/calendario/RFCF_Calendario_06_open_calendarizzati.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_ods.vw_fcf_misura_puntuale',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='edl_ods.vw_fcf_soggetti',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='edl_wap.ot_wap_punti_open_da_bpc_estrazione_ce_all_ver',
                db_name='edl_wap')
add_input_table(G, script_name=script,
                table_name='edl_wap.ot_wap_punti_open_da_bpc_estrazione_dr_all_ver',
                db_name='edl_wap')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tipo_regime',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_agg',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_nes',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_open_punti',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_open_volumi',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_open_consumi',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_pesi_punti_open',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/calendario/RFCF_Calendario_07_calendario_rettifica.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_ods.vw_fcf_misura_puntuale',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='edl_ods.vw_fcf_soggetti',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='edl_neta.vw_neta_fcf_fornitura',
                db_name='edl_neta')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_agg',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_attributi_forniture',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_ret_aggr_accise_iva',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_calendario_nes',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_calendario_iva_accisa',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_pesi_cluster_rettifiche',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/calendario/RFCF_calendario_08_calendario_nes_escluso.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_attributi_forniture',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='edl_neta.vw_neta_fcf_fornitura',
                db_name='edl_neta')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_nes',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_calendario_nes',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/previsione_componenti/RFCF_Previsione_Componenti_01_prescrizione_breve_att.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_testatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_sottotestatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_parametri_previsione_extra',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp_xe',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_pwr',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_previsione_pr_breve_attributes',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/previsione_componenti/RFCF_Previsione_Componenti_02_bollo_indennizzo_att.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_testatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_sottotestatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_parametri_previsione_extra',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp_xe',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_open_punti',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_previsione_bollo_attributes',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_previsione_indennizzo_attributes',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/previsione_componenti/RFCF_Previsione_Componenti_03_service_ops_att.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_testatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_sottotestatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_ods.vw_dg_l1_fattura',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_parametri_previsione_extra',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp_xe',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_previsione_bollo_attributes',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_previsione_SOPS_ASIS_attributes',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_previsione_SOPS_TOBE_attributes',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_previsione_SOPS_TOBE2_attributes',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/previsione_componenti/RFCF_Previsione_Componenti_04_componenti_no_ricavo.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_testatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_sottotestatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp_xe',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_nes',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_fatturato',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_fatturato_xe',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_open_consumi',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_previsione_comp_no_ric_attributes',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_previsione_bollo_attributes',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_previsione_SOPS_ASIS_attributes',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_previsione_SOPS_TOBE_attributes',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_previsione_SOPS_TOBE2_attributes',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_previsione_pr_breve_attributes',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_previsione_indennizzo_attributes',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_previsione_pr_breve_attributes',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_previsione_comp_no_ric',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_previsione_comp_no_ric_attributes',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/borsellino/RFCF_previsione_borsellino.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_borsellino',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_previsione_borsellino',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/canone_tv/RFCF_canone_tv_01_canone.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_ods.vw_fcf_soggetti',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='edl_ods.vw_dg_l1_fattura',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_nes',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_attributi_forniture',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_agg_forn_tioce',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_canone_tv',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/canone_tv/RFCF_canone_tv_02_canone_open.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_neta.vw_neta_fcf_fornitura',
                db_name='edl_neta')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_open_punti',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_attributi_forniture',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_agg_forn_tioce',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_canone_tv_open',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/importi/RFCF_importi_05_iva_extra.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_previsione_comp_no_ric',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_importi_iva_extra',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/importi/RFCF_importi_06_open_accise_iva.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_open_volumi',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_open_consumi',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_pwr',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_importi_open_accise_iva',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/calendario/RFCF_Calendario_09_calendario_iva_accise.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_neta.vw_neta_fcf_fornitura',
                db_name='edl_neta')
add_input_table(G, script_name=script,
                table_name='edl_ods.vw_fcf_misura_puntuale',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='edl_ods.vw_fcf_soggetti',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='edl_wap.ot_wap_punti_open_da_bpc_estrazione_ce_all_ver',
                db_name='edl_wap')
add_input_table(G, script_name=script,
                table_name='edl_wap.ot_wap_punti_open_da_bpc_estrazione_dr_all_ver',
                db_name='edl_wap')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_agg',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_ret_aggr_accise_iva',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_iva_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_iva_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_accise_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_accise_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_attributi_forniture',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_xe_forn',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_nes',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_xe_accise_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_xe_accise_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_xe_iva_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_importi_xe_iva_pwr',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_calendario_iva_accisa',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/calendario/RFCF_Calendario_10_calendario_rateo_anni_prec.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_sottotestatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_testatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_neta.vw_neta_fcf_fornitura',
                db_name='edl_neta')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_attributi_forniture',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_xe_forn',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_agg',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_fatturato',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_fatturato_xe',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_nes',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_pesi_cluster_rettifiche',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_calendario_rateo_anni_prec',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/calendario/RFCF_Calendario_11_calendario_saldi_lett_anno_prec.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_neta.vw_neta_fcf_fornitura',
                db_name='edl_neta')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_attributi_forniture',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_xe_forn',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_agg',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_saldi_letture_anno_prec',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_calendario_nes',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/calendario/RFCF_Calendario_13_calendario_iva_accise_anni_prec.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_rateo_anni_prec',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_forniture_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_attributi_forniture',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_calendario_rateo_anni_prec',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/setup/RFCF_06_info_saldi_lett_anno_prec.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_tcr.vw_tcr_fcf_ricavi_gas',
                db_name='edl_tcr')
add_input_table(G, script_name=script,
                table_name='edl_tcr.vw_tcr_fcf_ricavi_pwr',
                db_name='edl_tcr')
add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_sottotestatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_testatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_saldi_letture',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_rateo_anni_prec',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp_xe',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_ricavi_pwr',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_saldi_letture_anno_prec',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/setup/RFCF_08_info_saldi_lett_iva_acc_anno_prec.py'
all_scripts.append(script)
add_script(G, script_name=script)

add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_sottotestatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_testatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_tcr_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_mapping_voci_contabili_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_rateo_anni_prec',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_saldi_letture_anno_prec',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp_xe',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_pwr',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_saldi_letture_anno_prec',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/aggregazione_output/RFCF_aggregazione_output_01_ricavi_aggregati.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_snapshot',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_nes',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_iva_accisa',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_rateo_anni_prec',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_calendario_rettifiche_fcf',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_fatturato',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_fatturato_xe',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_attributi_forniture',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_calendario_nes_ridotto',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_cal_forn_fatt_ridotto',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/setup/RFCF_02_end_run.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_storico_run',
                 db_name='lab1_db')

#################################################
### ELIMINIAMO SCRIPT CONFIGURAZIONE INIZIALE ###
#################################################

G.remove_node('FCF/FCF/src/data/setup/RFCF_01_start_run.py')
G.remove_node('FCF/FCF/src/data/setup/RFCF_02_end_run.py')
G.remove_node('lab1_db.rfcf_storico_run')
G.remove_node('lab1_db.rfcf_configurazione_start_run')
G.remove_node('lab1_db.rfcf_run_snapshot')

all_scripts.remove('FCF/FCF/src/data/setup/RFCF_01_start_run.py')
all_scripts.remove('FCF/FCF/src/data/setup/RFCF_02_end_run.py')

edl_tables = {'edl_int': set(), 'edl_neta': set(), 'edl_ods':set(),
              'edl_tcr': set(), 'edl_wap': set(), 'edl_xe': set()}
lab1db_tables = set()
script_check = set()

for n in G.nodes():
    if n.find('.py') != -1:
        script_check.add(n)
    elif n.find('lab1_db.') != -1:
        lab1db_tables.add(n)
    else:
        for db in edl_tables.keys():
            if n.find(f"{db}.") != -1:
                edl_tables[db].add(n)

if set(script_check) != set(all_scripts):
    raise Exception('Mancano script in all_scripts')

del script_check

###############
### GRAFICO ###
###############
import numpy as np

pos = dict()
for sc,y in zip(all_scripts,np.linspace(1,-1,num=len(all_scripts))):
    pos[sc] = [0,y]

for sc,y in zip(edl_tables['edl_int'],np.linspace(1,-1,num=len(edl_tables['edl_int']))):
    pos[sc] = [-1,y]


nx.draw(G.subgraph(all_scripts+list(edl_tables['edl_int'])), pos=pos, with_labels=True, node_size=0)



###########################
### CONTROLLI SU SCRIPT ###
###########################
order_list = [[all_scripts[0]]]
checked_scripts = order_list[0].copy()

raggiungibili_al_contrario = list()

for sc in all_scripts[1:]:
    for old_sc in checked_scripts:
        to_add = list()
        if nx.has_path(G,sc,old_sc):
            raggiungibili_al_contrario.append(list(nx.shortest_path(G,sc,old_sc)))
        else:
            to_add.append(sc)
    checked_scripts = checked_scripts + to_add

only_input_tables = list()
only_output_tables = list()

for n in G.nodes():
    if n not in all_scripts:
        in_nodes_n = in_nodes(G, n)
        out_nodes_n = out_nodes(G, n)
        if len(in_nodes_n) == 0:
            only_input_tables.append(n)
        elif len(out_nodes_n) == 0:
            only_output_tables.append(n)

config_tables_lab1db = [x for x in only_input_tables if x.find('lab1_db.')!=-1]

pd.Series(config_tables_lab1db).to_excel('config_tables_lab1db.xlsx')

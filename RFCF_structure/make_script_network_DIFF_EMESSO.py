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
script = 'FCF/FCF/src/data/competenziazione/RFCF_Competenziazione_01_dett_fatt_comp.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_ods.vw_dg_l1_sottotestata',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='edl_ods.vw_dg_l1_dettaglio_fattura',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='edl_ods.vw_dg_l1_voce',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='edl_ods.vw_dg_l1_componente',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='edl_ods.vw_dg_l1_causale',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_configurazione_comp',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_dett_fatt_comp',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_decodifiche',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_dett_fatt_comp',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_configurazione_comp',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/competenziazione/RFCF_Competenziazione_02_XE.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_dettagliobollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_sottotestatabollette',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_xe.vw_xe_dbo_tbinvoices_allegato',
                db_name='edl_xe')
add_input_table(G, script_name=script,
                table_name='edl_xe.vw_xe_gas_tbinvoices_allegato',
                db_name='edl_xe')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_decodifiche',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_configurazione_comp',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_dett_fatt_comp',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_dett_fatt_comp_xe',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/setup/RFCF_07_anagrafica_forniture_emesso.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_ods.vw_dg_l1_conti_cessione_br',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='edl_ods.vw_dg_l1_conto_cliente',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='edl_ods.vw_dg_l1_anagrafica_domiciliazioni',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='edl_ods.vw_fcf_soggetti',
                db_name='edl_ods')
add_input_table(G, script_name=script,
                table_name='edl_int.vw_xe_tcr_tcr_soggetti_lr',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_neta.vw_neta_fcf_fornitura',
                db_name='edl_neta')
add_input_table(G, script_name=script,
                table_name='edl_tcr.ot_tcr_fcf_forniture_gas_on',
                db_name='edl_tcr')
add_input_table(G, script_name=script,
                table_name='edl_tcr.ot_tcr_fcf_forniture_pwr_on',
                db_name='edl_tcr')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_config_anagrafica_comp',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_attributi_forniture_comp',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/aggregazione_output/RFCF_aggregazione_output_03_fatturato_dett_fattura.py'
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
                table_name='lab1_db.rfcf_attributi_forniture_comp',
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
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_configurazione_comp',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_fatturato_emesso_dettaglio',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_fatturato_emesso_gas',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_fatturato_emesso_pwr',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_fatturato_emesso_lavori',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_forniture_fatture_emesso_gas',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_forniture_fatture_emesso_pwr',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_forniture_fatture_emesso_lavori',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/aggregazione_output/RFCF_aggregazione_output_04_fatturato_xe.py'
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
                table_name='lab1_db.rfcf_dett_fatt_comp_xe',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_mapping_xe_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_attributi_forniture_comp',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_fatturato_emesso_dettaglio',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_fatturato_emesso_gas',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_fatturato_emesso_pwr',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_fatturato_emesso_lavori',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_forniture_fatture_emesso_gas',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_forniture_fatture_emesso_pwr',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_forniture_fatture_emesso_lavori',
                 db_name='lab1_db')
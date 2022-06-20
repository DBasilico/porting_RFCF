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
script = 'FCF/cash_flow/src/data/setup/RFCF_01_start_run.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_configurazione_start_run_cashflow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run_cashflow',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_run_snapshot_cashflow',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_storico_run_cashflow',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/cash_flow/src/data/setup/RFCF_02_check_decodifiche.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_parametri_check_decodifiche',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_decodifiche_cash_flow',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_check_decodifiche',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/cash_flow/src/data/curve_incasso/RFCF_curve_incasso_01_fatturato_incassi_cons.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_neta.vw_neta_fcf_fatt_inca',
                db_name='edl_neta')
add_input_table(G, script_name=script,
                table_name='edl_xe.vw_xe_fcf_report_fatt_inca',
                db_name='edl_xe')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run_cashflow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_shanpshot_cashflow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_decodifiche_cash_flow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_neta_fcf_fatt_inca_prev',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_fatturato_incassi_consuntivo_analitico',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_fatturato_incassi_consuntivo',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/cash_flow/src/data/curve_incasso/RFCF_curve_incasso_02_curve_incassi.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run_cashflow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_shanpshot_cashflow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_fatturato_incassi_consuntivo',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_curve_incasso',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/cash_flow/src/data/cash_in/RFCF_cash_in_01_cluster_mancanti.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run_cashflow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_shanpshot_cashflow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_curve_incasso',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_fatturato_emesso_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_fatturato_emesso_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_fatturato_emesso_lavori',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_previsivo_dettaglio',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_cluster_mancanti_curve_incasso',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_cluster_mancanti_curve_incasso_analisi',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/cash_flow/src/data/cash_in/RFCF_cash_in_02_fatturato_in_scadenza.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run_cashflow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_shanpshot_cashflow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_curve_incasso',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_fatturato_incassi_consuntivo',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_fatturato_emesso_gas',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_fatturato_emesso_pwr',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_fatturato_emesso_lavori',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_previsivo_dettaglio',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_cluster_mancanti_curve_incasso',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_fatturato_in_scadenza',
                 db_name='lab1_db')


##############################################################################################################################################################################
script = 'FCF/cash_flow/src/data/cash_in/RFCF_cash_in_03_cash_in.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run_cashflow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_shanpshot_cashflow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_curve_incasso',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_fatturato_in_scadenza',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_cash_in',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/cash_flow/src/data/cash_in/RFCF_cash_in_05_applicazione_curve.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_fatturato_in_scadenza',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_curve_incasso',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_applicazione_curve_incasso',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/cash_flow/src/data/cash_out/RFCF_cash_out_01_consuntivo.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_wap.ot_wap_stanziamenti_costi_consumi_all_ver',
                db_name='edl_wap')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run_cashflow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_shanpshot_cashflow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_costi_distribuzione_consuntivi',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_decodifiche_cash_flow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_condizioni_contrattuali',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_cash_out_consuntivo',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_cash_out_stanziamenti_cons',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/cash_flow/src/data/cash_out/RFCF_cash_out_02_previsivo.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_wap.ot_wap_punti_open_da_bpc_estrazione_ce_all_ver',
                db_name='edl_wap')
add_input_table(G, script_name=script,
                table_name='edl_wap.ot_wap_punti_open_da_bpc_estrazione_dr_all_ver',
                db_name='edl_wap')
add_input_table(G, script_name=script,
                table_name='edl_wap.ot_wap_stanziamenti_costi_consumi_all_ver',
                db_name='edl_wap')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run_cashflow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_costi_materia_previsivi_fornitore',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_decodifiche_cash_flow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_condizioni_contrattuali',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_cash_out_stanziamenti_cons',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_cash_out_previsivo',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/cash_flow/src/data/setup/RFCF_03_vista_cash_flow.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run_cashflow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_vw_cash_flow_consuntivo',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_applicazione_curve_incasso',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_cash_in',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_cash_out_consuntivo',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_cash_out_previsivo',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_derivati',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_storico_cash_flow',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_vw_cash_flow',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/cash_flow/src/data/setup/RFCF_04_end_run.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run_cashflow',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_storico_run_cashflow',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/cash_flow/src/data/consuntivo/RFCF_consuntivo_incassi(cash_in).py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_neta.vw_neta_fcf_fatt_inca',
                db_name='edl_neta')
add_input_table(G, script_name=script,
                table_name='edl_xe.vw_xe_fcf_report_fatt_inca',
                db_name='edl_xe')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run_cashflow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_shanpshot_cashflow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_decodifiche_cash_flow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_neta_fcf_fatt_inca_actual',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_applicazione_curve_incasso',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_consuntivo_incassi',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_consuntivo_curve_incasso',
                 db_name='lab1_db')


##############################################################################################################################################################################
script = 'FCF/cash_flow/src/data/consuntivo/RFCF_consuntivo_costi(cash_out).py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_wap.ot_wap_stanziamenti_costi_consumi_all_ver',
                db_name='edl_wap')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run_cashflow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_run_shanpshot_cashflow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_costi_distribuzione_consuntivi',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_decodifiche_cash_flow',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_condizioni_contrattuali',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_consuntivo_costi',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/cash_flow/src/data/consuntivo/RFCF_vw_cash_flow_consuntivo.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_consuntivo_costi',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_consuntivo_incassi',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_derivati_cons',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_vw_cash_flow_consuntivo',
                 db_name='lab1_db')


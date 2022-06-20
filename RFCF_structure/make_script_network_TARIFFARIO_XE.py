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
script = 'FCF/FCF/src/data/tariffario_XE/RFCF_XE_01_tar_xe_preparazione_iniziale.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_tariffexe_table',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_int.vw_xe_tcr_tcr_forniture_lr',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_variabilixe_table',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run_cons',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_xe',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_xe_controllo_formule',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_xe_controllo_formule_bck',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_xe_bck',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_xe_semafori',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_xe_controllo_formule',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_xe_controllo_formule_bck',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/tariffario_XE/RFCF_XE_02_tar_xe_applica_tariffe.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_tariffexe_table',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_int.vw_xe_tcr_tcr_forniture_lr',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_xe.vw_xe_dbo_tbinstallationhighenergyconsumptions',
                db_name='edl_xe')
add_input_table(G, script_name=script,
                table_name='edl_xe.vw_xe_gas_tbremi',
                db_name='edl_xe')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run_cons',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_decodifiche_cons',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_codifiche_tariffario',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_xe_indici_fatturazione',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_xe_pre_variabili',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/tariffario_XE/RFCF_XE_03_tar_xe_applica_variabili.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_variabilixe_table',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run_cons',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_codifiche_tariffario',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_xe_pre_variabili',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_xe_indici_fatturazione',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_xe_pre_formule',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/tariffario_XE/RFCF_XE_04_tar_xe_applica_formule.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run_cons',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_xe_pre_formule',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_xe_indici_fatturazione',
                db_name='lab1_db')


add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_xe_semafori',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_xe_controllo_formule',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_xe_indicizzati',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_xe_controllo_errori',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_xe_indicizzati_bck',
                 db_name='lab1_db')
add_output_table(G, script_name=script,
                 table_name='lab1_db.rfcf_tariffario_xe',
                 db_name='lab1_db')

##############################################################################################################################################################################
script = 'FCF/FCF/src/data/check/RFCF_Check_02_XE.py'
all_scripts.append(script)
add_script(G, script_name=script)


add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_variabilixe_table',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_int.vw_xe_tcr_tcr_forniture_lr',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='edl_int.br_tcr_tariffexe_table',
                db_name='edl_int')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_codifiche_tariffario',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_decodifiche_cons',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_storico_run_cons',
                db_name='lab1_db')
add_input_table(G, script_name=script,
                table_name='lab1_db.rfcf_tariffario_xe',
                db_name='lab1_db')

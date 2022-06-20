for sc in all_scripts[1:]:
    ## nuovo script non raggiungibile da quelli vecchi ##
    for gr in order_list:
        for old_sc in gr:
            if nx.has_path(G,sc,old_sc) and sc!=old_sc:
                print(f"OLD script: {old_sc}")
                print(f"NEW script: {sc}")
                print(list(nx.shortest_path(G,old_sc,sc)))
                raise Exception('Nuovo script raggiungibile da script vecchio')
            else:
                gr.append(sc)



#####################
#### PLOT FINALE ####
#####################

G.remove_node('RFCF_01_start_run.py')
G.remove_node('lab1_db.rfcf_storico_run')
G.remove_node('lab1_db.rfcf_configurazione_start_run')
G.remove_node('lab1_db.rfcf_run_snapshot')

all_scripts.remove('RFCF_01_start_run.py')

ind_part = [[all_scripts[0]]]

for script in all_scripts[1:]:
    been_placed = False
    for group in ind_part:
        for group_script in group:
            if nx.has_path(G,group_script,script):
                group.append(script)
                been_placed = True
                break
        if been_placed == True:
            break
    if been_placed == False:
        ind_part.append([script])

list(nx.simple_cycles(G))

edges = list(G.edges)

script = 'RFCF_05_info_saldi_letture_iva_accise.py'
for e in edges:
    if e[1] == script:
        print(e)


nx.shortest_path(G, 'RFCF_05_info_saldi_letture_iva_accise.py', 'RFCF_calendario_04_calendario_nes_tioce.py')
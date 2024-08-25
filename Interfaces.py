import streamlit as st
import os
import time
from requests.auth import HTTPBasicAuth
import pandas as pd
import zipfile
######################## MES IMPORTS ##########################################################################################
from PLI import services
from Calendrier import schedule_options_Hebdo
from Dag_Template import generate_dag_code_PLI
from Variables import dag_directory
from Fonctions_Géneriques import get_dags,get_paused_dags,activer_dag,get_active_dags,desactiver_dag,delete_dag
from Fonctions_Géneriques import update_dag_schedule,Generate_copy_ssh_key,get_running_dags,get_dag_tasks
from Variables import username,password,My_IP,My_Port
################################################################################################################################


################################################################################################################################
#                                        Interface pour créer un dag                                                           #
################################################################################################################################

def Interface_Creation_DAG():
    st.title("Création des DAGS")
    menu_choice = st.selectbox("Type du DAG",("Dags PLI (ex: Apache, MariaDB)", "Dags Purge","Lister Les Dags","Modification d'un DAG"))
    if menu_choice == "Dags PLI (ex: Apache, MariaDB)":
        # Widgets pour sélectionner les services avec disposition en colonnes
        selected_services = []
        columns = st.columns(2)
        for i, service in enumerate(services.keys()):
            if columns[i % 2].checkbox(service):
                selected_services.append(service)
        # Champs de personnalisation pour les DAGs
        st.markdown('<div class="dag-info">Personnalisation des DAGs</div>', unsafe_allow_html=True)
        start_date = st.date_input("Date de début", value=None)
        retries = st.number_input("Nombre de tentatives", min_value=0, max_value=10, value=1)
        selected_schedule = st.selectbox("Choisissez un calendrier", list(schedule_options_Hebdo.keys()))
        Basicat = st.text_input("Basicat", value=None)
        if selected_schedule == 'Personnalisé':
                schedule_interval = st.text_input("Intervalle de planification (expression cron)", "0 0 * * *")
        else:
                schedule_interval = schedule_options_Hebdo[selected_schedule]
         # Bouton pour générer les DAGs
        if st.button('Générer DAGs', key="btn_generate"):
            total_services = len(selected_services)
            if total_services > 0:
                  if start_date is None: st.error("Veuillez sélectionner une date de début.");exit()
                  if Basicat is None: st.error("Veuillez Saisir un Basicat.");exit()
                  
                  progress_bar = st.progress(0)
                  generated_dag_files = []  # Liste pour stocker les chemins des fichiers DAGs générés
                  for i, service in enumerate(selected_services):
                        st.write(f"Génération du DAG pour {service}...")
                        actual_service_name = services[service] 
                        # Utilisation de la fonction pour générer le code du DAG
                        dag_code = generate_dag_code_PLI(service, actual_service_name, start_date, retries, schedule_interval)
                        #dag_file_path = os.path.join(dag_directory, f"{service.lower()}_service_control.py")
#W.BBB.MAR.ARR.J.DB1.                        
                        dag_file_path = os.path.join(dag_directory, f"D.{Basicat}.ARR_DEM.{service.lower()}.py")  
                        
                        with open(dag_file_path, 'w') as dag_file:
                               dag_file.write(dag_code)
                               generated_dag_files.append(dag_file_path)  # Ajouter le fichier généré à la liste
                        time.sleep(2)
                        progress = (i + 1) / total_services
                        progress_bar.progress(progress)
                  
                  st.success("DAGs générés avec succès !")
                  # Créer un fichier ZIP contenant uniquement les DAGs générés lors de cette exécution
                  zip_file_path = os.path.join(dag_directory, "dags_generated.zip")
                  with zipfile.ZipFile(zip_file_path, 'w') as zipf:
                        for dag_file in generated_dag_files:
                             zipf.write(dag_file, os.path.basename(dag_file))
            
                  # Option de téléchargement du fichier ZIP
                  with open(zip_file_path, "rb") as file:
                        st.download_button(label="Télécharger tous les DAGs",data=file,
                        file_name="dags_generated.zip",mime="application/zip")
            else: st.warning("Veuillez sélectionner au moins un service.")

################################################################################################################################
#                                        Interface pour Lister les dags                                                        #
################################################################################################################################

def Interface_Lister_DAG():
             # Logique pour la création des DAGs de purge
      st.title("Lister Mes DAGS")
    
      IP_Serveur = st.text_input("@IP du Serveur",value=My_IP)
      Port = st.text_input("Le Port",value=My_Port)
      airflow_api_url = "http://" + IP_Serveur + ":" + Port + "/api/v1/dags"
      filter_value = st.text_input("Mot-clé pour filtrer les DAGs (Optionnel)")
      if st.button('Lister Les Dags'):
           cnx = HTTPBasicAuth(username, password)
           dags = get_dags(airflow_api_url,cnx)
           if dags:
              data = []
              for dag in dags:
                 ################ Get que la valeur du Cron#########
                 schedule_interval = dag.get('schedule_interval', 'N/A')
                 if isinstance(schedule_interval, dict) and '__type' in schedule_interval:
                       cron_expression = schedule_interval.get('value', 'N/A').strip()
                 else:  cron_expression = schedule_interval
                 data.append({
                    'DAG ID': dag['dag_id'],
                    'Description': dag.get('description', 'N/A'),
                    'Schedule Interval': cron_expression,
                    'Owners': ', '.join(dag.get('owners', []))
            }) 
              df = pd.DataFrame(data)
              # Filtrer le DataFrame en fonction de la valeur saisie
              if filter_value:
                   df = df[df['DAG ID'].str.contains(filter_value, case=False, na=False)]
             # Configurer pandas pour afficher toutes les lignes avec largeur ajustée
              st.dataframe(df, width=1500, height=800)
           else: st.write("Aucun DAG trouvé.") 

################################################################################################################################
#                                        Interface pour Activer dags                                                           #
################################################################################################################################

def Interface_Activer_DAG():
     st.title("Activation des Dags")
     IP_Serveur = st.text_input("@IP du Serveur",value=My_IP)
     Port = st.text_input("Le Port",value=My_Port)
     airflow_api_url = "http://" + IP_Serveur + ":" + Port + "/api/v1/dags"
     #http://10.118.104.134:8081/api/v1/dags
     filter_value = st.text_input("Mot-clé pour filtrer les DAGs (Obligatoire)")
     cnx = HTTPBasicAuth(username, password)
     Desactivate_Dags=get_paused_dags(airflow_api_url,cnx) 
     if Desactivate_Dags:
              data = []
              for dag in Desactivate_Dags:
                   ################ Get que la valeur du Cron#########
                   schedule_interval = dag.get('schedule_interval', 'N/A')
                   if isinstance(schedule_interval, dict) and '__type' in schedule_interval:
                        cron_expression = schedule_interval.get('value', 'N/A').strip()
                   else:  cron_expression = schedule_interval
                   data.append({
                    'DAG ID': dag['dag_id'],
                    'Description': dag.get('description', 'N/A'),
                    'Schedule Interval': cron_expression,
                    'Owners': ', '.join(dag.get('owners', [])),
                    'Activer': dag.get('is_paused', False)  # Ajouter la colonne 'Activer' avec l'état des DAGs

                   })
              df = pd.DataFrame(data)              
              # Filtrer le DataFrame en fonction de la valeur saisie
              if filter_value:
                   df = df[df['DAG ID'].str.contains(filter_value, case=False, na=False)]
              checkbox_values = {}
              for _, row in df.iterrows():
                         # Afficher le DAG avec une case à cocher
                      checkbox_values[row['DAG ID']] = st.checkbox(f"Activer {row['DAG ID']}" , value=not row['Activer'])
                      #st.write("---")       
                      #st.write(f"DAG ID: {row['DAG ID']}")
                      #st.write(f"Description: {row['Description']}")
                      #st.write(f"Schedule Interval: {row['Schedule Interval']}")
                      #st.write(f"Owners: {row['Owners']}")
                      #checkbox_values[row['DAG ID']] = st.checkbox(f"Activer {row['DAG ID']}", value=not row['Activer'])
                      # Bouton pour activer les DAGs sélectionnés
              if st.button('Activer les DAGs sélectionnés'):
                 for dag_id, is_checked in checkbox_values.items():
                    if is_checked:
                        activer_dag(dag_id, airflow_api_url, cnx,is_paused=False)

                        
              # Configurer pandas pour afficher toutes les lignes avec largeur ajustée
              #st.dataframe(df, width=1500, height=800)
     else: st.write("Aucun DAG trouvé.")
     
################################################################################################################################
#                                        Interface pour Désactiver dags                                                        #
################################################################################################################################
def Interface_Desactiver_DAG():
     st.title("Désactivation des Dags")
     IP_Serveur = st.text_input("@IP du Serveur",value=My_IP)
     Port = st.text_input("Le Port",value=My_Port)
     airflow_api_url = "http://" + IP_Serveur + ":" + Port + "/api/v1/dags"
     #http://10.118.104.134:8081/api/v1/dags
     filter_value = st.text_input("Mot-clé pour filtrer les DAGs (Ici)")
     cnx = HTTPBasicAuth(username, password)
     Activate_Dags=get_active_dags(airflow_api_url,cnx) 
     total=len(Activate_Dags)
     print(f"ici{total}")
     if Activate_Dags:
              data = []
              for dag in Activate_Dags:
                   ################ Get que la valeur du Cron#########
                   schedule_interval = dag.get('schedule_interval', 'N/A')
                   if isinstance(schedule_interval, dict) and '__type' in schedule_interval:
                        cron_expression = schedule_interval.get('value', 'N/A').strip()
                   else:  cron_expression = schedule_interval
                   data.append({
                    'DAG ID': dag['dag_id'],
                    'Description': dag.get('description', 'N/A'),
                    'Schedule Interval': cron_expression,
                    'Owners': ', '.join(dag.get('owners', [])),
                    'desactiver': dag.get('is_paused', False)  # Ajouter la colonne 'Activer' avec l'état des DAGs

                   })
              df = pd.DataFrame(data)              
              # Filtrer le DataFrame en fonction de la valeur saisie
              if filter_value:
                   df = df[df['DAG ID'].str.contains(filter_value, case=False, na=False)]
              checkbox_values = {}
              for _, row in df.iterrows():
                         # Afficher le DAG avec une case à cocher
                      checkbox_values[row['DAG ID']] = st.checkbox(f"Desactiver {row['DAG ID']}" , value = row['desactiver'])
                      #st.write("---")       
                      #st.write(f"DAG ID: {row['DAG ID']}")
                      #st.write(f"Description: {row['Description']}")
                      #st.write(f"Schedule Interval: {row['Schedule Interval']}")
                      #st.write(f"Owners: {row['Owners']}")
                      #checkbox_values[row['DAG ID']] = st.checkbox(f"Activer {row['DAG ID']}", value=not row['Activer'])
                      # Bouton pour activer les DAGs sélectionnés
              if st.button('Desactiver les DAGs sélectionnés'):
                 for dag_id, is_checked in checkbox_values.items():
                    if is_checked:
                        desactiver_dag(dag_id, airflow_api_url, cnx,is_paused=True)

                        
              # Configurer pandas pour afficher toutes les lignes avec largeur ajustée
              #st.dataframe(df, width=1500, height=800)
     else: st.write("Aucun DAG trouvé.")     

################################################################################################################################
#                                        Interface pour Supprimer dags                                                         #
################################################################################################################################

def Interface_Delete_DAG():
     st.title("Suppression des Dags")
     IP_Serveur = st.text_input("@IP du Serveur",value=My_IP)
     Port = st.text_input("Le Port",value=My_Port)
     airflow_api_url = "http://" + IP_Serveur + ":" + Port + "/api/v1/dags"
     #http://10.118.104.134:8081/api/v1/dags
     filter_value = st.text_input("Mot-clé pour filtrer les DAGs (Obligatoire)")
     cnx = HTTPBasicAuth(username, password)
     Desactivate_Dags=get_paused_dags(airflow_api_url,cnx) 
     if Desactivate_Dags:
              data = []
              for dag in Desactivate_Dags:
                   ################ Get que la valeur du Cron#########
                   schedule_interval = dag.get('schedule_interval', 'N/A')
                   if isinstance(schedule_interval, dict) and '__type' in schedule_interval:
                        cron_expression = schedule_interval.get('value', 'N/A').strip()
                   else:  cron_expression = schedule_interval
                   data.append({
                    'DAG ID': dag['dag_id'],
                    'Description': dag.get('description', 'N/A'),
                    'Schedule Interval': cron_expression,
                    'Owners': ', '.join(dag.get('owners', [])),
                    'Activer': dag.get('is_paused', False)  # Ajouter la colonne 'Activer' avec l'état des DAGs

                   })
              df = pd.DataFrame(data)              
              # Filtrer le DataFrame en fonction de la valeur saisie
              if filter_value:
                   df = df[df['DAG ID'].str.contains(filter_value, case=False, na=False)]
              checkbox_values = {}
              for _, row in df.iterrows():
                         # Afficher le DAG avec une case à cocher
                      checkbox_values[row['DAG ID']] = st.checkbox(f"Delete {row['DAG ID']}" , value=not row['Activer'])
                      #st.write("---")       
                      #st.write(f"DAG ID: {row['DAG ID']}")
                      #st.write(f"Description: {row['Description']}")
                      #st.write(f"Schedule Interval: {row['Schedule Interval']}")
                      #st.write(f"Owners: {row['Owners']}")
                      #checkbox_values[row['DAG ID']] = st.checkbox(f"Activer {row['DAG ID']}", value=not row['Activer'])
                      # Bouton pour activer les DAGs sélectionnés
              if st.button('Delete les DAGs sélectionnés'):
                 for dag_id, is_checked in checkbox_values.items():
                    if is_checked:
                        delete_dag(dag_id, airflow_api_url, cnx)

                        
              # Configurer pandas pour afficher toutes les lignes avec largeur ajustée
              #st.dataframe(df, width=1500, height=800)
     else: st.write("Aucun DAG trouvé.")

################################################################################################################################
#                                        Interface pour modifier schedule                                                      #
################################################################################################################################

def Interface_Modifier_Schedule():
     st.title("Modifier Schedule du Dag")
     IP_Serveur = st.text_input("@IP du Serveur",value=My_IP)
     Port = st.text_input("Le Port",value=My_Port)
     airflow_api_url = "http://" + IP_Serveur + ":" + Port + "/api/v1/dags"
     #http://10.118.104.134:8081/api/v1/dags
     filter_value = st.text_input("Mot-clé pour filtrer les DAGs")
     cnx = HTTPBasicAuth(username, password)
     Liste_Dags=get_dags(airflow_api_url,cnx) 
     total=len(Liste_Dags)
     print(f"ici{total}")
     if Liste_Dags:
              data = []
              for dag in Liste_Dags:
                   ################ Get que la valeur du Cron#########
                   schedule_interval = dag.get('schedule_interval', 'N/A')
                   if isinstance(schedule_interval, dict) and '__type' in schedule_interval:
                        cron_expression = schedule_interval.get('value', 'N/A').strip()
                   else:  cron_expression = schedule_interval
                   data.append({
                    'DAG ID': dag['dag_id'],
                    'Description': dag.get('description', 'N/A'),
                    'Schedule Interval': cron_expression,
                    'Owners': ', '.join(dag.get('owners', [])),
                    'Update': dag.get('is_paused', False)  # Ajouter la colonne 'Activer' avec l'état des DAGs

                   })
              df = pd.DataFrame(data)              
              # Filtrer le DataFrame en fonction de la valeur saisie
              if filter_value:
                   df = df[df['DAG ID'].str.contains(filter_value, case=False, na=False)]
              checkbox_values = {}
              for _, row in df.iterrows():
                         # Afficher le DAG avec une case à cocher
                      #checkbox_values[row['DAG ID']] = st.checkbox(f"Desactiver {row['DAG ID']}" , value = row['desactiver'])
                      st.write("---")       
                      st.write(f"DAG ID: {row['DAG ID']}")
                      st.write(f"Description: {row['Description']}")
                      st.write(f"Schedule Interval: {row['Schedule Interval']}")
                      st.write(f"Owners: {row['Owners']}")
                      checkbox_values[row['DAG ID']] = st.checkbox(f"Update {row['DAG ID']}", value=not row['Update'])
                      # Bouton pour activer les DAGs sélectionnés
              if st.button('Update le Schedule sélectionné'):
                 for dag_id, is_checked in checkbox_values.items():
                    if is_checked:
                        new_schedule_interval = '0 0 * * *'
                        update_dag_schedule(dag_id, new_schedule_interval ,airflow_api_url, cnx)

                        
              # Configurer pandas pour afficher toutes les lignes avec largeur ajustée
              #st.dataframe(df, width=1500, height=800)
     else: st.write("Aucun DAG trouvé.")

################################################################################################################################
#                                        Interface pour Refresh Dag                                                            #
################################################################################################################################

def Interface_Refresh():
     st.title("Refresh Dag")
     IP_Serveur = st.text_input("@IP du Serveur",value=My_IP)
     Port = st.text_input("Le Port",value=My_Port)
     airflow_api_url = "http://" + IP_Serveur + ":" + Port + "/api/v1/dags"
     #http://10.118.104.134:8081/api/v1/dags
     filter_value = st.text_input("Remote User")
     cnx = HTTPBasicAuth(username, password)
     Liste_Dags=get_dags(airflow_api_url,cnx) 
     total=len(Liste_Dags)
     print(f"ici{total}")
     if Liste_Dags:
              data = []
              for dag in Liste_Dags:
                   ################ Get que la valeur du Cron#########
                   schedule_interval = dag.get('schedule_interval', 'N/A')
                   if isinstance(schedule_interval, dict) and '__type' in schedule_interval:
                        cron_expression = schedule_interval.get('value', 'N/A').strip()
                   else:  cron_expression = schedule_interval
                   data.append({
                    'DAG ID': dag['dag_id'],
                    'Description': dag.get('description', 'N/A'),
                    'Schedule Interval': cron_expression,
                    'Owners': ', '.join(dag.get('owners', [])),
                    'Update': dag.get('is_paused', False)  # Ajouter la colonne 'Activer' avec l'état des DAGs

                   })
              df = pd.DataFrame(data)              
              # Filtrer le DataFrame en fonction de la valeur saisie
              if filter_value:
                   df = df[df['DAG ID'].str.contains(filter_value, case=False, na=False)]
              checkbox_values = {}
              for _, row in df.iterrows():
                         # Afficher le DAG avec une case à cocher
                      #checkbox_values[row['DAG ID']] = st.checkbox(f"Desactiver {row['DAG ID']}" , value = row['desactiver'])
                      st.write("---")       
                      st.write(f"DAG ID: {row['DAG ID']}")
                      st.write(f"Description: {row['Description']}")
                      st.write(f"Schedule Interval: {row['Schedule Interval']}")
                      st.write(f"Owners: {row['Owners']}")
                      checkbox_values[row['DAG ID']] = st.checkbox(f"Update {row['DAG ID']}", value=not row['Update'])
                      # Bouton pour activer les DAGs sélectionnés
              if st.button('Rafraichir'):
                 for dag_id, is_checked in checkbox_values.items():
                    if is_checked:
                        remote_host = '10.118.104.135'
                        remote_user = 'osadmin'
                        pass_osadmin='bu1+Jz\\@zj+B6.'
                        
                        print("now")
                        Generate_copy_ssh_key(remote_user,remote_host)

                        
              # Configurer pandas pour afficher toutes les lignes avec largeur ajustée
              #st.dataframe(df, width=1500, height=800)
     else: st.write("Aucun DAG trouvé.")

################################################################################################################################
#                                        Interface pour Running DAG                                                            #
################################################################################################################################


def Interface_Lister_DAG_Running():
             # Logique pour la création des DAGs de purge
      st.title("Lister Les Dags Running")
      IP_Serveur = st.text_input("@IP du Serveur",value=My_IP)
      Port = st.text_input("Le Port",value=My_Port)
      airflow_api_url = "http://" + IP_Serveur + ":" + Port + "/api/v1/dags"
      filter_value = st.text_input("Mot-clé pour filtrer les DAGs (Optionnel)")
      if st.button('Dags Running'):
           cnx = HTTPBasicAuth(username, password)
           running_dags = get_running_dags(airflow_api_url, cnx)
           if running_dags:
              data = []
              for dag in running_dags:
                 ################ Get que la valeur du Cron#########
                 schedule_interval = dag.get('schedule_interval', 'N/A')
                 if isinstance(schedule_interval, dict) and '__type' in schedule_interval:
                       cron_expression = schedule_interval.get('value', 'N/A').strip()
                 else:  cron_expression = schedule_interval
                 data.append({
                    'DAG ID': dag['dag_id'],
                    'Description': dag.get('description', 'N/A'),
                    'Schedule Interval': cron_expression,
                    'Owners': ', '.join(dag.get('owners', []))
            }) 
              df = pd.DataFrame(data)
              # Filtrer le DataFrame en fonction de la valeur saisie
              if filter_value:
                   df = df[df['DAG ID'].str.contains(filter_value, case=False, na=False)]
             # Configurer pandas pour afficher toutes les lignes avec largeur ajustée
              st.dataframe(df, width=1500, height=800)
           else: st.write("Aucun DAG trouvé.") 
    
################################################################################################################################
#                                        Interface pour Get Taches du Dag                                                      #
################################################################################################################################    
        
def Interface_Get_Taches_Dag():
     st.title("Get tasks")
     IP_Serveur = st.text_input("@IP du Serveur",value=My_IP)
     Port = st.text_input("Le Port",value=My_Port)
     airflow_api_url = "http://" + IP_Serveur + ":" + Port + "/api/v1/dags"
     #http://10.118.104.134:8081/api/v1/dags
     filter_value = st.text_input("Mot-clé pour filtrer les DAGs (Ici)")
     cnx = HTTPBasicAuth(username, password)
     Activate_Dags=get_active_dags(airflow_api_url,cnx) 
     total=len(Activate_Dags)
     print(f"ici{total}")
     if Activate_Dags:
              data = []
              for dag in Activate_Dags:
                   ################ Get que la valeur du Cron#########
                   schedule_interval = dag.get('schedule_interval', 'N/A')
                   if isinstance(schedule_interval, dict) and '__type' in schedule_interval:
                        cron_expression = schedule_interval.get('value', 'N/A').strip()
                   else:  cron_expression = schedule_interval
                   data.append({
                    'DAG ID': dag['dag_id'],
                    'Description': dag.get('description', 'N/A'),
                    'Schedule Interval': cron_expression,
                    'Owners': ', '.join(dag.get('owners', [])),
                    'desactiver': dag.get('is_paused', False)  # Ajouter la colonne 'Activer' avec l'état des DAGs

                   })
              df = pd.DataFrame(data)              
              # Filtrer le DataFrame en fonction de la valeur saisie
              if filter_value:
                   df = df[df['DAG ID'].str.contains(filter_value, case=False, na=False)]
              checkbox_values = {}
              for _, row in df.iterrows():
                         # Afficher le DAG avec une case à cocher
                      checkbox_values[row['DAG ID']] = st.checkbox(f"Desactiver {row['DAG ID']}" , value = row['desactiver'])
                      #st.write("---")       
                      #st.write(f"DAG ID: {row['DAG ID']}")
                      #st.write(f"Description: {row['Description']}")
                      #st.write(f"Schedule Interval: {row['Schedule Interval']}")
                      #st.write(f"Owners: {row['Owners']}")
                      #checkbox_values[row['DAG ID']] = st.checkbox(f"Activer {row['DAG ID']}", value=not row['Activer'])
                      # Bouton pour activer les DAGs sélectionnés
              if st.button('Get Taches'):
                 for dag_id, is_checked in checkbox_values.items():
                    if is_checked:
                        tasks=get_dag_tasks(dag_id, airflow_api_url, cnx)
                        print("Tâches du DAG", dag_id)
                        for task in tasks:
                             print(task)

                        
              # Configurer pandas pour afficher toutes les lignes avec largeur ajustée
              #st.dataframe(df, width=1500, height=800)
     else: st.write("Aucun DAG trouvé.")   
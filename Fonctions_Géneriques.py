from requests.auth import HTTPBasicAuth
import requests
import subprocess
import os
import streamlit as st  # Si tu utilises Streamlit pour les affichages d'erreur

#############################################################################################################################
#                                 Get All DAGS                                                                              #
#############################################################################################################################
def get_dags(airflow_api_url, cnx, limit=100, offset=0):
    dags = []
    while True:
        try:
            # Ajouter les paramètres de pagination
            response = requests.get(f"{airflow_api_url}?limit={limit}&offset={offset}", auth=cnx, timeout=10)
            response.raise_for_status()  # Lève une exception pour les codes de statut HTTP 4xx/5xx
            
            data = response.json()
            dags.extend(data.get('dags', []))
            
            # Si moins de résultats que la limite, fin de la pagination
            if len(data.get('dags', [])) < limit:
                break
            
            # Incrémenter l'offset pour la prochaine page
            offset += limit
            
        except requests.exceptions.HTTPError as http_err:
            st.error(f"Erreur HTTP lors de la récupération des DAGs: {http_err}")
            break
        except requests.exceptions.ConnectionError as conn_err:
            st.error(f"Erreur de connexion: {conn_err}")
            break
        except requests.exceptions.Timeout as timeout_err:
            st.error(f"Timeout lors de la connexion: {timeout_err}")
            break
        except requests.exceptions.RequestException as req_err:
            st.error(f"Erreur lors de la requête: {req_err}")
            break
        except ValueError as json_err:
            st.error(f"Erreur de décodage JSON: {json_err}")
            break

    return dags

#############################################################################################################################
#                                 Get Paused DAGS                                                                           #
#############################################################################################################################

def get_paused_dags(airflow_api_url, cnx):
    try:
        dags = get_dags(airflow_api_url, cnx)
        total=len(dags)
        print(f"Le total des Dags est : {total}")
        
        # Filtrer les DAGs en pause
        paused_dags = [dag for dag in dags if dag.get('is_paused', False)]
        
        print(f"Nombre de DAGs en pause: {len(paused_dags)}")
        return paused_dags
    
    except Exception as e:
        st.error(f"Erreur lors de la récupération des DAGs en pause: {e}")
        return []

#############################################################################################################################
#                                 Get Active DAGS                                                                           #
#############################################################################################################################

def get_active_dags(airflow_api_url, cnx):
       try:
          dags = get_dags(airflow_api_url, cnx)
          total=len(dags)
          print(f"Le total des Dags est : {total}")
        
        # Filtrer les DAGs en pause
          active_dags = [dag for dag in dags if not dag.get('is_paused', False)]
        
          print(f"Nombre de DAGs active {len(active_dags)}")
          return active_dags
    
       except Exception as e:
        st.error(f"Erreur lors de la récupération des DAGs activé : {e}")
        return []

#############################################################################################################################
#                                 Get all Run DAGS                                                                           #
#############################################################################################################################
def get_running_dag_runs(dag_id, airflow_api_url, cnx):
    try:
        # URL pour obtenir les exécutions du DAG
        url = f"{airflow_api_url}/{dag_id}/dagRuns"
        response = requests.get(url, auth=cnx)
        if response.status_code == 200:
            dag_runs = response.json().get('dag_runs', [])
            # Filtrer les DAG runs en cours (running)
            running_dag_runs = [run for run in dag_runs if run['state'] == 'running']
            return running_dag_runs
        else:
            print(f"Erreur lors de la récupération des exécutions du DAG {dag_id}. Code de statut: {response.status_code}. Message: {response.text}")
            return []
    except requests.exceptions.RequestException as req_err:
        print(f"Erreur lors de la requête: {req_err}")
        return []


def get_all_dags(airflow_api_url, cnx):
    try:
        # URL pour obtenir tous les DAGs
        url = f"{airflow_api_url}"
        response = requests.get(url, auth=cnx)
        if response.status_code == 200:
            return response.json().get('dags', [])
        else:
            print(f"Erreur lors de la récupération des DAGs. Code de statut: {response.status_code}. Message: {response.text}")
            return []
    except requests.exceptions.RequestException as req_err:
        print(f"Erreur lors de la requête: {req_err}")
        return []

def get_running_dags(airflow_api_url, cnx):
    all_dags = get_all_dags(airflow_api_url, cnx)
    running_dags = []
    
    for dag in all_dags:
        dag_id = dag['dag_id']
        running_dag_runs = get_running_dag_runs(dag_id, airflow_api_url, cnx)
        if running_dag_runs:
            running_dags.append({
                'dag_id': dag_id,
                'running_runs': running_dag_runs
            })    
    return running_dags


#############################################################################################################################
#                                 Activer un/des DAGS                                                                       #
#############################################################################################################################

def activer_dag(dag_id, airflow_api_url, cnx,is_paused):   
    try:
        url = f"{airflow_api_url}/{dag_id}?update_mask=is_paused"
        #http://10.118.104.134:8081/api/v1/dags
        headers = {'Content-Type': 'application/json'}
        data = {'is_paused': is_paused}
        response = requests.patch(url, json=data, auth=cnx, headers=headers)
        if response.status_code == 200:
            action = "activé" if not is_paused else "désactivé"
            #print(f"DAG {dag_id} {action} avec succès.")
            st.success(f"DAG '{dag_id}' désactivé avec succès ! 🎉")
        else:
            print(f"Erreur lors de la mise à jour du DAG {dag_id}. Code de statut: {response.status_code}. Message: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"Erreur lors de la requête: {req_err}")


#############################################################################################################################
#                                 Désactiver un/des DAGS                                                                    #
#############################################################################################################################

def desactiver_dag(dag_id, airflow_api_url, cnx,is_paused):   
    try:
        url = f"{airflow_api_url}/{dag_id}?update_mask=is_paused"
        #http://10.118.104.134:8081/api/v1/dags
        headers = {'Content-Type': 'application/json'}
        data = {'is_paused': is_paused}
        response = requests.patch(url, json=data, auth=cnx, headers=headers)
        if response.status_code == 200:
            action = "desactivé" if not is_paused else "activé"
            #print(f"DAG {dag_id} {action} avec succès.")
            st.success(f"DAG '{dag_id}' désactivé avec succès ! 🎉")
        else:
            print(f"Erreur lors de la mise à jour du DAG {dag_id}. Code de statut: {response.status_code}. Message: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"Erreur lors de la requête: {req_err}")

#############################################################################################################################
#                                 Delete un/des DAGS                                                                       #
#############################################################################################################################
def delete_dag(dag_id, airflow_api_url, cnx):
    try:
        # Construire l'URL pour la suppression du DAG
        url = f"{airflow_api_url}/{dag_id}"        
        # Envoyer la requête DELETE
        response = requests.delete(url, auth=cnx)
        # Vérifier la réponse
        if response.status_code == 204:
            #print(f"DAG {dag_id} supprimé avec succès.")
            st.success(f"DAG '{dag_id}' supprimé avec succès ! 🎉")
            st.warning(f"le fichier du DAG '{dag_id}'  doit être supprimé manuellement du Répertoire ! 🎉")
        else:
            print(f"Erreur lors de la suppression du DAG {dag_id}. Code de statut: {response.status_code}. Message: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"Erreur lors de la requête: {req_err}")

################################################################################################################################
#                                       Modifier schedule                                                                      #
################################################################################################################################

def update_dag_schedule(dag_id, new_schedule_interval, airflow_api_url, cnx):
    try:
        url = f"{airflow_api_url}/{dag_id}?update_mask=schedule_interval"
        headers = {'Content-Type': 'application/json'}
        data = {'schedule_interval': new_schedule_interval}
        response = requests.patch(url, json=data, auth=cnx, headers=headers)
        if response.status_code == 200:
            st.success(f"Planning du DAG '{dag_id}' modifié avec succès ! 🎉")
            return f"Planning du DAG {dag_id} mis à jour avec succès."
        else:
            print (f"Erreur lors de la mise à jour du planning du DAG {dag_id}. Code de statut: {response.status_code}. Message: {response.text}")
            return f"Erreur lors de la mise à jour du planning du DAG {dag_id}. Code de statut: {response.status_code}. Message: {response.text}"
    except requests.exceptions.RequestException as req_err:
        print(f"Erreur lors de la requête: {req_err}")
        return f"Erreur lors de la requête: {req_err}"
        
#############################################################################################################################
#                                 Gerer clé ssh DAGS                                                                        #
#############################################################################################################################

def generate_ssh_key():
    """Génère une clé SSH si elle n'existe pas déjà."""
    key_path = os.path.expanduser('~/.ssh/id_rsa')
    
    if not os.path.exists(key_path):
        print("Aucune clé SSH trouvée. Génération d'une nouvelle clé...")
        try:
            subprocess.run(
                ['ssh-keygen', '-t', 'rsa', '-b', '2048', '-f', key_path, '-N', ''],
                check=True
            )
            print("Clé SSH générée avec succès.")
        except subprocess.CalledProcessError as e:
            print(f"Erreur lors de la génération de la clé SSH : {e}")
            return False
    else:
        print("La clé SSH existe déjà.")
    
    return True

def copy_ssh_key(remote_user, remote_host):
    """Copie la clé SSH publique sur le serveur distant."""
    try:
        subprocess.run(
            ['ssh-copy-id', f'{remote_user}@{remote_host}'],
            check=True
        )
        print(f"Clé SSH copiée sur {remote_user}@{remote_host}.")
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors de la copie de la clé SSH : {e}")

def Generate_copy_ssh_key(remote_user,remote_host):
    if generate_ssh_key():
        copy_ssh_key(remote_user, remote_host)

#############################################################################################################################
#                                 Lister les taches d'un dag                                                                #
#############################################################################################################################

def get_dag_tasks(dag_id, airflow_api_url, cnx):
    try:
        # URL pour obtenir les détails du DAG
        url = f"{airflow_api_url}/{dag_id}"
        response = requests.get(url, auth=cnx)
        if response.status_code == 200:
            dag_details = response.json()
            print("=============")
            print(dag_details)
            print("=============")
            tasks = dag_details.get('tasks', [])
            return tasks
        else:
            print(f"Erreur lors de la récupération des détails du DAG {dag_id}. Code de statut: {response.status_code}. Message: {response.text}")
            return []
    except requests.exceptions.RequestException as req_err:
        print(f"Erreur lors de la requête: {req_err}")
        return []
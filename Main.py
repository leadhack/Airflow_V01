import streamlit as st
import os
import time
import zipfile
######################## MES IMPORTS #################
#from PLI import services
#from Calendrier import schedule_options_Hebdo
#from Dag_Template import generate_dag_code_PLI
#from Variables import dag_directory
from Interfaces import Interface_Creation_DAG,Interface_Delete_DAG,Interface_Modifier_Schedule
from Interfaces import Interface_Lister_DAG,Interface_Activer_DAG,Interface_Desactiver_DAG
from Interfaces import Interface_Refresh,Interface_Lister_DAG_Running,Interface_Get_Taches_Dag
####################################################
def main():
         # CSS inline pour un test rapide
    st.markdown("""
             <style>
             .sidebar-title {
                 font-size: 24px;
                 font-weight: bold;
                 color: #4CAF50;
                 margin-bottom: 20px;
             }
             .sidebar-radio > div {
                 background-color: #f0f0f0;
                 border-radius: 10px;
                 padding: 10px;
                 margin-bottom: 10px;
             }
             .sidebar-radio label {
                 font-size: 18px;
                 color: #333;
             }
             .sidebar-radio > div:hover {
                 background-color: #e0e0e0;
             }
             </style>
             """, unsafe_allow_html=True)
         
    st.sidebar.markdown('<p class="sidebar-title">Menu</p>', unsafe_allow_html=True)
         
             ################################""
         
             #st.sidebar.title("Menu 1")
             #menu_options = ["Création des DAGS", "Lister les DAGS", "Gérer Vos Dags"]
             # Options du menu
    api_options = [
             "Lister les DAGs",
             "Créer un DAG",
             "Activer un DAG",
             "Désactiver un DAG",
             "Supprimer un DAG",
             #"Exécuter une tâche",
             #"Arrêter une tâche en cours",
             #"Recharger les DAGs",
             #"Obtenir les logs d'une tâche",
             "Obtenir les Dags Running"
            # "Obtenir les taches d'un dag"
             #"Obtenir le statut d'une tâche"
         ]
    choice = st.sidebar.radio("Aller à", api_options)
         
             # Afficher le contenu en fonction de l'option sélectionnée
    if choice == "Créer un DAG": Interface_Creation_DAG()
    elif choice == "Lister les DAGs": Interface_Lister_DAG()
    elif choice == "Activer un DAG": Interface_Activer_DAG()
    elif choice == "Désactiver un DAG":Interface_Desactiver_DAG()
    elif choice == "Supprimer un DAG":Interface_Delete_DAG() 
    elif choice == "Mettre à jour le planning d'un DAG": Interface_Modifier_Schedule()
    elif choice == "Recharger les DAGs": Interface_Refresh()
    elif choice == "Obtenir les Dags Running": Interface_Lister_DAG_Running();
    elif choice == "Obtenir les taches d'un dag": Interface_Get_Taches_Dag();
     
if __name__ == "__main__":
    main()


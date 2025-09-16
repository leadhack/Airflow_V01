import streamlit as st
import requests
from requests.auth import HTTPBasicAuth
import urllib3
import json
import openpyxl
import io
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

STATE_FILE = "Dags_updated_avant_Mep.json"

# ---------------------------
# Fonctions utilitaires
# ---------------------------
def get_all_dags(airflow_api_url, cnx, limit=100, offset=0, proxy=None):
    dags = []
    while True:
        try:
            response = requests.get(
                f"{airflow_api_url}?limit={limit}&offset={offset}",
                auth=cnx,
                timeout=10,
                verify=False,
                proxies=proxy
            )
            response.raise_for_status()
            data = response.json()
            dags.extend(data.get('dags', []))
            if len(data.get('dags', [])) < limit:
                break
            offset += limit
        except Exception as e:
            st.error(f"Erreur : {e}")
            break
    return dags

def get_dags_by_tag(airflow_api_url, cnx, tag_name, proxy=None):
    all_dags = get_all_dags(airflow_api_url, cnx, proxy=proxy)
    return [dag for dag in all_dags if tag_name in [t.get("name") for t in dag.get("tags", [])]]

def set_dag_state(airflow_api_url, cnx, dag_id, paused, proxy=None):
    try:
        url = f"{airflow_api_url}/{dag_id}"
        payload = {"is_paused": paused}
        response = requests.patch(url, json=payload, auth=cnx, verify=False, proxies=proxy)
        response.raise_for_status()
        return True
    except Exception as e:
        st.error(f"Erreur lors du changement d'état du DAG '{dag_id}': {e}")
        return False

def export_to_excel(all_dags, dags_x, tag):
    total_dags = len(all_dags)
    active = len([d for d in all_dags if not d.get("is_paused")])
    paused = total_dags - active
    tag_active = len([d for d in dags_x if not d.get("is_paused")])
    tag_paused = len(dags_x) - tag_active

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Rapport DAGs"

    ws.append(["Indicateur", "Valeur"])
    ws.append(["Nombre total de DAGs", total_dags])
    ws.append(["Nombre de DAGs activés", active])
    ws.append(["Nombre de DAGs désactivés", paused])
    ws.append([f"Nombre de DAGs avec le tag {tag} (activés)", tag_active])
    ws.append([f"Nombre de DAGs avec le tag {tag} (désactivés)", tag_paused])

    # Sauvegarde dans mémoire
    excel_bytes = io.BytesIO()
    wb.save(excel_bytes)
    excel_bytes.seek(0)
    return excel_bytes

def save_paused_dags(dags):
    paused_dags = [dag["dag_id"] for dag in dags if not dag.get("is_paused")]
    with open(STATE_FILE, "w") as f:
        json.dump(paused_dags, f, indent=2)
    return paused_dags

def load_paused_dags():
    if not os.path.exists(STATE_FILE):
        return []
    with open(STATE_FILE, "r") as f:
        return json.load(f)

# ---------------------------
# Interface Streamlit
# ---------------------------
st.title("⚙️ Gestion des DAGs Airflow")

plateformes = {
    "DEV1_AFW": "https://schedulair-dev1.sso-test.infra.ftgroup/api/v1/dags",
    "DEV2_AFW": "https://schedulair-dev2.sso-test.infra.ftgroup/api/v1/dags",
    "HP2": "https://url3/api/v1/dags",
    "PP1": "https://url3/api/v1/dags",
    "PP2": "https://url3/api/v1/dags",
}

plateforme = st.selectbox("Sélectionnez une plateforme :", list(plateformes.keys()))
tag = st.text_input("Entrez le tag à filtrer :", "")
action = st.radio("Action :", ["disable", "enable"])

if st.button("Exécuter"):
    if not tag:
        st.warning("Merci de saisir un tag.")
    else:
        url = plateformes[plateforme]
        auth = HTTPBasicAuth("Admin_api", "Admin_api")
        proxy = None

        if action == "disable":
            all_dags = get_all_dags(url, auth, proxy=proxy)
            dags_x = get_dags_by_tag(url, auth, tag_name=tag, proxy=proxy)

            st.info(f"⏸️ Désactivation des DAGs avec le tag '{tag}' sur {plateforme}...")
            paused_dags = []
            for dag in dags_x:
                if not dag.get("is_paused"):
                    set_dag_state(url, auth, dag["dag_id"], paused=True, proxy=proxy)
                    paused_dags.append(dag["dag_id"])

            st.success(f"👉 Nombre de DAGs désactivés : {len(paused_dags)}")
            save_paused_dags(dags_x)

            # Export Excel
            excel_file = export_to_excel(all_dags, dags_x, tag)
            st.download_button(
                label="📊 Télécharger rapport Excel",
                data=excel_file,
                file_name=f"fichier_avant_mep_{plateforme}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        elif action == "enable":
            st.subheader("▶️ Réactivation des DAGs")
            paused_dags = load_paused_dags()
            if not paused_dags:
                st.warning("⚠️ Aucun DAG sauvegardé trouvé. Veuillez d’abord faire disable.")
            else:
                st.info(f"{len(paused_dags)} DAGs prêts à être réactivés")

                for dag_id in paused_dags:
                    set_dag_state(url, auth, dag_id, paused=False, proxy=proxy)

                st.success(f"✅ {len(paused_dags)} DAGs réactivés avec succès")

                # Rapport Excel après MEP
                all_dags_after = get_all_dags(url, auth, proxy=proxy)
                dags_x_after = get_dags_by_tag(url, auth, tag_name=tag, proxy=proxy)
                excel_file = export_to_excel(all_dags_after, dags_x_after, tag)
                st.download_button(
                    label="📊 Télécharger rapport Excel (après MEP)",
                    data=excel_file,
                    file_name=f"fichier_apres_mep_{plateforme}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

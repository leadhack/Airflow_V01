def generate_dag_code_PLI(service, actual_service_name, start_date, retries, schedule_interval):
    return f"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {{
    'owner': 'airflow',
    'start_date': datetime({start_date.year}, {start_date.month}, {start_date.day}),
    'retries': {retries},
}}

dag = DAG('{service.lower()}_service_control', default_args=default_args, schedule_interval='{schedule_interval}')

start_task = BashOperator(
    task_id='start_{service.lower()}',
    bash_command='sudo systemctl start {actual_service_name}',
    dag=dag,
)

stop_task = BashOperator(
    task_id='stop_{service.lower()}',
    bash_command='sudo systemctl stop {actual_service_name}',
    dag=dag,
)

start_task >> stop_task
"""


########################################## Template du DAG de PURGE ##############################################
def generate_dag_Purge(purge_date, purge_interval,FS_A_PURGER):
    dag_code = f"""
# DAG de Purge
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {{
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime({purge_date.year}, {purge_date.month}, {purge_date.day}),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}

dag = DAG(
    'purge_dag',
    default_args=default_args,
    schedule_interval='{purge_interval}',
)

start = DummyOperator(task_id='start', dag=dag)

purge_logs = BashOperator(
    task_id='purge_logs',
    bash_command=(
        'dir_to_purge=$(echo "{FS_A_PURGER}" | sed "s:/*$::"); '  # Supprimer les slashes finaux
        'if [ -d "$dir_to_purge" ]; then '  # Verifier si le repertoire existe
        'Nbr_File_A_purger=$(find "$dir_to_purge"/ -type f -name "*.log" | wc -l); '
        'echo "Nombre de fichier A supprimer est : $Nbr_File_A_purger" ';
        'rm "$dir_to_purge"/*.log; '  # Supprimer les fichiers .log si le repertoire existe
        'echo "Purge effectuee" ; '  # Supprimer les fichiers .log si le repertoire existe
        'else echo "Repertoire non trouve : $dir_to_purge"; fi'  # Afficher un message si le repertoire n'existe pas
    ),
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

start >>  purge_logs >> end
"""
    return dag_code













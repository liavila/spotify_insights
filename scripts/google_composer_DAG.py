from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

def download_scripts_from_gcs(bucket_name, scripts_info):
    """
    Downloads multiple scripts from GCS to the local file system.
    `scripts_info` is a list of tuples with (object_name, download_path) for each script.
    """
    hook = GCSHook()
    for object_name, download_path in scripts_info:
        hook.download(bucket_name=bucket_name, object_name=object_name, filename=download_path)
        print(f"Downloaded {object_name} from {bucket_name} to {download_path}")


gcs_bucket_dag = 'us-west1-spotify-dds2024-096270d6-bucket'

with DAG('run_scripts_from_gcs',
         description= 'A DAG for spotify project pipeline',
         start_date=datetime(2024, 2, 1),
         schedule_interval='0 8 * * *',
         catchup=False) as dag:

    download_all_scripts = PythonOperator(
        task_id='download_all_scripts',
        python_callable=download_scripts_from_gcs,
        op_kwargs={
            'bucket_name': gcs_bucket_dag,
            'scripts_info': [
                ('scripts/main_script.sh', '/home/airflow/gcs/data/main_script.sh'),
                ('scripts/data_fetcher.py', '/home/airflow/gcs/data/data_fetcher.py'),
                ('scripts/recently_played_json_converter.py', '/home/airflow/gcs/data/recently_played_json_converter.py'),
                ('scripts/top_tracks_json_converter.py', '/home/airflow/gcs/data/top_tracks_json_converter.py'),
                ('scripts/data_upload.py', '/home/airflow/gcs/data/data_upload.py'),
                ('scripts/create_collections_recently_played.py', '/home/airflow/gcs/data/create_collections_recently_played.py'),
                ('scripts/create_collections_top_tracks.py', '/home/airflow/gcs/data/create_collections_top_tracks.py')
                # Add other scripts as needed
            ],
        },
    )

    execute_data_fetching_script = BashOperator(
        task_id='execute_data_fetching_script',
        bash_command='python /home/airflow/gcs/data/data_fetcher.py '
    )

    execute_recently_played_processing = BashOperator(
        task_id='execute_recently_played_processing',
        bash_command='python /home/airflow/gcs/data/recently_played_json_converter.py '
    )

    execute_top_tracks_processing = BashOperator(
        task_id='execute_top_tracks_processing',
        bash_command='python /home/airflow/gcs/data/top_tracks_json_converter.py '
    )

    execute_data_upload = BashOperator(
        task_id='execute_data_upload',
        bash_command='python /home/airflow/gcs/data/data_upload.py '
    )

    execute_recently_played_collection = BashOperator(
        task_id='execute_recently_played_collection',
        bash_command='python /home/airflow/gcs/data/create_collections_recently_played.py '
    )

    execute_top_tracks_collection = BashOperator(
        task_id='execute_top_tracks_collection',
        bash_command='python /home/airflow/gcs/data/create_collections_top_tracks.py '
    )


    download_all_scripts >> execute_data_fetching_script >> execute_recently_played_processing >> execute_top_tracks_processing >> execute_data_upload >> execute_recently_played_collection >> execute_top_tracks_collection


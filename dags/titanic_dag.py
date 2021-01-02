from airflow import models
import datetime as dt
from airflow.contrib.operators.gcp_container_operator import GKEPodOperator

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 11, 16),
    'concurrency': 1,
    'retries': 3,
    'depends_on_past': False,
    'retry_delay': dt.timedelta(minutes=10)
}


def get_image(step_name):
    images = {'preprocess': 'gcr.io/daria66414/preprocess_titanic@sha256:4527826396b584b603d7bcdb49db6effeef109b16e340da4fb1f82b420b56020',
              'train': 'gcr.io/daria66414/train_titanic@sha256:e3e9836ca3ad5c66e64eed9f3d4205f51b5d4ad7f1748a9de6bb9eed153d97e9',
              'predict': 'gcr.io/daria66414/predict_titanic@sha256:5f88ab44f65e1cebc20d39500d7f96e9786b73b3ea0e068614776341de65b8fe'}
    return images[step_name]


def get_ds_step_pod(step_name):
    # ToDo dag_args = Your code here
    dag_args = ['darias-titanic-data-bucket']
    return GKEPodOperator(project_id='daria66414',
                          location='europe-west1-b',
                          cluster_name='europe-west1-darias-compose-226602bd-gke',
                          namespace='default',
                          task_id=step_name + '_' + 'titanic',
                          retries=3,
                          image=get_image(step_name),
                          name=step_name.replace('_', '-') + '-' + 'titanic',
                          task_concurrency=1,
                          image_pull_policy='IfNotPresent',
                          is_delete_operator_pod=True,
                          hostnetwork=False,
                          arguments=dag_args
                          )


with models.DAG(
       # ToDo: change name to something like "[my_name]_titanic_ked_dag"
        'simple_titanic',
        default_args=default_args,
        max_active_runs=1,
        schedule_interval=None) as dag:
    prepare_data = get_ds_step_pod('preprocess')
    train = get_ds_step_pod('train')
    predict = get_ds_step_pod('predict')

    prepare_data >> train >> predict

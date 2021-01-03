from airflow import models
import datetime as dt
from airflow.contrib.operators.gcp_container_operator import GKEPodOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

#################################
## TODO : change here

# my_user should be the output of
# echo $USER
# in google cloud shell
my_user   = XXXX

# if the convention has been followed
# there is nothing to change here
my_bucket = my_user + '-ked-airflow-01-2021'

# you need to get the GKE cluster for your composer
# available at https://console.cloud.google.com/kubernetes/list?cloudshell=true&project=ked-airflow-01-2021
# it should be something like 'europe-west1-user-XXXX-gke'
my_cluster_name = 'europe-west1-' + my_user + '-' + XXXX + '-gke'

# verify the location of your GKE cluster
# if it does not match, a credential error will be raised when launching the tasks
my_location = 'europe-west1-d'
#################################

images = Variable.get('images', deserialize_json=True)

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 11, 16),
    'concurrency': 1,
    'depends_on_past': False
}


def get_image(step_name):
    return images[step_name]


def get_ds_step_pod(step_name):
    dag_args = [my_bucket]
    return GKEPodOperator(project_id='ked-airflow-01-2021',
                          startup_timeout_seconds=600,
                          location=my_location,
                          cluster_name=my_cluster_name,
                          namespace='default',
                          task_id=step_name + '_' + 'titanic',
                          image=get_image(step_name),
                          name=step_name.replace('_', '-') + '-' + 'titanic',
                          task_concurrency=1,
                          image_pull_policy='IfNotPresent',
                          is_delete_operator_pod=True,
                          hostnetwork=False,
                          arguments=dag_args
                          )


with models.DAG('simple_titanic',
        default_args=default_args,
        max_active_runs=1,
        schedule_interval=None) as dag:

    preprocess   = get_ds_step_pod('preprocess')
    train        = get_ds_step_pod('train')
    predict      = get_ds_step_pod('predict')

    backup_preprocess = BashOperator(
        task_id='backup_preprocess',
        bash_command='gsutil cp gs://' + my_bucket + '/preprocessed_* gs://' + my_bucket + '/{{ ts_nodash }}/')

    backup_train = BashOperator(
        task_id='backup_train',
        bash_command='gsutil cp gs://' + my_bucket + '/model.joblib gs://' + my_bucket + '/{{ ts_nodash }}/')

    backup_predict = BashOperator(
        task_id='backup_predict',
        bash_command='gsutil cp gs://' + my_bucket + '/predict.csv gs://' + my_bucket + '/{{ ts_nodash }}/')

    preprocess >> train >> predict
    preprocess >> backup_preprocess
    train      >> backup_train
    predict    >> backup_predict

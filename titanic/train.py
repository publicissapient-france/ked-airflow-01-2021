import sys
import pandas as pd
from google.cloud import storage
import joblib
from xgboost import XGBClassifier
from titanic.config import Config


def upload_to_gcp_bucket(bucket, source_file, destination_file):
    blob = bucket.blob(destination_file)
    blob.upload_from_filename(source_file)


def train(train_data, classifier):
    y_label = config.y_label
    train_y = train_data[y_label]
    train_X = train_data.drop([y_label, "PassengerId"], axis=1)
    classifier = classifier
    classifier.fit(train_X, train_y)
    return classifier


if __name__ == "__main__":
    config = Config()
    config.bucket_name = sys.argv[1]
    bucket_name = config.bucket_name
    storage_client = storage.Client(project=config.GCP_PROJECT)
    bucket = storage_client.get_bucket(bucket_name)

    classifier = XGBClassifier(learning_rate=0.55, n_estimators=11, max_depth=8)
    train_data = pd.read_csv('gs://' + bucket_name + '/' + config.preprocessed_train, encoding='utf-8')
    model = train(train_data, classifier)

    joblib.dump(model, config.model_file_name)
    upload_to_gcp_bucket(bucket, config.model_file_name, config.model_file_name)

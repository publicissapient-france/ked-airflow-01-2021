import sys
from joblib import load
import pandas as pd
from google.cloud import storage
from titanic.config import Config


def upload_to_gcp_bucket(bucket, source_file, destination_file):
    blob = bucket.blob(destination_file)
    blob.upload_from_filename(source_file)


def predict_and_evaluate(config, classifier, test_x):
    prediction = pd.DataFrame()
    prediction["PassengerId"] = test_x["PassengerId"]
    test_x = test_x.drop(["PassengerId"], axis=1)
    y_hat = classifier.predict(test_x)
    prediction[config.y_label] = y_hat
    prediction.to_csv(config.predict_file_name, index=False)
    return prediction


def load_model(bucket):
    model_blob = bucket.blob(config.model_file_name)
    model_blob.download_to_filename(config.model_local_file_name)
    model = load(config.model_local_file_name)
    return model


if __name__ == "__main__":
    config = Config()
    config.bucket_name = sys.argv[1]
    bucket_name = config.bucket_name
    storage_client = storage.Client(project=config.GCP_PROJECT)
    bucket = storage_client.get_bucket(bucket_name)

    model = load_model(bucket)

    test_X= pd.read_csv('gs://' + bucket_name + '/' + config.preprocessed_test, encoding='utf-8')
    prediciton = predict_and_evaluate(config, model, test_X)
    upload_to_gcp_bucket(bucket, config.predict_file_name, config.predict_file_name)





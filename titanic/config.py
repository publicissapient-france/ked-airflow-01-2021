
class Config:
    def __init__(self):
        self.GCP_PROJECT = "daria66414"
        self.bucket_name = 'darias-titanic-data-bucket'
        self.train_file_name = "train.csv"
        self.test_file_name = "test.csv"
        self.preprocessed_train = "preprocessed_train.csv"
        self.preprocessed_test = "preprocessed_test.csv"
        self.predict_file_name = "predict.csv"

        self.y_label = "Survived"
        self.report_file_name = "classification_report.json"
        self.model_file_name = "model.joblib"
        self.model_local_file_name = "model_local.joblib"
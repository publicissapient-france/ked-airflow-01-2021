import sys
from google.cloud import storage
import re
from titanic.config import Config
import numpy as np
import pandas as pd
from pandas import DataFrame


def extract_maritial(name):
    """ extract the person's title, and bin it to Mr. Miss. and Mrs.
    assuming a Miss, Lady or Countess has more change to survive than a regular married woman."""

    re_maritial = r' ([A-Za-z]+\.) '  # use regular expressions to extract the persons title
    found = re.findall(re_maritial, name)[0]
    replace = [['Dr.', 'Sir.'],
               ['Rev.', 'Sir.'],
               ['Major.', 'Officer.'],
               ['Mlle.', 'Miss.'],
               ['Col.', 'Officer.'],
               ['Master.', 'Sir.'],
               ['Jonkheer.', 'Sir.'],
               ['Sir.', 'Sir.'],
               ['Don.', 'Sir.'],
               ['Countess.', 'High.'],
               ['Capt.', 'Officer.'],
               ['Ms.', 'High.'],
               ['Mme.', 'High.'],
               ['Dona.', 'High.'],
               ['Lady.', 'High.']]

    for i in range(0, len(replace)):
        if found == replace[i][0]:
            found = replace[i][1]
            break
    return found


def father(sex, age, parch):
    if sex == 'male' and age > 16 and parch > 0:
        return 1
    else:
        return 0


def mother(sex, age, parch):
    if sex == 'female' and age > 16 and parch > 0:
        return 1
    else:
        return 0


def parent(sex, age, parch):
    if mother(sex, age, parch) == 1 or father(sex, age, parch) == 1:
        return 1
    else:
        return 0


def extract_cabin_nr(cabin):
    """ Extracts the cabin number.  If there no number found, return NaN """
    if not pd.isnull(cabin):
        cabin = cabin.split(' ')[-1]  # if several cabins on ticket, take last one
        re_numb = r'[A-Z]([0-9]+)'
        try:
            number = int(re.findall(re_numb, cabin)[0])
            return number
        except:
            return np.nan
    else:
        return np.nan


def extract_cabin_letter(cabin):
    """ Extracts the cabin letter.  If there no letter found, return NaN """
    if not pd.isnull(cabin):
        cabin = cabin.split(' ')[-1]  # if several cabins on ticket, take last one
        re_char = r'([A-Z])[0-9]+'
        try:
            character = re.findall(re_char, cabin)[0]
            return character
        except:
            return np.nan
    else:
        return np.nan


def expand_sex(sex, age):
    """ this expands male/female with kid.  Cause below 14 years old, male or female is irrelevant"""
    if age < 14:
        return 'kid'
    else:
        return sex


def missing(data):
    data.loc[(data.Age.isnull()) & (data.Title == 'Sir.'), 'Age'] = data.loc[data.Title == 'Sir.', 'Age'].median()
    data.loc[(data.Age.isnull()) & (data.Title == 'Officer.'), 'Age'] = data.loc[data.Title == 'Officer.', 'Age'].median()
    data.loc[(data.Age.isnull()) & (data.Title == 'Miss.'), 'Age'] = data.loc[data.Title == 'Miss.', 'Age'].median()
    data.loc[(data.Age.isnull()) & (data.Title == 'High.'), 'Age'] = data.loc[data.Title == 'High.', 'Age'].median()
    data.loc[(data.Age.isnull()) & (data.Title == 'Mrs.'), 'Age'] = data.loc[data.Title == 'Mrs.', 'Age'].median()
    data.loc[(data.Age.isnull()) & (data.Title == 'Mr.'), 'Age'] = data.loc[data.Title == 'Mr.', 'Age'].median()

    median_fare = data['Fare'].median()
    data['Fare'].fillna(value=median_fare, inplace=True)
    mode_embarked = data['Embarked'].mode()[0]
    data['Embarked'].fillna(value=mode_embarked, inplace=True)

    data['Cabin_char'].fillna(value=-9999, inplace=True)
    data['Cabin_nr'].fillna(value=-9999, inplace=True)
    data['Cabin_nr_odd'].fillna(value=-9999, inplace=True)

    data = data.drop(['Name', 'Cabin', 'Fare', 'Age', 'Ticket'], 1)
    return data


def preprocess_data(data: DataFrame) -> DataFrame:
    data['Title'] = list(map(extract_maritial, data['Name']))
    data['Cabin_char'] = list(map(extract_cabin_letter, data['Cabin']))
    data['Cabin_nr'] = list(map(extract_cabin_nr, data['Cabin']))
    data['Cabin_nr_odd'] = data.Cabin_nr.apply(lambda x: np.nan if x == np.nan else x % 2)
    data['Father'] = list(map(father, data.Sex, data.Age, data.Parch))
    data['Mother'] = list(map(mother, data.Sex, data.Age, data.Parch))
    data['Parent'] = list(map(parent, data.Sex, data.Age, data.Parch))
    data['has_parents_or_kids'] = data.Parch.apply(lambda x: 1 if x > 0 else 0)
    data['FamilySize'] = data.SibSp + data.Parch
    data['Sex'] = list(map(expand_sex, data['Sex'], data['Age']))
    data['FareBin'] = pd.cut(data.Fare, bins=(-1000, 0, 8.67, 16.11, 32, 350, 1000), labels=["f1", "f2", "f3",
                                                                                             "f4", "f5", "f6"])
    data['AgeBin'] = pd.cut(data.Age, bins=(0, 15, 25, 60, 90), labels=["age015",
                                                                        "age1525",
                                                                        "age2560",
                                                                        "age6090"])
    data = missing(data)
    data = pd.get_dummies(data, drop_first=True)
    return data


def upload_to_gcp_bucket(bucket, data, file_name):
    data.to_csv(file_name, index=False)
    blob = bucket.blob(file_name)
    blob.upload_from_filename(file_name)


if __name__ == "__main__":
    config = Config()
    config.bucket_name = sys.argv[1]

    bucket_name = config.bucket_name
    storage_client = storage.Client(project=config.GCP_PROJECT)
    bucket = storage_client.get_bucket(bucket_name)

    test_source = pd.read_csv('gs://' + bucket_name + '/' + config.test_file_name, encoding='utf-8')
    train_source = pd.read_csv('gs://' + bucket_name + '/' + config.train_file_name, encoding='utf-8')

    test = preprocess_data(test_source)
    train = preprocess_data(train_source)

    upload_to_gcp_bucket(bucket, test, config.preprocessed_test)
    upload_to_gcp_bucket(bucket, train, config.preprocessed_train)
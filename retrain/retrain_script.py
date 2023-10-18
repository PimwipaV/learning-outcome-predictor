import json
import multiprocessing as mp
import numpy as np
import os
import pandas as pd

from datetime import datetime
from dynamic_ongoing_classes import get_ongoing_classes_list, get_required_level
from get_classes_morethan1act import get_classes_morethan1act
from get_class_amplitude_data import get_class_amplitude_data
from get_class_amplitude_data import process_files
from prepro_amplitude_chunk import prepro_per_day
from get_class_db import get_class_db
from sklearn.model_selection import train_test_split

from concat_classdata import concat_classdata
from sum_norm import sum_norm

from sklearn.neural_network import MLPClassifier
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split

import boto3
import pickle
import shutil

def get_classes(today):

    today = datetime.today().strftime('%Y%m%d')
    
    # get a list of which obem classes are in progress today
    get_ongoing_classes_list(today)

    # select only classes with more than 1 activity
    selected_classes = get_classes_morethan1act(today)
    
    return selected_classes

def get_classdata(selected_classes):
    
    for class_id_num in selected_classes:
        print(class_id_num)
        # download the data
        get_class_amplitude_data(class_id_num, today)

        # switch into the folder with the data
        os.chdir('../efs/amp_tmp_files/extracted/211291')
        file_paths = os.listdir()

        # process all the hourly files into 1 dataframe for all the days
        dfconcat = process_files(file_paths, class_id_num)

        # get back to main location
        os.chdir('../../../../learning-outcome-predictor')
        get_required_level(today)

        # get data from leb2 database
        galmost = get_class_db(today)

        classdata = concat_classdata(galmost, dfconcat, class_id_num)

        normalized = sum_norm(classdata)
        normalized.to_csv('normalized.csv', mode='a')
        
    allclasses_normalized = pd.read_csv('normalized.csv')
    #os.remove('normalized.csv')

    old_data =pd.read_csv('data_original.csv')
    combined_old_new = pd.concat([old_data, allclasses_normalized])
    
    df = combined_old_new.drop(combined_old_new.columns[0], axis=1)
    df = df.drop(df.loc[df['class_id'] == 'class_id'].index)
    df =df.drop(['user_id','class_id'], axis =1)
    df = df.reset_index(drop=True)
    df = df.astype(float)
    
    return df


def train(df):

    dataset = df.values
    X = dataset[:,0:13]
    Y = dataset[:,13]

    np.random.seed(0)
    train_features, test_features, train_labels, test_labels = train_test_split(
        X, Y, test_size=0.2)


    #X, y = make_classification(n_samples=100, random_state=1)
    X_train, X_test, y_train, y_test = train_test_split(X, Y, stratify=Y,
                                                        random_state=1)
    clf = MLPClassifier(random_state=1, max_iter=300).fit(X_train, y_train)
    
    return clf


def save_model_to_s3(clf):
# Load your machine learning model
    #model2_2022 = MLPClassifier(random_state=1, max_iter=300).fit(X_train, y_train)
    #model2_2022 = clf
    # Serialize the model object using pickle
    model_bytes = pickle.dumps(clf)

    # Save the model to S3
    s3 = boto3.client('s3')
    model_filename = f'model{today}.pkl'
    s3.put_object(Bucket='sagemaker-studio-gm9l1avdk4m', Key=model_filename, Body=model_bytes)

    shutil.rmtree('../efs/amp_tmp_files/extracted/211291')

    
if __name__ == "__main__":
    print("start retraining process")
    today = datetime.today().strftime('%Y%m%d')
    selected_classes = get_classes(today)
    print("getting data to process..")
    df = get_classdata(selected_classes)
    clf = train(df)
    save_model_to_s3(clf)
    print("retraining done, saved new model to s3")

import boto3
import pickle
import sklearn
from sklearn import preprocessing
from datetime import datetime
from decimal import Decimal

def inference(today, incremented):

    gincremented = incremented.groupby(['user_id', 'class_id']).agg(lambda x:x.tolist())

    gincremented['activity_count_per_class'] = gincremented['activity_id'].str.len()
    avglevel = gincremented.explode('level').groupby(['user_id', 'class_id'])['level'].mean()
    avgspent_time = gincremented.explode('spent_time').groupby(['user_id', 'class_id'])['spent_time'].mean()
    avgseconds_early = gincremented.explode('seconds_early').groupby(['user_id', 'class_id'])['seconds_early'].mean()

    gincremented = gincremented.assign(avglevel= avglevel)
    gincremented = gincremented.assign(avgspent_time=avgspent_time)
    gincremented = gincremented.assign(avgseconds_early=avgseconds_early)

    gincremented['expectation_met_count'] = gincremented['expectation_met_count'].apply(lambda x: sum(x))
    gincremented['activity_page_view'] = gincremented['activity_page_view'].apply(lambda x: sum(x))
    gincremented['material_view'] = gincremented['material_view'].apply(lambda x: sum(x))
    gincremented['act+quiz_view'] = gincremented['act+quiz_view'].apply(lambda x: sum(x))
    gincremented['submit_button_clicked'] = gincremented['submit_button_clicked'].apply(lambda x: sum(x))
    gincremented['class_view_count'] = gincremented['class_view_count'].apply(lambda x: sum(x))
    gincremented['specific_activity_view'] = gincremented['specific_activity_view'].apply(lambda x: sum(x))
    gincremented['session_id_count'] = gincremented['session_id_count'].apply(lambda x: sum(x))

    gincremented['total_submission'] = incremented.explode('submission_id').groupby(['user_id', 'class_id'])['submission_id'].count()


    min_max_scaler = preprocessing.MinMaxScaler()

    gincremented[['N_session_id_count','N_class_view_count',
        'N_activity_page_view','N_material_view', 
        'N_avgspent_time', 'N_avgseconds_early', 'N_act+quiz_view','N_specific_activity_view', 'N_submit_button_clicked', 'N_activity_count_per_class', 'N_total_submission', 'N_avglevel', 'N_expectation_met_count']] = min_max_scaler.fit_transform(gincremented[['session_id_count','class_view_count',
        'activity_page_view','material_view', 
        'avgspent_time', 'avgseconds_early', 'act+quiz_view', 'specific_activity_view','submit_button_clicked', 'activity_count_per_class', 'total_submission', 'avglevel', 'expectation_met_count']])

    normalized = gincremented[['N_specific_activity_view','N_activity_count_per_class','N_total_submission',
           'N_session_id_count', 'N_class_view_count', 'N_activity_page_view',
           'N_material_view', 'N_avgspent_time', 'N_avgseconds_early',
           'N_act+quiz_view', 'N_submit_button_clicked', 'N_expectation_met_count', 'N_avglevel']]
                    #'maxlevel']]

    dataset = normalized.values
    X = dataset[:,0:13]
    Y = dataset[:,12]

    #Y = [float(x) for x in Y]

    #pickled_model = pickle.load(open('/home/ec2-user/SageMaker/learning-outcome-predictor/model20230327.pkl', 'rb'))
    pickled_model = pickle.load(open('model20230327.pkl', 'rb'))


    gincremented['predicted_label'] = pickled_model.predict(X)
    predict_proba = pickled_model.predict_proba(X)
    gincremented['predicted_prob'] = [row.tolist() for row in predict_proba] #this is it

    #gincremented['predicted_dt'] = datetime.today()
    gincremented['predicted_dt'] = today

    gincremented = gincremented.reset_index()
    
    return gincremented
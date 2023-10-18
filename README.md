# learning-outcome-predictor

This repository is a python package that provides a machine learning service when set up to run the 3 main scripts. Right now they are run in SageMaker notebook instances at https://ap-southeast-1.console.aws.amazon.com/sagemaker/home?region=ap-southeast-1#/notebook-instances

The service aims to provide a prediction for a learning outcome (level) that a student would get for a class_id (main module). 

The service expects to assist teachers overseeing the class to be aware if any student is prone to fail to meet the expected level of learning outcome, by providing the learning outcome prediction on a daily basis.

The predictions take into account the behaviors and interactions the student has with LEB2 platform during their learning activities(modules).

The data comes from 2 sources;
1. LEB2 database such as no.of session_id, submission_id, spent_time, submitted_at, and level.
2. Amplitude, which is an analytic platform collecting user behaviors, such as activity_page_view, class_view_count, activity+quiz view
There are 13 features in total.

The 3 scripts are set up as follows;

1. prep_job.py 
This script will extract the data from both sources, preprocess and combine it, save the preprocessed data to a Dynamodb table 'daily', then do sum and normalize before inputting in a model for inference. The predictions will be written to another Dynamodb table 'predictions'.

This script will be run daily. It is located on branch dynamic-ongoing-classes

2. retrain_script.py
This script will extract the data from both sources, preprocess and combine it, then together with the newly available data label (learning outcome of the main module) and existing data, retrain a new model. The new model will be save in an S3 bucket.

This script will be run once per semester. It is located on branch master.

3. dl_and_replace.py
This script will download a new model from S3 bucket once it becomes available, and change the filename inside inference.py, so that new model can be available for use by prep_job.py

This script will be run when a new object is put on S3. It is located on branch dynamic-ongoing-classes

To complete the service, the predictions from 1. will then be consumed by LEB2 backend via an API Gateway that is connected to lambda function url that queries predictions from the DynamoDB table.

Zoom out: How are the scripts getting to work

At the moment, each of them is being run inside an instance, at instance start-up, managed by a lifecycle configuration. Each instance is scheduled to be started by lambda function and triggered by an event as follows;

1. script name prep_job.py
instance name run-prep-job-py
lifecycle configuration run-prep-job-py-on-startup
lambda function launch-run-prep-job-py
trigger eventbridge daily
lambda stop-prep-job-py

2. script name retrain_script.py
instance name retrain-with-efs
lifecycle configuration run-retrain-script-on-startup
lambda function launch-retrain-with-efs
trigger eventbridge once per semester

3. script name dl_and_replace.py
instance name dl-new-model-and-replace
lifecycle configuration run-dl-and-replace-on-startup
lambda function launch-dl-and-replace
trigger s3 bucket notification

Consume predictions
lambda function get-predictions-from-dynamodb
this lambda is a bit bigger then other lambda, because it's serverless. it's small enough and quick enough, so no need to be run in an instance. It queries predicted_dt, class_id, user_id, predicted_label, and predicted_prob from dynamodb table predictions
trigger when being curled, get queryStringParameters in a request via API Gateway event

API Gateway
url https://6klft3yi1f.execute-api.ap-southeast-1.amazonaws.com/test-get-predictions

queryStringParameters
1. predicted_dt (date in str format) e.g. "20230325", 
2. class_id(str) e.g. "173177"

example url request
https://6klft3yi1f.execute-api.ap-southeast-1.amazonaws.com/test-get-predictions?predicted_dt="20230325"&class_id="173177"

authentication with API key in the request header


Zoom in: Drill down inside each script

1. prep_job.py
    getting date today in datetime format
    calculate into yesterday
    convert yesterday into str format
    input yesterday into amp_export.sh to get yesterday's data from amplitude
    process the data in box2r, get daily minidf->joined
    get ongoing classes and their required level
    get data from LEB2 database in box3, only of the ongoing classes(almost)-->dbslice
    concat joined and dbslice to get daily_increment
    query dynamodb, get upto_yesterday preprocessed data that was saved in dynamodb
    concat upto_yesterday with daily_increment --> incremented
    write incremented to dynamodb, to be queried tomorrow
    sum incremented into gincremented and normalize it
    input normalized to the model for inference(to get predictions)
    save predictions to dynamodb

    
2. retrain_script.py
    get date today in string format
    get classes with more than 1 activity to use for training
    get the selected classdata from amplitude(data from the whole period of class)
    chunk the data and process 10 hourly files each for all the files, concat all of them
    get data from LEB2 database
    concat data from LEB@ and amplitude
    normalize and combine it with existing data
    train
    save new model to s3
    
3. dl_and_replace.py
    get date today in string format
    download a file from s3 with f'model{today}.pkl'
    replace the model filename in inference.py to be f'model{today}.pkl'
    git push back to the repo so that prep_job.py, who's calling inference.py has access to the newly trained model

Zoom in: Drill down inside each script

1. prep_job.py
getting date today in datetime format
calculate into yesterday
convert yesterday into str format
input yesterday into amp_export.sh to get yesterday's data from amplitude
process the data in box2r, get daily minidf->joined
get ongoing classes and their required level
get data from LEB2 database in box3, only of the ongoing classes(almost)-->dbslice
concat joined and dbslice to get daily_increment
query dynamodb, get upto_yesterday preprocessed data that was saved in dynamodb
concat upto_yesterday with daily_increment --> incremented
write incremented to dynamodb, to be queried tomorrow
sum incremented into gincremented and normalize it
input normalized to the model for inference(to get predictions)
save predictions to dynamodb

2. retrain_script.py
get date today in string format
get classes with more than 1 activity to use for training
get the selected classdata from amplitude(data from the whole period of class)
chunk the data and process 10 hourly files each for all the files, concat all of them
get data from LEB2 database
concat data from LEB@ and amplitude
normalize and combine it with existing data
train
save new model to s3
3. dl_and_replace.py
get date today in string format
download a file from s3 with f'model{today}.pkl'
replace the model filename in inference.py to be f'model{today}.pkl'
git push back to the repo so that prep_job.py, who's calling inference.py has access to the newly trained model


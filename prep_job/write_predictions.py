import boto3
import os
from datetime import datetime
from decimal import Decimal

def dictionary_iterator(input):
    result = {}
#     prec = 16
    prec = 28
    for key, value in input.items():
        if isinstance(value, dict):
            result[key] = dictionary_iterator(value)
        elif isinstance(value, list):
            result[key] = [Decimal(str(round(item, prec))) for item in value]
        elif isinstance(value, float):
            result[key] = Decimal(str(round(value, prec)))
        else:
            result[key] = value
    return result
    
def write_predictions(today, gincremented):
    
    timestampprediction = [{'date_today': int(today),
    #timestampprediction = [{'date_today': int(datetime.today().strftime("%Y%m%d")), 
                       #'updated_at': datetime.strptime(str(today), '%Y%m%d').timestamp(), 
                            'updated_at': datetime.now().timestamp(),
                       'class_id':x['class_id'],'user_id':x['user_id'], 
                            'predicted_label': x['predicted_label'],
                            'predicted_prob': x['predicted_prob'],
                       #'predicted_dt': datetime.today().strftime("%Y%m%d, %H:%M:%S),           
                    'predicted_dt': today, 'interactions':x[['avgspent_time', 'avglevel','expectation_met_count', 'avgseconds_early', 'activity_count_per_class','activity_page_view','material_view', 'act+quiz_view','submit_button_clicked','class_view_count','specific_activity_view', 'total_submission','session_id_count']].to_dict()}for _,x in gincremented.iterrows()]
    
    #delete today's data if it already exists:
    dynamo_client = boto3.resource(service_name = 'dynamodb',region_name = 'ap-southeast-1',
    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'])

    predictions = dynamo_client.Table('predictions')
    
    # before writing, check and delete today's data if it already exists
    partition_key_value = int(today)
    response = predictions.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('date_today').eq(partition_key_value)
        )
    for item in response['Items']:
        predictions.delete_item(Key={'date_today': item['date_today'], 'updated_at': item['updated_at']})
    
    #now write
    with predictions.batch_writer() as batch:
        for item in timestampprediction:

            result = dictionary_iterator(item)

            response = batch.put_item(result)
            
    return response
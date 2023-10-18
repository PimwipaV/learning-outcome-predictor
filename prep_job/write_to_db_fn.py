import boto3
import os
from datetime import datetime, timedelta
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

#incremented = aggregation(today)

def write_to_db(today, incremented):
    
    dynamo_client = boto3.resource(service_name = 'dynamodb',region_name = 'ap-southeast-1',
        aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'])

    daily = dynamo_client.Table('daily')


    timestamptoday = [{#'date_today': int(datetime.today().strftime("%Y%m%d")), 
        #'date_today': int((datetime.today()-timedelta(days=1)).strftime('%Y%m%d')),
        'date_today': today,
                       'updated_at': datetime.now().timestamp(), 
                       'class_id':x['class_id'],'user_id':x['user_id'], 
                       'last_updated_dt': x['last_updated_dt'],
                       'interactions':x[['spent_time', 'level','expectation_met_count', 
                                         'seconds_early', 'activity_page_view',
                                         'material_view', 'act+quiz_view', 
                                         'submit_button_clicked','class_view_count', 
                                         'specific_activity_view', 
                                         'session_id_count']].to_dict()}for _,x in incremented.iterrows()]

#if partition key of today already exist, delete them first before writing the new one
    #partition_key_value = 20230203
    #partition_key_value = int(datetime.today().strftime("%Y%m%d"))
    #partition_key_value = int((datetime.today()-timedelta(days=1)).strftime('%Y%m%d'))
    partition_key_value = today

    #def delete_partition_key(partition_key_value):
    response = daily.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('date_today').eq(partition_key_value)
        )
    for item in response['Items']:
        daily.delete_item(Key={'date_today': item['date_today'], 'updated_at': item['updated_at']})


    with daily.batch_writer() as batch:
        for row in timestamptoday:
            
            result = dictionary_iterator(row)
            response = batch.put_item(result)

        print(response)
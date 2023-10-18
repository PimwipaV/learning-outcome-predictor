import boto3
import pandas as pd
from datetime import datetime, timedelta

def query_upto_yesterday(today):
# Create a DynamoDB client
    client = boto3.client('dynamodb')
    
    #yesterday = datetime.strptime(today.strftime('%Y%m%d'), '%Y%m%d')-timedelta(days=1)

    yesterday = datetime.strptime(today,'%Y%m%d')-timedelta(days=1)
    yesterday = yesterday.strftime('%Y%m%d')       

    #yesterday = datetime.strptime(today) - timedelta(days=1)
    # Specify the table name and attribute values
    table_name = 'daily'
    key_value = yesterday
    print("yesterday", yesterday)
    # Use an expression attribute name to replace the reserved word "key"
        
    response = client.query(
        TableName=table_name,
        KeyConditionExpression='#date = :date_today_val',
        ExpressionAttributeNames={
            '#date': 'date_today'
        },
        ExpressionAttributeValues={
            ':date_today_val': {'N': key_value}
        }
    )

    items = response['Items']

    #pip install dynamodb-json
    from dynamodb_json import json_util as json
    
    upto_yesterday = pd.DataFrame(json.loads(items))
    df2 = pd.json_normalize(upto_yesterday['interactions'])
    df2 = df2[['spent_time',
               'expectation_met_count', 'seconds_early',
               'activity_page_view', 'material_view', 'act+quiz_view',
               'submit_button_clicked', 'class_view_count', 'specific_activity_view',
               'session_id_count','level']]
    df2.columns = ['spent_time',
               'expectation_met_count', 'seconds_early',
               'activity_page_view', 'material_view', 'act+quiz_view',
               'submit_button_clicked', 'class_view_count', 'specific_activity_view',
               'session_id_count','level']
    
    upto_yesterday = upto_yesterday[['user_id', 'class_id', 'last_updated_dt']].join(df2)

    #yesterday = datetime.now() - timedelta(days = 1)
    #yesterday = yesterday.strftime('%Y-%m-%d')
    #upto_yesterday_file = f'upto_yesterday_{yesterday}.csv'
    #df.to_csv(upto_yesterday_file, index=False)
    return upto_yesterday
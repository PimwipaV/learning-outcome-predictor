
import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime,timedelta


def lambda_handler(event, context):

    today = datetime.today()
    yesterday = today - timedelta(days=1)
    yesterday = yesterday.strftime('%Y%m%d')
    
    client = boto3.client('dynamodb')
    table_name = 'predictions'

    response = client.query(
                    TableName=table_name,
                    KeyConditionExpression='#date = :date_today_val',
                    ExpressionAttributeNames={
                        '#date': 'date_today'
                    },
                    ExpressionAttributeValues={
                        ':date_today_val': {'N': yesterday}
                    }
                )
    predictions = response['Items']

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json'
        },
         'body': json.dumps(predictions)
       #  'body': json.dumps(transactionResponse)
    }
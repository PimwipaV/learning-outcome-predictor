import boto3
import pandas as pd
from datetime import datetime, timedelta
from prep_script import get_date, run_bash_script
import shutil
import os

def query_upto_yesterday(today):
# Create a DynamoDB client
    client = boto3.client('dynamodb')
    table_name = 'daily'
    #key_value = '20230202' date of yesterday
    #yesterday = (datetime.today()-timedelta(days=1)).strftime('%Y%m%d')
    #this was correct until have to pass around today yesterday = (today -timedelta(days=1)).strftime('%Y%m%d')
    print(today)
    #yesterday = datetime.strptime(today.strftime('%Y%m%d'), '%Y%m%d')-timedelta(days=1)
    yesterday = datetime.strptime(today, ('%Y%m%d')) - timedelta(days=1)

    yesterday = yesterday.strftime('%Y%m%d')       
    #key_value = (datetime.today()-timedelta(days=8)).strftime('%Y%m%d')

        # Use an expression attribute name to replace the reserved word "key"
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

    items = response['Items']

    if items == []:
        print('there is no yesterdays data')
        
        # so we more on to query dynamo for records of 2 days ago   
        #if no value passed key_value = (datetime.today()-timedelta(days=2)).strftime('%Y%m%d')
        today = datetime.strptime(today, ('%Y%m%d'))
        print("today for query", today)

        key_value = (today-timedelta(days=2)).strftime('%Y%m%d')


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

        items2 = response['Items']

        if items2:
            
            #ต้อง delete amplitude data ที่ไปดาวโหลดมาใน amp_tmp_files/extracted/211291
            #if there is records from 2 days ago, then we can start reparation process
            #assuming we are back to yesterday to rerun the prep script
            #if no value passed yesterday = datetime.today()- timedelta(days=2)
            shutil.rmtree('../amp_tmp_files/extracted/211291')
            yesterday = today-timedelta(days=2)
            #first download the data of 2 days ago
            start, end, output_path = get_date(yesterday)
            print (start, end, output_path)            

            run_bash_script("./amp_export.sh", start, end, output_path)    

            today = yesterday + timedelta(days=1)
            print("today in the block at download", today)
            
            
            from query_upto_yesterday import query_upto_yesterday
            from get_daily_increment_dynamic import get_daily_increment
            from box2r_prepro_amplitude import box2
            from box3_prepro_db import box3_prepro_db
            from dynamic_ongoing_classes import get_ongoing_classes_list, get_required_level
            
            today = today.strftime('%Y%m%d')

            get_ongoing_classes_list(today)
            get_required_level(today)
            almost = box3_prepro_db(today)
            minidf = box2()

            upto_yesterday = query_upto_yesterday(today)
            daily_increment = get_daily_increment(today, minidf, almost)

            #from content_to_write2_param import aggregation
            from agg_and_write import aggregation
            print("today in the block at agg_and_write", today)
            #print(upto_yesterday)
            #ติดอยู่ตรงนี้  [last_updated_dt] มีอะไรสักอย่างผิดปกติ ใน agg_and_write มันเป็น int(today) แต่ตอนนี้เป็นอะไรนะ?? upto_yesterday = upto_yesterday.groupby(['user_id', 'class_id'], group_keys=False).apply(lambda g:g[(g['last_updated_dt']) == g['last_updated_dt'].max()])
            incremented = aggregation(today, upto_yesterday, daily_increment)

            from write_to_db_fn import write_to_db
            today = int(today)
            write_to_db(today, incremented)
            today = str(today)
            #today = datetime.strptime(today, '%Y%m%d') need str for query
            #ตรงนี้ ถ้าออกจาก if clause ค่าของ today จะเป็นอะไร? ค่าเดิมปะ คือ today =เมื่อวาน
            print('write_to_db แล้ว')  
            
        else:
            raise Exception('no data for 2 days. alarm!')
        #yesterday = (today -timedelta(days=1)).strftime('%Y%m%d') so don't do this line
        print("fixed and now move to today", today) #actually it's still yesterday in the if clause
        
        response = client.query(
                TableName=table_name,
                KeyConditionExpression='#date = :date_today_val',
                ExpressionAttributeNames={
                    '#date': 'date_today'
                },
                ExpressionAttributeValues={
                    ':date_today_val': {'N': today}
                }
            )

        items = response['Items']
        #because it's still in the if clause, get yesterday data is actually today's
        today = datetime.strptime(today, '%Y%m%d')
        gobackaday = today-timedelta(days=1)
        gobackaday = gobackaday.strftime('%Y%m%d')
        today = today.strftime('%Y%m%d')
        start = gobackaday+"T17"
        end = today+"T16"
        output_path = today+".json"

        #start, end, output_path = get_date(yesterday)
        print("so this is data from", today)
        print ("and d/l data", start, end, output_path)            

        run_bash_script("./amp_export.sh", start, end, output_path)    
        #here we are ready to get out of the if clause
        
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
    print(upto_yesterday.index)
    #yesterday = datetime.now() - timedelta(days = 1)
    #yesterday = yesterday.strftime('%Y-%m-%d') #error_handling จะcomment บรรทัดนี้ออก
    
    #correct upto_yesterday_file = f'upto_yesterday_{yesterday}.csv'
    #correct but don't need anymore df.to_csv(upto_yesterday_file, index=False)
    print("upto_yesterday length", len(upto_yesterday))
    return upto_yesterday
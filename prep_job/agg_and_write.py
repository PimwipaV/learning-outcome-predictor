import os
import pandas as pd
from datetime import datetime, timedelta
from get_daily_increment import get_daily_increment
from query_upto_yesterday_handling import query_upto_yesterday

def aggregation(today, upto_yesterday, daily_increment):

#upto_yesterday.drop_duplicates(inplace = True)
    upto_yesterday.reset_index(drop=True)
    upto_yesterday['user_id'].astype(float)
    upto_yesterday['class_id'].astype(float)

    upto_yesterday['user_id'] = upto_yesterday['user_id'].astype(int)
    upto_yesterday['class_id'] = upto_yesterday['class_id'].astype(int)
    upto_yesterday['user_id'] = upto_yesterday['user_id'].astype(str)
    upto_yesterday['class_id'] = upto_yesterday['class_id'].astype(str)
    #upto_yesterday = upto_yesterday.set_index(['user_id', 'class_id'])
    upto_yesterday = upto_yesterday.groupby(['user_id', 'class_id'], group_keys=False).apply(lambda g:g[(g['last_updated_dt']) == g['last_updated_dt'].max()])

    upto_yesterday = upto_yesterday.reset_index(drop=True)
    upto_yesterday = upto_yesterday.set_index(['user_id', 'class_id'])

    #today เดิมมาเป็น str คือ strftime('%Y%m%d')
    #upto_yesterday มา 964 แล้วพอ groupby เหลือ 867
    #daily_increment ท่929
    daily_increment['last_updated_dt'] = int(today) #<---ต้องเปลี่ยน today ตรงนี้เป็น int ถึงจะเอามาลบกันได้ที่ max

    incremented = pd.concat([upto_yesterday, daily_increment])
    #ตรงนี้ 1893
    incremented = incremented.reset_index()

    incremented = incremented.groupby(['user_id', 'class_id'], group_keys=False).apply(lambda g:g[(g['last_updated_dt']) == g['last_updated_dt'].max()])
    #ตรงนี้ 938

    incremented = incremented.reset_index(drop=True)
    incremented = incremented.fillna(0)
    incremented = incremented[['user_id', 'class_id', 'last_updated_dt', 'spent_time',
           'expectation_met_count', 'seconds_early', 'activity_page_view',
           'material_view', 'act+quiz_view', 'submit_button_clicked',
           'class_view_count', 'specific_activity_view', 'session_id_count',
           'level', 'activity_id', 'submission_id']]

    incremented['seconds_early'] = incremented['seconds_early'].astype(float)#has been float
    incremented['activity_id'] = incremented['activity_id'].astype(float)
    incremented['spent_time'] = incremented['spent_time'].astype(float)
    incremented['expectation_met_count'] = incremented['expectation_met_count'].astype(float)
    incremented['activity_page_view'] = incremented['activity_page_view'].astype(float)
    incremented['material_view'] = incremented['material_view'].astype(float)
    incremented['act+quiz_view'] = incremented['act+quiz_view'].astype(float)
    incremented['submit_button_clicked'] = incremented['submit_button_clicked'].astype(float)
    incremented['class_view_count'] = incremented['class_view_count'].astype(float)
    incremented['specific_activity_view'] = incremented['specific_activity_view'].astype(float)
    incremented['session_id_count'] = incremented['session_id_count'].astype(float)
    incremented['level'] = incremented['level'].astype(float)
    incremented['activity_id'] = incremented['activity_id'].astype(float)
    incremented['submission_id'] = incremented['submission_id'].astype(float)
    
    return incremented
#from write_to_db_fn import write_to_db
#today = int(today.strftime('%Y%m%d'))
#today = int(today)
#write_to_db(today, incremented) 
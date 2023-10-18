import glob
import json
import os
import pandas as pd
import numpy as np
from datetime import datetime
from dynamic_ongoing_classes import get_ongoing_classes_list, get_required_level

#not receiving value today = datetime.today().strftime('%Y%m%d')
#not receiving value get_ongoing_classes_list(today)
#not receiving value get_required_level(today)

def box3_prepro_db(today):

    #today = datetime.today().strftime('%Y%m%d')

    ongoing_activities = pd.read_csv(f'ongoing_obem_classes_{today}.csv')
    #correct original ongoing_activities = pd.read_csv('ongoing_classes1_2022_with_level.csv')

    #ongoing_activities['cri_id'].unique() -->เอานี่ไป query required level มาจาก db อีกที
    #correct original required_level = pd.read_csv('required_level_1_2022.csv')
    required_level = pd.read_csv(f'required_level_{today}.csv')

    required_level_ka = required_level.drop(['cri_levels_id', 'flag', 'activity_id'], axis=1)
    required_level_ka = required_level_ka.rename(columns={'cri_id':'cri_idB','level':'required_level'})

    ongoing_activities = ongoing_activities.merge(required_level_ka, how='left', left_on='cri_id', right_on='cri_idB')

    ongoing_activities['pass'] = np.where(ongoing_activities['level'] >= ongoing_activities['required_level'], True, False)

    ongoing_activities['user_id'] = ongoing_activities['user_id'].map(str)
    ongoing_activities['class_id'] = ongoing_activities['class_id'].map(str)

    #remove test user 18912 who has true duplicates (multiple submission in same class_id)
    #ongoing_activities.drop((ongoing_activities.loc[ongoing_activities['user_id'] == '18912']['class_id'] == '177701').index, inplace=True)

    # sort by due_date for consistency with data from amplitude
    ongoing_activities.sort_values(by=['user_id','class_id', 'due_date']).groupby(['user_id', 'class_id']).agg(lambda x:x.tolist())
    # select all activity sorted by due_date
    almost = ongoing_activities.sort_values(by=['user_id','class_id', 'due_date'])
    almost['expectation_met_count'] = almost['pass'].astype(int)

    # add feature seconds_early, and clean spent_time off None
    almost['due_date'] = pd.to_datetime(almost['due_date'])
    almost['submitted_at'] = pd.to_datetime(almost['submitted_at'])
    almost['earlybird'] = almost['due_date'] - almost['submitted_at']
    earlybird = almost['earlybird']
    seconds_early = [i.total_seconds() for i in earlybird]
    almost['seconds_early'] = seconds_early
    #fill Nan in seconds_early with 0
    almost['seconds_early'] = almost['seconds_early'].fillna(0)

    almost['spent_time'] = almost['spent_time'].fillna(0)
    almost['spent_time'] = almost['spent_time'].map(lambda x: pd.to_timedelta(x).seconds)

    #set_index ready to be concat
    almost['user_id'] = almost['user_id'].map(str)
    almost['class_id'] = almost['class_id'].map(str)

    almost = almost.set_index(['user_id', 'class_id'])
    print("almost index length", len(almost.index))
    
    return almost
#print(ongoing_activities.loc[ongoing_activities['user_id'] == '18912'])
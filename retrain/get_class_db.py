from datetime import datetime
import pandas as pd
import numpy as np
# import dataframe itcomesdownto มาด้วยที่นี่ หรือจะใส่เป็น argument?
#from get_classes_morethan1act import ค่า itcomesdownto หรือจะมาทำเองตรงนี้
#today = datetime.today().strftime('%Y%m%d')

#from dynamic_ongoing_classes import get_ongoing_classes_list, get_required_level
#get_required_level(today)

def get_class_db(today):
    
    #get_ongoing_classes_list(today)
    ongoing_obem_classes_list = pd.read_csv(f'ongoing_obem_classes_{today}.csv')

    #keep only classes with more than 1 activity
    activity_id_per_class = ongoing_obem_classes_list.reset_index().groupby('class_id')['activity_id'].nunique()
    activity_id_per_class == 1
    keep = activity_id_per_class != 1

    classes_morethan1_act = np.unique(ongoing_obem_classes_list['class_id'])[keep].tolist()
    
    itcomesdownto = ongoing_obem_classes_list.loc[ongoing_obem_classes_list['class_id'].isin(classes_morethan1_act)]
#required_level = pd.read_csv('required_level_flag_by_cri_id.csv')
    required_level = pd.read_csv(f'required_level_{today}.csv')
    required_level_ka = required_level.drop(['cri_levels_id', 'flag', 'activity_id'], axis=1)
    required_level_ka = required_level_ka.rename(columns={'cri_id':'cri_idB','level':'required_level'})

    # merge required_level into orginal df and compare if a student passes in that cri_id
    itcomesdowntolevel = itcomesdownto.merge(required_level_ka, how='left', left_on='cri_id', right_on='cri_idB')
    itcomesdowntolevel['pass'] = np.where(itcomesdowntolevel['level'] >= itcomesdowntolevel['required_level'], True, False)

    # convert value for index to string format as required by concat
    itcomesdowntolevel['user_id'] = itcomesdowntolevel['user_id'].map(str)
    itcomesdowntolevel['class_id'] = itcomesdowntolevel['class_id'].map(str)

    # sort by due_date for consistency with data from amplitude
    itcomesdowntolevel.sort_values(by=['user_id','class_id', 'due_date']).groupby(['user_id', 'class_id']).agg(lambda x:x.tolist())

    # select only the first activity sorted by due_date
    almost = itcomesdowntolevel.sort_values(by=['user_id','class_id', 'due_date']).groupby(['user_id', 'class_id']).first()
    almost['expectation_met_count'] = almost['pass'].astype(int)

    # add feature submission_count from db submission
    almost['submission_count_fromdb'] = itcomesdowntolevel.groupby(['user_id', 'class_id'])['submission_id'].count()

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
    # now select only class98222 from db to concat with 98222half from amplitude
    # ignore student who is not in both sources

    galmost = almost.groupby(['user_id', 'class_id']).agg(lambda x:x.tolist())

    galmost['activity_count_per_class'] = galmost['activity_id'].str.len()
    avglevel = galmost.explode('level').groupby(['user_id', 'class_id'])['level'].mean()
    avgspent_time = galmost.explode('spent_time').groupby(['user_id', 'class_id'])['spent_time'].mean()
    avgseconds_early = galmost.explode('seconds_early').groupby(['user_id', 'class_id'])['seconds_early'].mean()


    galmost = galmost.assign(avglevel= avglevel)
    galmost = galmost.assign(avgspent_time=avgspent_time)
    galmost = galmost.assign(avgseconds_early=avgseconds_early)

    galmost['total_submission'] = itcomesdowntolevel.groupby(['user_id', 'class_id'])['submission_id'].count()

    maxlevel = galmost.explode('level').groupby(['user_id', 'class_id'])['level'].max()
    galmost = galmost.assign(maxlevel=maxlevel)

    return galmost
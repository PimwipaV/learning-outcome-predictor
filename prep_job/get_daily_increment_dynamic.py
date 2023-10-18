import pandas as pd
from dynamic_ongoing_classes import get_ongoing_classes_list, get_required_level
from datetime import datetime, timedelta
from box3_prepro_db import box3_prepro_db
from box2r_prepro_amplitude import box2
import os

#minidf = box2()
#get_ongoing_classes_list(today)

def get_daily_increment(today, minidf, almost):
    
    ongoing = pd.read_csv(f'ongoing_obem_classes_{today}.csv')

    ongoing_obem_classes = ongoing['class_id'].unique()
    ongoing_obem_classes_list = [str(i) for i in ongoing_obem_classes]
    #ongoing_obem_classes_list.remove('177701')
    
    #check that all class_id to be loc is in string format
    classdata_from_amp_today = minidf.loc[minidf['class_id'].isin(ongoing_obem_classes_list)]
    assert(classdata_from_amp_today['class_id'].dtype == 'object')
    
    #today = datetime.datetime.strftime(today, '%Y%m%d')
    #no need anymore daily_increment_file = f'daily_increment_{yesterday}.csv'

    for boh in ongoing_obem_classes_list:
        classindexlist = minidf['class_id'].loc[minidf['class_id'] == boh].index
        course98222 = minidf.loc[minidf.index[classindexlist]]

        joined = course98222.groupby(['user_id', 'class_id']).count().drop('activity_id', axis=1).join(course98222.groupby(['user_id', 'class_id'])['activity_id'].nunique())

        joined = joined.rename(columns={'activity_id':'specific_activity_view'})
        t = minidf.groupby(['user_id', 'class_id'])['session_id'].nunique().index.isin(joined.index)
        intheclass = [i for i, x in enumerate(t) if x]
        minidf.groupby(['user_id', 'class_id'])['session_id'].nunique()[intheclass]
        session_id_count = minidf.groupby(['user_id', 'class_id'])['session_id'].nunique()[intheclass]
        joined = joined.assign(session_id_count=session_id_count)
        joined = joined.drop('session_id', axis =1)
        #print("length joined", len(joined))
        rows_from_amp_today = len(joined)

        dbslice = almost.loc[(slice(None), boh),:]
        #print("dbslice index", dbslice.index)
        #print("joined index", joined.index)
        #print(joined)
        dbslice.reset_index(inplace=True)#, drop=True)
        joined.reset_index(inplace=True)#, drop=True)

        classdata = pd.concat([dbslice, joined], axis=1, join='inner')
        
        # check that length of joined dataframe is less than or equal to rows from amp
        assert (len(classdata.index) <= rows_from_amp_today)
        
        classdata.to_csv('daily_increment.csv', mode='a')
        
    daily_increment_manual = pd.read_csv('daily_increment.csv')
    os.remove('daily_increment.csv')
    
    daily_increment_manual = daily_increment_manual.drop('Unnamed: 0', axis=1)
    daily_increment_manual = daily_increment_manual.fillna(0)

    dup = daily_increment_manual.loc[daily_increment_manual['class_id'] == 'class_id']
    daily_increment_manual.drop(dup.index, inplace=True)

    daily_increment_manual['user_id'].astype(float)
    daily_increment_manual['user_id'] = daily_increment_manual['user_id'].astype(int)
    daily_increment_manual['class_id']=daily_increment_manual['class_id'].astype(float)
    daily_increment_manual['class_id'] = daily_increment_manual['class_id'].astype(int)

    daily_increment_manual['user_id'] = daily_increment_manual['user_id'].map(str)
    daily_increment_manual['class_id'] = daily_increment_manual['class_id'].map(str)
    daily_increment_manual = daily_increment_manual.set_index(['user_id', 'class_id'])
    
    return daily_increment_manual
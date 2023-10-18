import glob
import json
import os
import pandas as pd
import csv

#this function needs to be executed where the extracted daily amplitude files are located

def process_file(file_path, class_id_num):
    
    #json_dir = os.getcwd()
    #print(json_dir)
    #json_pattern = os.path.join(json_dir, '*.json')
   # file_list = glob.glob(json_pattern)

   # dfs = []
    #for file in file_list:
    with open(file_path) as f:
                #json_data = pd.json_normalize(json.loads(f.read()))
        json_data = pd.json_normalize(json.loads(line) for line in f)
        json_data['site'] = file_path.rsplit("/", 1)[-1]
        #dfs.append(json_data)
        
    json_data.drop(json_data.columns.difference(['user_id', 'client_event_time', 'event_id',
           'session_id', 'event_type','event_properties.CLASS',
           'event_properties.TEXT', 'event_properties.LOGIN_BY',
           'event_properties.CURRENT_URL', 'user_properties.USERNAME',
           'user_properties.EMAIL', 'user_properties.USER_ID',
           'user_properties.UNIV_ID', 'event_properties.TIME_STAMP',
           'event_properties.ID', 'event_properties.VALUE',
           'event_properties.K_OBJ', 'event_properties.K_OBJ_WRAPPER',
           'user_properties.PROFILE', 'event_properties.NAME','site']), 1, inplace=True)
    
    df = json_data.loc[json_data['event_type'] == 'Page Load'] 


    sortedbyuserid = df.sort_values('user_id')
    sortedandreset = sortedbyuserid.reset_index()
    class_id = sortedandreset['event_properties.CURRENT_URL'].str.findall("class/(\d+)")
    sortedandreset['class_id'] = class_id
    sortedandreset['activity_page_view'] = sortedandreset['event_properties.CURRENT_URL'].str.extract(r"(/activity$)")

    minidf = sortedandreset[['user_id', 'class_id','session_id','activity_page_view']]
    minidf = minidf.explode('class_id')

    minidf['activity_id'] = sortedandreset['event_properties.CURRENT_URL'].str.findall("/(?<=activity/)(\d+)")
    minidf['material_view'] = sortedandreset['event_properties.CURRENT_URL'].str.findall("/learning-activities")
    minidf['act+quiz_view'] = sortedandreset['event_properties.CURRENT_URL'].str.findall("/(?<=activity/)(\d+)|/(?<=quiz/)(\d+)")
    minidf['submit_button_clicked'] = sortedandreset['event_properties.CURRENT_URL'].str.findall("(submission)")

    minidf = minidf.explode('activity_id')
    minidf = minidf.explode('material_view')
    minidf = minidf.explode('act+quiz_view')
    minidf = minidf.explode('submit_button_clicked')

    minidf['class_view_count'] = minidf.groupby(['user_id', 'class_id']).cumcount()+1

    classindexlist = minidf['class_id'].loc[minidf['class_id'] == str(class_id_num)].index
    course98222 = minidf.loc[minidf.index[classindexlist]]

    #print(course98222.groupby(['user_id', 'class_id']).count())
    #print(course98222['activity_id'])
    
#here i feature engineering specific activity view, not just regex out like above
    joined = course98222.groupby(['user_id', 'class_id']).count().drop('activity_id', axis=1).join(course98222.groupby(['user_id', 'class_id'])['activity_id'].nunique())

    joined = joined.rename(columns={'activity_id':'specific_activity_view'})

    t = minidf.groupby(['user_id', 'class_id'])['session_id'].nunique().index.isin(joined.index)
    intheclass = [i for i, x in enumerate(t) if x]
    minidf.groupby(['user_id', 'class_id'])['session_id'].nunique()[intheclass]
    session_id_count = minidf.groupby(['user_id', 'class_id'])['session_id'].nunique()[intheclass]
    joined = joined.assign(session_id_count=session_id_count)
    joined = joined.drop('session_id', axis =1)
    
    
    return joined
    
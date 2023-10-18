import glob
import json
import os
import pandas as pd
import numpy as np
import shutil

def box2():
#json_dir = '../amp_tmp_files6/extracted/211291/'
    json_dir = '../amp_tmp_files/extracted/211291/'


    json_pattern = os.path.join(json_dir, '*.json')
    file_list = glob.glob(json_pattern)

    dfs = []
    for file in file_list:
        with open(file) as f:
        #json_data = pd.json_normalize(json.loads(f.read()))
            json_data = pd.json_normalize(json.loads(line) for line in f)
            json_data['site'] = file.rsplit("/", 1)[-1]
            dfs.append(json_data)
    df = pd.concat(dfs)

    df.drop(df.columns.difference(['user_id', 'client_event_time', 'event_id',
               'session_id', 'event_type','event_properties.CLASS',
               'event_properties.TEXT', 'event_properties.LOGIN_BY',
               'event_properties.CURRENT_URL', 'user_properties.USERNAME',
               'user_properties.EMAIL', 'user_properties.USER_ID',
               'user_properties.UNIV_ID', 'event_properties.TIME_STAMP',
               'event_properties.ID', 'event_properties.VALUE',
               'event_properties.K_OBJ', 'event_properties.K_OBJ_WRAPPER',
               'user_properties.PROFILE', 'event_properties.NAME','site']), 1, inplace=True)
    df = df.loc[df['event_type'] == 'Page Load'] 
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
    
    shutil.rmtree("../amp_tmp_files/extracted/211291/") 
        
    return minidf
#box2()

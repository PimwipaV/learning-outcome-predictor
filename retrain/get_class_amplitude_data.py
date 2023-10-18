import pandas as pd
import numpy as np
import datetime
import pytz
from datetime import datetime, timedelta
from prep_script import get_date, run_bash_script

#class_id_num = 227263

def get_class_amplitude_data(class_id_num):

    ongoing_obem_classes_list = pd.read_csv(f'../prep_job/ongoing_classes_list/ongoing_obem_classes_{today}.csv')
    
    selectedclassdf = ongoing_obem_classes_list[['class_id','activity_id','start_date', 'due_date']].loc[ongoing_obem_classes_list['class_id'] == str(class_id_num)]

    selectedclassdf = selectedclassdf.sort_values(by='due_date') #<--by due_date or start_date?
    
    #get data of a class upto midclass
    mid_class = len(selectedclassdf)/2
    rounded_mid_class = round(mid_class)

    mid = selectedclassdf[['due_date']].iloc[rounded_mid_class-1].to_string(index=False)
    cutpoint = datetime.strptime(mid,'%Y-%m-%d %H:%M:%S') + timedelta(hours=1)

    #choose date to download data of this class from the start_date till half class
    min_start = selectedclassdf['start_date'].min()
    min_start = datetime.strptime(min_start,'%Y-%m-%d %H:%M:%S')

    local = pytz.timezone("Asia/Bangkok")
    local_dt_start = local.localize(min_start, is_dst=None)
    local_dt_cutpoint = local.localize(cutpoint, is_dst=None)
    utc_dt_start = local_dt_start.astimezone(pytz.utc)
    utc_dt_cutpoint = local_dt_cutpoint.astimezone(pytz.utc)

    start = utc_dt_start.strftime('%Y%m%dT%H')
    end = utc_dt_cutpoint.strftime('%Y%m%dT%H')
    output_path = start_to_dl+"-"+end_to_dl+".json"
    print(start)
    print(end)
    print(output_path)
    
    #download data
    run_bash_script("./amp_export.sh", start, end, output_path)

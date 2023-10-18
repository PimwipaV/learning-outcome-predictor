from datetime import datetime, timedelta
import pandas as pd
from dynamic_ongoing_classes import get_ongoing_classes_list
import numpy as np

def get_classes_morethan1act(today):
    
    #today = datetime.today()

    #get_ongoing_classes_list(today)
    ongoing_obem_classes_list = pd.read_csv(f'../prep_job/ongoing_classes_list/ongoing_obem_classes_{today}.csv')

    #keep only classes with more than 1 activity
    activity_id_per_class = ongoing_obem_classes_list.reset_index().groupby('class_id')['activity_id'].nunique()
    activity_id_per_class == 1
    keep = activity_id_per_class != 1

    classes_morethan1_act = np.unique(ongoing_obem_classes_list['class_id'])[keep].tolist()
    itcomesdownto = ongoing_obem_classes_list.loc[ongoing_obem_classes_list['class_id'].isin(classes_morethan1_act)]

    selected_classes = itcomesdownto['class_id'].unique()
    
    return selected_classes
from sklearn import preprocessing

def sum_norm(classdata):

    min_max_scaler = preprocessing.MinMaxScaler()

    classdata[['N_specific_act_view', 'N_activity_count',
           'N_total_submission', 'N_session_id_count', 'N_class_view_count',
           'N_activity_page_view', 'N_material_view', 'N_avgspent_time',
           'N_avgseconds_early', 'N_act+quiz_view', 'N_submit_button_clicked',
           'N_expectation_met_count', 'N_avglevel']]=min_max_scaler.fit_transform(classdata[['specific_activity_view', 'activity_count_per_class',
           'total_submission', 'session_id_count', 'class_view_count',
           'activity_page_view', 'material_view', 'avgspent_time',
           'avgseconds_early', 'act+quiz_view', 'submit_button_clicked',
           'expectation_met_count', 'avglevel']])
    
    normalized =classdata[['N_specific_act_view', 'N_activity_count',
           'N_total_submission', 'N_session_id_count', 'N_class_view_count',
           'N_activity_page_view', 'N_material_view', 'N_avgspent_time',
           'N_avgseconds_early', 'N_act+quiz_view', 'N_submit_button_clicked',
           'N_expectation_met_count', 'N_avglevel', 'maxlevel']]
        
    return normalized

import pandas as pd

def concat_classdata(galmost, dfconcat, class_id_num):

    dfconcat = dfconcat.reset_index()
    dfconcat['user_id'] = dfconcat['user_id'].map(str)
    dfconcat['class_id'] = dfconcat['class_id'].map(str)

    gdfconcat = dfconcat.groupby(['user_id', 'class_id']).agg(lambda x:x.tolist())

    gdfconcat['activity_page_view'] = gdfconcat['activity_page_view'].apply(lambda x: sum(x))
    gdfconcat['material_view'] = gdfconcat['material_view'].apply(lambda x: sum(x))
    gdfconcat['act+quiz_view'] = gdfconcat['act+quiz_view'].apply(lambda x: sum(x))
    gdfconcat['submit_button_clicked'] = gdfconcat['submit_button_clicked'].apply(lambda x: sum(x))
    gdfconcat['class_view_count'] = gdfconcat['class_view_count'].apply(lambda x: sum(x))
    gdfconcat['specific_activity_view'] = gdfconcat['specific_activity_view'].apply(lambda x: sum(x))
    gdfconcat['session_id_count'] = gdfconcat['session_id_count'].apply(lambda x: sum(x))

# concat amplitude (dfconcat) with slice of db only for this class_id
    classdata =pd.concat([galmost.loc[(slice(None), str(class_id_num)),:],gdfconcat], axis=1, join='inner')
    
    classdata['expectation_met_count'] = classdata['expectation_met_count'].apply(lambda x: sum(x))

    return classdata

#writer = pd.ExcelWriter('class171499.xlsx', engine='xlsxwriter')
#class171499.to_excel(writer, sheet_name='class171499')
#writer.save()
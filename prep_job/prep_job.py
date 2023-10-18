from datetime import datetime, timedelta
from prep_script import get_date, run_bash_script
from dynamic_ongoing_classes import get_ongoing_classes_list, get_required_level
from box2r_prepro_amplitude import box2
from box3_prepro_db import box3_prepro_db
from get_daily_increment_dynamic import get_daily_increment
from query_upto_yesterday_handling import query_upto_yesterday
from agg_and_write import aggregation
from write_to_db_fn import write_to_db
from inference import inference
from write_predictions import dictionary_iterator, write_predictions

def daily_prep_job():

    #get yesterday's date to download amplitude data
    today = datetime.today() #uncomment this to fix alarm #- timedelta(days =1)
    yesterday = today- timedelta(days=1)

    # download data from amplitude
    start, end, output_path = get_date(yesterday)
    run_bash_script("./amp_export.sh", start, end, output_path) 

    #get today in the required format
    today = yesterday + timedelta(days=1)
    today = today.strftime('%Y%m%d')

    # connect to db to get the latest list of ongoing_obem_classes
    get_ongoing_classes_list(today)
    get_required_level(today)

    # query_upto_yesterday from dynamodb, get data from leb2 db, and get amplitude data
    upto_yesterday = query_upto_yesterday(today)
    minidf = box2()
    almost = box3_prepro_db(today)

    # process amplitude data taking only class_id_num in question(minidf)
    # add it to data from leb2 db(almost)
    daily_increment = get_daily_increment(today, minidf, almost)

    # concat what we got back from dynamodb (upto_yesterday) with today's data
    incremented = aggregation(today, upto_yesterday, daily_increment)

    today = int(today)

    # write processed data to dynamodb
    write_to_db(today, incremented)

    # sum, avg, normalize then input for inference
    gincremented = inference(today, incremented)

    # write predictions to dynamodb
    response = write_predictions(today, gincremented)
    
    print ("wrote"+str(len(gincremented))+"predictions to dynamodb on"+str(today))
    return response

if __name__ == "__main__":
    daily_prep_job()
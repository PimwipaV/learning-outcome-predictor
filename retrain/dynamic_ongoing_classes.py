import csv
from re import T
import mysql.connector
import os
from datetime import datetime

def get_ongoing_classes_list(today):

    print(today)
    #today = today.strftime('%Y%m%d')
    DB_USER = os.environ.get('DB_USER')
    DB_PASS = os.environ.get('DB_PASS')
    DB_HOST = os.environ.get('DB_HOST')

    cnx = mysql.connector.connect(
    host =DB_HOST,
    user =DB_USER,
    password =DB_PASS,
    database ="leb2")

    # Open the SQL file
   # with open('ongoing_classes1_2022_with_level.sql', 'r') as file:
    with open('../sql/ongoing_obem_dated_within_4months.sql', 'r') as file:
        query = file.read()

    cursor = cnx.cursor()
    cursor.execute(query)

    # Save the result to a CSV file
    with open(f'../prep_job/ongoing_classes_list/ongoing_obem_classes_{today}.csv', 'w', newline='') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor.fetchall())

    # Close the cursor and connection
    cursor.close()
    cnx.close()




import pandas as pd

def get_required_level(today):

    ongoing = pd.read_csv(f'../prep_job/ongoing_classes_list/ongoing_obem_classes_{today}.csv')

    ongoing_obem_classes = ongoing['class_id'].unique()
    ongoing_obem_classes_list = [str(i) for i in ongoing_obem_classes]
    ongoing_cri_id = ongoing['cri_id'].unique()
    ongoing_cri_id_list = [str(i) for i in ongoing_cri_id]

    DB_USER = os.environ.get('DB_USER')
    DB_PASS = os.environ.get('DB_PASS')
    DB_HOST = os.environ.get('DB_HOST')

    cnx = mysql.connector.connect(
    host =DB_HOST, 
    user =DB_USER,
    password =DB_PASS,
    database ="leb2")

    # Define the list to be injected
    #my_list = ['value1', 'value2', 'value3']

    # Define the SQL query with a placeholder for the list
    #query = "SELECT * FROM my_table WHERE column_name IN (%s)"

    with open('../sql/required_level_1_2022_with_placeholder.sql', 'r') as file:
        query = file.read()

    # Create a string representation of the list for the query placeholder
    query_placeholder = ', '.join(['%s'] * len(ongoing_cri_id_list))

    # Execute the query with the list of values as a parameter
    cursor = cnx.cursor()
    cursor.execute(query % query_placeholder, ongoing_cri_id_list)



    with open(f'../prep_job/required_level/required_level_{today}.csv', 'w', newline='') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor.fetchall())

    # Close the cursor and connection
    cursor.close()
    cnx.close()

if __name__ == '__main__':
    today = datetime.today().strftime('%Y%m%d')
    get_ongoing_classes_list(today)
    get_required_level(today)
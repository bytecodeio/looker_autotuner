import looker_sdk
import pandas as pd
import os
from dotenv import load_dotenv
from looker_sdk import methods40, models40

load_dotenv()

sdk = looker_sdk.init40() 
me = sdk.me()
slug = os.getenv('QUERY_PERFORMANCE_EXPLORE_SLUG')


def get_dashboard_data(slug):
# This function retrieves the data from the explore in Looker and saves it to a pandas dataframe
    try:
        response = sdk.query_for_slug(slug)
        table1 =response['id']
        if table1 is not None:
            query = sdk.run_query(query_id=table1, result_format='csv')
        else:
            exit("Query ID was not returned")

    except Exception as error:
        print("The following error occurred:",error)
    
    
    return query

# Writing data to a .csv file
query1= get_dashboard_data(slug)
file = open("result.csv", "w")
file.write(query1)
file.close()
    
df = pd.read_csv('result.csv')

# Checking to make sure dataframe is not empty
if len(df) < 1:
    exit("Error: Dataframe is currently empty. Please check the source table in Looker")
else:
    pass


def create_sql_model(df):
# This function takes the top result from the pandas dataframe and retrieves the SQL from the query id
    try:
        query_id = df["Query ID"].iloc[0]
        query_id_2 = query_id.astype('str')
        query_2 = sdk.run_query(query_id=query_id_2, result_format='sql')
        
    except Exception as error:
        print("The following error occurred:",error)
    
    return query_2

dashboard1= dashboard = df["Dashboard Title"].iloc[0].lower().replace(' ', '_').replace('.','').replace("'",'')
query2=create_sql_model(df)

# Checking to make sure the function has returned SQL
if query2 is not None:
    file = open("models/%s.sql" % dashboard1, "w")
    file.write(query2)
    file.close()
else:
    exit("Query ID returned no SQL")

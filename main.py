import looker_sdk
import pandas as pd


from looker_sdk import methods40, models40
sdk = looker_sdk.init40() 
me = sdk.me()

response = sdk.query_for_slug(slug="nbAhUcfmfJD83VfUXNvWOb")
table1 =response['id']
query = sdk.run_query(query_id=table1, result_format='csv')
file = open("result.csv", "w")
file.write(query)
file.close()
df = pd.read_csv('result.csv')

query_id = df["Query ID"].iloc[0]
query_id_2 = query_id.astype('str')
query_2 = sdk.run_query(query_id=query_id_2, result_format='sql')
file = open("query.sql", "w")
file.write(query_2)
file.close()
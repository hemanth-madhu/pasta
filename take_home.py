import pandas as pd
import requests
import mysql.connector
from sqlalchemy import create_engine
#Define API Parameters
email = 'sfdcshared@gmail.com'
password = 'InNn8li^AY27CRa8'

client_id = '3MVG9ux34Ig8G5epaMPqbA1E25OpLuKGuGcWZixMzgV6myFvvKoIQnGrMY5mg9pTNHPBWj9GgJNuwD0TAIEIy'
client_secret = '50D529BA45FA479E8FE492C5BC5CBE774452FF21597B1ED8D73352105428F3FC'

request_token_url = "https://login.salesforce.com/services/oauth2/token"
api_two_circles= 'http://koresoftware-dev-ed.develop.my.salesforce.com'

request_json = {
    'grant_type': 'password',
    'client_id': client_id,
    'client_secret': client_secret,
    'username': email,
    'password': password
}

#Define MYSQL Parameters for source
host_source ='kinterview-db.cluster-cnawrkmxrmmc.us-west-2.rds.amazonaws.com'
userid_source ='adventureworks_madhu_user'
password_source ='v4#%W007Cl'

#Define MYSQL Parameters for target
host_target ='kinterview-db.cluster-cnawrkmxrmmc.us-west-2.rds.amazonaws.com'
userid_target ='dw_madhu_user'
password_target ='v4#%W007Cl'
target_port = 3306
target_database = 'dw_madhu'

#SQL to Merge three tables in MYSQL
sql = "SELECT a.FirstName, a.LastName, b.EmailAddress, c.PhoneNumber FROM Person_Person a INNER JOIN Person_EmailAddress b ON a.BusinessEntityID = b.BusinessEntityID INNER JOIN Person_PersonPhone c ON a.BusinessEntityID = c.BusinessEntityID LIMIT 2000"

#Function to Parse Nested JSON

def cross_join(left,right):
    return left.assign(key=1).merge(right.assign(key=1), on='key', how='outer').drop('key',axis = "columns")

def json_to_dataframe(data_in):
    def to_frame(data,prev_key=""):
        if isinstance(data, dict):
            df = pd.DataFrame()
            for key in data:
                df = cross_join(df, to_frame(data[key], prev_key + '.' + key))
        elif isinstance(data, list):
            df = pd.DataFrame()
            for i in range(len(data)):
                df = pd.concat([df, to_frame(data[i],prev_key)])
        else:
            df = pd.DataFrame({prev_key[1:]: [data]})
        return df
    return to_frame(data_in)

#Call SalesForce API
def get_api_data():
    req_token= requests.post(request_token_url,headers={"Content-Type":"application/x-www-form-urlencoded"}, data=request_json)
    body = req_token.json()
    token = body['access_token']
    url = api_two_circles + "/services/data/v51.0/query/?q=SELECT+Id,FirstName,LastName,Email,Phone,DoNotCall+FROM+contact"
    req_data= requests.get(url, headers = {"Authorization":"Bearer " + token}).json()
    return req_data

#Process the API Response
def transform_json():
    json_data = get_api_data()
    df_salesforce = json_to_dataframe(json_data)
    df_salesforce = df_salesforce.loc[df_salesforce['records.DoNotCall'] != 'True']
    df_final = df_salesforce[['records.FirstName', 'records.LastName', 'records.Email','records.Phone']].drop_duplicates(subset=['records.Email'])
    df_final.columns = df_final.columns.str.lstrip('records.')
    df_final['flag'] = 'isSalesforce'
    return df_final 

#Process the MYSQL Tables
def mysql_connection_source():
    db = mysql.connector.connect(
        host= host_source,
        user= userid_source,
        password= password_source 
    )
    cur = db.cursor()
    cur.execute("use adventureworks")
    cur.execute(sql)
    df_mysql = pd.DataFrame(cur.fetchall(),columns=['FirstName','LastName','EmailAddress','PhoneNumber']).drop_duplicates(subset=['EmailAddress'])
    df_mysql.rename(columns={'EmailAddress': 'Email', 'PhoneNumber': 'Phone'}, inplace=True)
    df_mysql['flag'] = 'isSalesLT'
    return df_mysql
#Store the Table in Target
def to_target_mysql():
    df_salesforce = transform_json()
    df_mysql = mysql_connection_source()
    df_leads = pd.concat([df_salesforce, df_mysql], ignore_index=True).drop_duplicates(subset=['Email'])
    engine = create_engine(url = "mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}".format(userid_target, password_target, host_target, target_port, target_database))
    df_leads.to_sql('dw_leads', con=engine, if_exists='append', index=False)

if __name__ == '__main__':
    to_target_mysql()

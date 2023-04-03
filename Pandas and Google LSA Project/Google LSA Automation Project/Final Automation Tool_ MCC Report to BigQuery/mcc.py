import os
from google.cloud import bigquery
import requests
import json
import re
import ast
import numpy as np
import pandas as pd
import datetime
import gspread
import time
from functools import reduce
from oauth2client.service_account import ServiceAccountCredentials
from pytz import timezone

def Test():
    print('Updating Script or Temporary paused or Testing')

def refreshToken(client_id, client_secret, refresh_token):
        params = {
                "grant_type": "refresh_token",
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token
        }

        authorization_url = "https://www.googleapis.com/oauth2/v4/token"

        r = requests.post(authorization_url, data=params)

        if r.ok:
                return r.json()['access_token']
        else:
                return None

def LSA_main():
  
    client_id = '4905***'
    client_secret = 'GOC***'     
    refresh_token='1//0gY***'

    access_token = refreshToken(client_id, client_secret, refresh_token)
    access_token_auth = 'Bearer ' + access_token
    
    base_url = "https://localservices.googleapis.com/v1/"
    z =  datetime.datetime.now(tz  = timezone('America/New_York')) 
    y = z.date()
    y1 = y - datetime.timedelta(days=1)
    day = y1.day
    month = y1.month
    year = y1.year
    
    url_prep_for_mcc = base_url + "accountReports:search?query=manager_customer_id:{manager_id}"
    final_url = url_prep_for_mcc.format(manager_id="368***" )
    lsa_response = requests.get(final_url,params={'startDate.day':day,'startDate.month':month,'startDate.year': year,'endDate.day':day,'endDate.month':month,'endDate.year':year},
    	headers={'Host':'localservices.googleapis.com','User-Agent':'curl','Content-Type':'application/json','Accept':'application/json','Authorization': access_token_auth}) 
    if lsa_response.status_code ==200:
        lsa_data = json.dumps(lsa_response.json(), indent = 3)
        lsa_data = lsa_response.json()['accountReports']
    business_Name=[]
    accountId=[]
    try:
        for i in range(len(lsa_data)):
            accountId.append(lsa_data[i]['accountId'])
            business_Name.append(lsa_data[i]['businessName'])
    except:
        pass
    return [accountId,business_Name]

def BigQueryTableCreation(Acc_info):
	Table = [s.replace(",", "") for s in Acc_info[1]]
	Tabletemp = [s.replace(".", "") for s in Table]
	Table_name = [s.replace("&", "and") for s in Tabletemp]
	os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'bigquery-service-key.json'
	base_table_id = 'bigquery-database-3***.MCC_Report.'
	client = bigquery.Client()
	for i in range(len(Table_name)):
	    try:
	        table_id = base_table_id + Table_name[i]
	        table = bigquery.Table(table_id)
	        try:
	            table = client.create_table(table)  # Make an API request.
	            print(
	                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
	                 )
	        except:
	            pass
	    except:
	        pass
	return Table_name

def Getting_LSA_dataframe(Acc_info):
	base_url = "https://localservices.googleapis.com/v1/"
	z =  datetime.datetime.now(tz  = timezone('America/New_York')) 
	y = z.date()
	y1 = y - datetime.timedelta(days=1)
	day = y1.day
	month = y1.month
	year = y1.year 
	client_id = '4905***'
	client_secret = 'GOC***'     
	refresh_token='1//0gY***'
	access_token = refreshToken(client_id, client_secret, refresh_token)
	access_token_auth = 'Bearer ' + access_token
	base = datetime.date.today()
	List=[]
	try:
		url_prep_for_mcc = base_url + "accountReports:search?query=manager_customer_id:{manager_id};customer_id:{customer_id}"
		final_url = url_prep_for_mcc.format(manager_id="368***",customer_id=Acc_info)
		lsa_response = requests.get(final_url,params={'startDate.day':day ,'startDate.month':month,'startDate.year': year,'endDate.day':day ,'endDate.month':month,'endDate.year':year},headers={'Host':'localservices.googleapis.com','User-Agent':'curl','Content-Type':'application/json','Accept':'application/json','Authorization': access_token_auth}) 
		if lsa_response.status_code==200:
		    lsadata = json.dumps(lsa_response.json(), indent = 3)
		    lsadata = lsa_response.json()['accountReports'][0]
		    lsadata['Date'] = y1
		    List.append(lsadata)
	except:
		pass
	accountReports = []
	try:
		Data = json.dumps(List, indent = 3 , sort_keys=True, default=str)
		accountReports = pd.read_json(Data , orient='records')
		accountReports.sort_values(by=['Date'],ascending=False)
	except:
		pass
	return accountReports

def Data_preprocessing(accountReports):
	for i in range(len(accountReports)):
		try:
		    accountReports[i] = accountReports[i].rename(columns={'currentPeriodChargedLeads':'Charged_Leads','currentPeriodConnectedPhoneCalls':'Connected_Calls','currentPeriodPhoneCalls':'Phone_Calls','currentPeriodTotalCost':'Total_Cost','phoneLeadResponsiveness':'Phone_Lead_Responsiveness','averageFiveStarRating':'Avg_Rating','averageWeeklyBudget': 'Avg_Weekly_Budget','totalReview':'Total_Reviews'})
		    try:
		        accountReports[i]['Avg_CPL'] = accountReports[i]['Total_Cost'].astype(float)/accountReports[i]['Charged_Leads'].astype(int)
		    except ZeroDivisionError:
		        accountReports[i]['Avg_CPL'] = np.nan
		    accountReports[i]['Avg_CPL'] = accountReports[i]['Avg_CPL'].fillna(0)
		    accountReports[i][['Charged_Leads', 'Connected_Calls','Phone_Calls','Total_Cost','Avg_CPL','previousPeriodChargedLeads','previousPeriodPhoneCalls','previousPeriodConnectedPhoneCalls','previousPeriodTotalCost']] = accountReports[i][['Charged_Leads', 'Connected_Calls','Phone_Calls','Total_Cost','Avg_CPL','previousPeriodChargedLeads','previousPeriodPhoneCalls','previousPeriodConnectedPhoneCalls','previousPeriodTotalCost']].apply(pd.to_numeric)
		    accountReports[i]['Missed_Calls'] = accountReports[i]['Phone_Calls'] - accountReports[i]['Connected_Calls']
		    accountReports[i]['Total_Cost'] = accountReports[i]['Total_Cost'].astype('float')
		    accountReports[i]['previousPeriodTotalCost'] = accountReports[i]['previousPeriodTotalCost'].astype('float')
		    accountReports[i]['Phone_Lead_Responsiveness'] = accountReports[i]['Phone_Lead_Responsiveness'].astype('float')
		    accountReports[i] = accountReports[i][['Date','accountId','businessName','Avg_Rating','Total_Reviews','Charged_Leads','Connected_Calls','Phone_Calls','Missed_Calls','Total_Cost','Avg_CPL','Avg_Weekly_Budget','Phone_Lead_Responsiveness','previousPeriodChargedLeads','previousPeriodPhoneCalls','previousPeriodConnectedPhoneCalls','previousPeriodTotalCost']]
		    accountReports[i].drop_duplicates(keep='first',inplace=True)
		except:
		    pass
	return accountReports

def TobigQuery(Final_df,Table_name):
	try:
	    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'bigquery-service-key.json'
	    client = bigquery.Client()
	    for i in range(len(Final_df)):
	        try:
	           table_id = 'bigquery-database-3***.MCC_Report.' + Table_name[i]	
	           job_config = bigquery.LoadJobConfig(
	           autodetect=True,
	           write_disposition="WRITE_APPEND",
	           skip_leading_rows=0,
	           # The source format defaults to CSV, so the line below is optional.
	           source_format=bigquery.SourceFormat.CSV,
	           )         
	           job = client.load_table_from_dataframe(
	           Final_df[i] , table_id , job_config=job_config
	           )  # Make an API request.
	           print('Successfully Transfered: ' + Table_name[i])
	           time.sleep(5)             
	        except:
	           print('Empty: ' + Table_name[i])
	except:
	    pass

def Execute_Main():
	Acc_info = LSA_main()
	Table_name = BigQueryTableCreation(Acc_info)
	List = []
	for i in range(len(Acc_info[0])):
	    try:
	        Lead_List = Getting_LSA_dataframe(Acc_info[0][i])
	        List.append(Lead_List)
	    except:
	        print('Except ' + str(Acc_info[0][i]) + ": " + Acc_info[1][i])
	Final_df = Data_preprocessing(List)
	TobigQuery(Final_df,Table_name)

if __name__ == "__main__":
	Execute_Main()
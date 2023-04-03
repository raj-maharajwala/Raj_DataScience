import os
from google.cloud import bigquery
import requests
import json
import re
import ast
import numpy as np
import pandas as pd
import datetime
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
    client_id = '490***'
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
    final_url = url_prep_for_mcc.format(manager_id="36871***")
    lsa_response = requests.get(final_url,params={'startDate.day':day,'startDate.month':month,'startDate.year': year,'endDate.day':day,'endDate.month':month,'endDate.year':year},headers={'Host':'localservices.googleapis.com','User-Agent':'curl','Content-Type':'application/json','Accept':'application/json','Authorization': access_token_auth}) 
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
    time.sleep(2)
    return [accountId,business_Name]

def BigQueryTableCreation(Acc_info):
	Table = [s.replace(",", "") for s in Acc_info[1]]
	Tabletemp = [s.replace(".", "") for s in Table]
	Table_name = [s.replace("&", "and") for s in Tabletemp]
	os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'bigquery-service-key.json'
	base_table_id = 'bigquery-database-35***.Customer_Report.'
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
	time.sleep(2)
	return Table_name

def Getting_LSA_dataframe(customerid):
	base_url = "https://localservices.googleapis.com/v1/"
	z =  datetime.datetime.now(tz  = timezone('America/New_York')) 
	y = z.date()
	y1 = y - datetime.timedelta(days=1)
	day = y1.day
	month = y1.month
	year = y1.year 
	client_id = '490***'
	client_secret = 'GOC***'     
	refresh_token='1//0gYR***'
	access_token = refreshToken(client_id, client_secret, refresh_token)
	access_token_auth = 'Bearer ' + access_token
	base = datetime.date.today()
	List=[]
	try:
	    url_prep_for_mcc = base_url + "detailedLeadReports:search?query=manager_customer_id:{manager_id};customer_id:{customer_id}"
	    final_url = url_prep_for_mcc.format(manager_id="3687***", customer_id = customerid)
	    lsa_response = requests.get(final_url,params={'startDate.day':day ,'startDate.month':month,'startDate.year': year,'endDate.day':day ,'endDate.month':month,'endDate.year':year},
	    	headers={'Host':'localservices.googleapis.com','User-Agent':'curl','Content-Type':'application/json','Accept':'application/json','Authorization': access_token_auth}) 
	    if lsa_response.status_code ==200:
	        lsa_data = json.dumps(lsa_response.json(), indent = 3)
	        lsa_data = lsa_response.json()
	        if len(lsa_data)<1:
	            pass
	        elif len(lsa_data['detailedLeadReports'])==1:
	            lsaData = []
	            lsaData.append(lsa_data['detailedLeadReports'][0])
	            List.append(lsaData.copy())
	            print('Single')
	        elif len(lsa_data['detailedLeadReports'])>1:
	            print('Multiple')
	            lsaData = []
	            for j in range(len(lsa_data['detailedLeadReports'])):
	                lsaData.append(lsa_data['detailedLeadReports'][j])
	            List.append(lsaData.copy())
	        else:
	            pass
	            time.sleep(2)
	except:
	    pass
	time.sleep(2)
	return List
        
def Data_preprocessing(List,Acc_info):
	try:
	#Working on adding missing empty columns in all of the accountId
		for i in range(len(List)):
		    try:
		        if 'phoneLead.consumerPhoneNumber' not in List[i][0].columns:
		            List[i][0]['phoneLead.consumerPhoneNumber'] = np.nan
		        if 'phoneLead.chargedCallTimestamp' not in List[i][0].columns:
		            List[i][0]['phoneLead.chargedCallTimestamp'] = np.nan
		        if 'phoneLead.chargedConnectedCallDurationSeconds' not in List[i][0].columns:
		            List[i][0]['phoneLead.chargedConnectedCallDurationSeconds'] = np.nan
		        if 'disputeStatus' not in List[i][0].columns:
		            List[i][0]['disputeStatus'] = np.nan
		        if 'messageLead.consumerPhoneNumber' not in List[i][0].columns:
		            List[i][0]['messageLead.consumerPhoneNumber'] = np.nan
		        if 'messageLead.customerName' not in List[i][0].columns:
		            List[i][0]['messageLead.customerName'] = np.nan
		        if 'messageLead.postalCode' not in List[i][0].columns:
		            List[i][0]['messageLead.postalCode'] = np.nan
		        if 'messageLead.jobType' not in List[i][0].columns:
		            List[i][0]['messageLead.jobType'] = np.nan
		        time.sleep(2)
		    except:
		        pass
		time.sleep(3)
		#Renaming, Rearranging the columns, and setting datatypes
		for i in range(len(List)):
		    try:
		        List[i][0] = List[i][0].rename(columns={"phoneLead.consumerPhoneNumber": "consumerPhoneNumber", "phoneLead.chargedCallTimestamp": "chargedCallTimestamp","phoneLead.chargedConnectedCallDurationSeconds":"chargedConnectedCallDurationSeconds","timezone.id":"timezone"})
		        List[i][0][["leadId", "accountId"]] = List[i][0][["leadId", "accountId"]].astype('int64')
		        List[i][0][['disputeStatus','consumerPhoneNumber','chargedCallTimestamp','chargedConnectedCallDurationSeconds']] = List[i][0][['disputeStatus','consumerPhoneNumber','chargedCallTimestamp','chargedConnectedCallDurationSeconds']].astype(str)
		        
		        List[i][0] = List[i][0].rename(columns={"messageLead.consumerPhoneNumber": "consumerPhoneNumberML", "messageLead.customerName": "customerNameML","messageLead.postalCode":"postalCodeML","messageLead.jobType":"jobTypeML"})
		        List[i][0][['consumerPhoneNumberML','customerNameML','postalCodeML','jobTypeML']] = List[i][0][['consumerPhoneNumberML','customerNameML','postalCodeML','jobTypeML']].astype(str)
		        # Rearranging Columns
		        List[i][0] = List[i][0][['leadId', 'accountId', 'businessName', 'leadCreationTimestamp','leadType','disputeStatus', 'leadCategory', 'geo','consumerPhoneNumber','chargedCallTimestamp','chargedConnectedCallDurationSeconds' , 'chargeStatus', 'currencyCode', 'timezone' , 'consumerPhoneNumberML','customerNameML','postalCodeML','jobTypeML']]
		        time.sleep(2)
		    except:
		        print(str(i) + ':  ' + Acc_info[1][i])
		time.sleep(3)
	except:
		pass
	return List

def TobigQueryLeadReport(List,Table_name):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'bigquery-service-key.json'
    client = bigquery.Client()
    for i in range(len(List)):
        try:
            table_id = 'bigquery-database-350***.DetailedLead_Report.' + Table_name[i]
            job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition="WRITE_APPEND",
            skip_leading_rows=0,
            # The source format defaults to CSV, so the line below is optional.
            source_format=bigquery.SourceFormat.CSV,
            )        
            job = client.load_table_from_dataframe(
            List[i][0] , table_id , job_config=job_config
            )  # Make an API request.
            print('Last step: ' + Table_name[i])
            time.sleep(7)
        except:
            print('Empty: ' + Table_name[i] + str(i))

def Start_DataFetching():
    Acc_info = LSA_main()
    time.sleep(3)
    Table_name = BigQueryTableCreation(Acc_info)
    time.sleep(3)
    List = [[] for x in range(len(Acc_info[0]))]
    for i in range(len(Acc_info[0])):
        try:
            Lead_List = Getting_LSA_dataframe(Acc_info[0][i])
            accountReports = []
            for j in range(len(Lead_List)):
                Data = pd.json_normalize(Lead_List[j])
                accountReports.append(Data)
            df_merged = reduce(lambda left,right: pd.merge(left,right,how='outer'), accountReports)
            List[i].append(df_merged)
            time.sleep(5)
        except:
            print('Except ' + str(Acc_info[0][i]) + ": " + Acc_info[1][i])
    time.sleep(3)
    Final_Report = Data_preprocessing(List,Acc_info)
    time.sleep(3)
    TobigQueryLeadReport(Final_Report,Table_name)

if __name__ == "__main__":
    Start_DataFetching()
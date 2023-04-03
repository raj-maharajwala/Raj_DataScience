#basecamp_lsa_automation.py
import requests
import json
from basecampy3 import Basecamp3
import os
from google.cloud import bigquery
import requests
import json
import re
import ast
import numpy as np
import pandas as pd
import datetime
import pygsheets
import gspread
import time
from oauth2client.service_account import ServiceAccountCredentials
from config import CONFIG
from pytz import timezone
from apscheduler.schedulers.blocking import BlockingScheduler 


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

    print('Started main function\n')
    # use creds to create a client to interact with the Google Drive API
    scope = ['https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('graceful-trees-30**.json', scope)
    client = gspread.authorize(creds)
    sheet = client.open("LSA CLIENT LEADS").sheet1


    account_id = sheet.col_values(3)
    bussiness_name = sheet.col_values(4)
    Internal = sheet.col_values(1)


    Active_accounts = []
    for i in range(len(account_id)):
        if Internal[i]=='1':
            Active_accounts.append(bussiness_name[i])


    act_cmp = np.array(Active_accounts)
    uniq_active_cmp = np.unique(act_cmp)
    tnum = len(uniq_active_cmp)


    List = [[] for i in range(tnum)]
                
    for i in range(len(account_id)):
        for j in range(len(uniq_active_cmp)):
            if bussiness_name[i]==uniq_active_cmp[j] and Internal[i]=='1':
                List[j].append(account_id[i])

    n=0

            
    for i in range(tnum):
    	if uniq_active_cmp[i]=='Rob Levine & Associates':
    		n = i
    		List[n] = List[n] + List[n+1]
    		List.pop(n+1)

    client_id='7627***'
    client_secret='tk***'
    rfsheet = client.open("LSA Lead Refresh Token").sheet1
    RFtoken = rfsheet.col_values(1)
    RFtoken = RFtoken[1:]
    access_token = refreshToken(client_id, client_secret, RFtoken[0])
    access_token_auth = 'Bearer ' + access_token

    customer_id = []
    Output = [[] for i in range(tnum-1)]
    accountReports = [[] for i in range(tnum-1)]
    base_url = "https://localservices.googleapis.com/v1/"
    z =  datetime.datetime.now(tz  = timezone('America/New_York')) 
    y = z.date()
    y1 = y - datetime.timedelta(days=1)
    day = y1.day
    month = y1.month
    year = y1.year
        
    i=0
    for customer_account_id in List:    
        if len(customer_account_id)==1:
            
            customer_id = customer_account_id
            url_prep_for_mcc = base_url + "accountReports:search?query=manager_customer_id:{manager_id};" + "customer_id:{customer_id}"
            final_url = url_prep_for_mcc.format(manager_id="29***" , customer_id=customer_id[0])
            lsa_response = requests.get(final_url,params={'startDate.day':day ,'startDate.month':month,'startDate.year': year,'endDate.day':day ,'endDate.month':month,'endDate.year':year},headers={'Host':'localservices.googleapis.com','User-Agent':'curl','Content-Type':'application/json','Accept':'application/json','Authorization': access_token_auth}) 

            if lsa_response.status_code ==200:
                lsa_data = json.dumps(lsa_response.json(), indent = 3)
                lsa_data = lsa_response.json()['accountReports'][0]
                calls_connected = lsa_data['currentPeriodConnectedPhoneCalls']
                phone_calls = lsa_data['currentPeriodPhoneCalls']
                calls_notconnected = int(phone_calls) - int(calls_connected)
                Output[i].append(calls_notconnected)
            else:
            	print('Might be InActive for customer_id ' + customer_id[0])
            
            
        elif len(customer_account_id)>1:
            customer_id = customer_account_id
            
            for customer_id_n in customer_id:            
                url_prep_for_mcc = base_url + "accountReports:search?query=manager_customer_id:{manager_id};" + "customer_id:{customer_id}"
                final_url = url_prep_for_mcc.format(manager_id="29***" , customer_id=customer_id_n)
                lsa_response = requests.get(final_url,params={'startDate.day':day ,'startDate.month':month,'startDate.year': year,'endDate.day':day ,'endDate.month':month,'endDate.year':year},headers={'Host':'localservices.googleapis.com','User-Agent':'curl','Content-Type':'application/json','Accept':'application/json','Authorization': access_token_auth}) 

                if lsa_response.status_code ==200:
                    lsa_data = json.dumps(lsa_response.json(), indent = 3)
                    lsa_data = lsa_response.json()['accountReports'][0]
                    calls_connected = lsa_data['currentPeriodConnectedPhoneCalls']
                    phone_calls = lsa_data['currentPeriodPhoneCalls']
                    calls_notconnected = int(phone_calls) - int(calls_connected)
                    Output[i].append(calls_notconnected)  
                else:
                	print ('Might be inactive for customer_id' + customer_id_n)  

        i=i+1
        time.sleep(3)

    print('for loops completed')
    Tables = sheet.col_values(7)
    Tables = Tables[1:]
    account_id = account_id[1:]
    Internal = Internal[1:]
    ActTable = []
    n=0
    for i in range(len(account_id)):  
    	if Internal[i]=='1':
    		ActTable.append(Tables[i])

    indexes = np.unique(ActTable, return_index=True)[1]
    uniq_active_table = [ActTable[index] for index in sorted(indexes)]
    print(uniq_active_table)
    return [List,Output,uniq_active_table]

def BasecampMain():
	bc3 = Basecamp3(client_id=CONFIG['client_id'],client_secret = CONFIG['client_secret'],redirect_uri=CONFIG['redirect_uri'],
                access_token=CONFIG['access_token'],refresh_token=CONFIG['refresh_token'])
	Result = []
	Result = LSA_main()

	Tables = Result[2]
	keys_list = Result[0]
	values_list = Result[1]
	zip_iterator=[]
	x = datetime.date.today() - datetime.timedelta(days=1)
	for i in range(len(keys_list)):
	    zip_iterator.append(dict(zip(keys_list[i], values_list[i])))

	    L=[]
	    L.append("Date:" + str(x) + "\n")
	    for i in range(len(keys_list)):
	        try:
	            if len(zip_iterator[i].keys())==1:
	                res = list(zip_iterator[i].keys())[0]
	                val = list(zip_iterator[i].values())[0]
	                L.append("account_id: " + res + ",\t\tmissed calls are: " + str(val) + ",\t\tcompany_name: " + Tables[i] +"\n")
	            elif len(zip_iterator[i].keys())>1:
	                res = list(zip_iterator[i].keys())
	                val = list(zip_iterator[i].values())
	                for j in range(len(res)):
	                    L.append("account_id: " + res[j] + ",\t\tmissed calls are: " + str(val[j]) + ",\t\tcompany_name: " + Tables[i])
	                L.append("\n")
	        except:
	            print("Except Handling")
	        lst = '\n'.join(L)
	        
	old_project = bc3.projects.get(project=22***)
	old_project.campfire.post_message(lst)
	new_message = old_project.message_board.post_message("Check this out", content=lst)

# Main function to run code
if __name__ == "__main__":

	# Create an instance of scheduler and add function.
	scheduler = BlockingScheduler()
	# wilshire_report_section()
	scheduler.add_job(BasecampMain , 'cron', hour=1, minute=40, jitter=500)
	#start the job
	scheduler.start()
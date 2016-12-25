from __future__ import print_function
import httplib2
import os, sys

import csv
import pymysql

import datetime

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None


SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'fusion-ops-data'

def get_credentials():

    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir, 'sheets.googleapis.com-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else:
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)

    return credentials


def main():

    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                            discoveryServiceUrl=discoveryUrl)
    spreadsheetId = '1b-8R-bYg9LAZliy_9dZOJFtJV8lTFVqNlhp1KdGMKPo'


    '''TEST RUN GET
    rangeName = 'sheet1!A1:C10'
    result = service.spreadsheets().values().get(spreadsheetId=spreadsheetId, range=rangeName).execute()
    print(type(result))
    values = result.get('values', [])
    for row in values:
        print(row)
    '''


    customers = []
    data = {}

    yesterday = datetime.date.fromordinal(datetime.date.today().toordinal()-1)
    #print(yesterday)

    try:
        #CONNECT TO MYSQL
        HOST = 'rm-gs59cfo26m7j59ov2o.mysql.singapore.rds.aliyuncs.com'
        db = pymysql.connect(HOST, 'fusion_read_only', 'D5techsdb%', 'sg-prod-fusion',charset='utf8')
        cursor = db.cursor()

        #UPDATE customers
        cursor.execute("SELECT * FROM `sg-prod-fusion`.auth_company;")
        for row in cursor:
            if row[1] not in customers:
                customers.append(row[1])
        #test
        #print(customers)

        #initiate data
        for item in customers:
            data[item] = {'login':0, 'cook':0, 'active':0}

        print(str(data))

        #body = {'values':}

        #fetch login data
        sql_login = "select distinct title from auth_company \
        inner join auth_user on auth_company.id = auth_user.company_id \
        where date(FROM_UNIXTIME(auth_user.last_login/1000)) = '2016-12-06';"
        cursor.execute(sql_login)
        for row in cursor:
            data[row[0]]['login'] =1

        #fetch cook data
        sql_cook = "select distinct title from auth_company \
        inner join auth_user on auth_company.id = auth_user.company_id \
        inner join cook_task on cook_task.user_id = auth_user.id \
        where date(FROM_UNIXTIME(cook_task.submit_time/1000)) = '2016-12-06';"
        cursor.execute(sql_cook)
        for row in cursor:
            data[row[0]]['cook'] =1
        print(str(data))

        #process active
        for customer in customers:
            if(data[customer]['login']==1 and data[customer]['cook']==1):
                data[customer]['active'] = 2
            elif(data[customer]['login']==1 or data[customer]['cook']==1):
                data[customer]['active'] = 1


        #print(str(data))
        values = []
        for key in data.keys():
            values.append([
            key,
            '2016-12-05',
            data[key]['login'],
            data[key]['cook'],
            data[key]['active']
            ])
        body = {'values': values}
        print(str(body))

        rangeName = 'sheet1!A1:E1'

        result = service.spreadsheets().values().append(
        spreadsheetId=spreadsheetId, range=rangeName,
        valueInputOption='USER_ENTERED', body=body).execute()


    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise




if __name__ == '__main__':
    main()

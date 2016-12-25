#encoding=utf-8
import csv
import pymysql
import sys
from datetime import timedelta, date

#print(sys.getdefaultencoding())

customers = []


#CONNECT TO MYSQL
HOST = 'rm-gs59cfo26m7j59ov2o.mysql.singapore.rds.aliyuncs.com'
db = pymysql.connect(HOST, 'fusion_read_only', 'D5techsdb%', 'sg-prod-fusion',charset='utf8')
cursor = db.cursor()

#UPDATE customers
cursor.execute("SELECT * FROM `sg-prod-fusion`.auth_company;")
for row in cursor:
    if row[1] not in customers:
        customers.append(row[1])

#fetch cook data
sql = "select distinct auth_company.title as company,\
    date(FROM_UNIXTIME(`sg-prod-fusion`.cook_task.finish_time/1000)) as active_date\
    from cook_task inner join auth_user on cook_task.user_id = auth_user.id \
    inner join auth_company on auth_company.id = auth_user.company_id\
    order by active_date;"

cursor.execute(sql)
data = cursor.fetchall()

#functions
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

print('check')


with open("data.csv", "w", newline="") as csvfile:
    fieldnames = ["Company", "Date", "Login", "Bake", "Active"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    login = 0
    bake = 0
    active = 0
    for company in customers:
        #print(company)
        for single_date in daterange(date(2016,11,12), date(2016,12,5)):
            #print(single_date)
            #print("cursor is %d" % len(data))
            if (company, single_date) in data:
                print(company, single_date, True)
                login = 1
                bake = 1
                active = 1
            else:
                print(company, single_date, False)
                #print(company, single_date, active)

            writer.writerow({
                'Company': company,
                "Date":str(single_date),
                "Login":login,
                "Bake":bake,
                "Active":active})

            login = 0
            bake = 0
            active = 0


            #print(row[0], type(row[0]))
            #print(row[1], type(row[1]))
            #print(single_date)
            #print(company)



'''




'''
cursor.close()
db.close()

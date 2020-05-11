import redis
import bs4
import csv
import xlrd
import mysql.connector
import re

# redis connection information. COnfigure accordingly
rhost = "localhost"
rport = 6379
rpwd = ""


# The following method finds the datasource type, retrieves data and initiates the KL constructor

def create_klstore(name, ds, qs, p1, p2, d):
    p1 = int(p1)
    p2 = int(p2)
    d = int(d)
    soup = bs4.BeautifulSoup(open(ds, "r").read(), features="xml")
    datasource = soup.find('datasource').text
    sourcetype = soup.find('datasource')['type']
    custlist = []
    translist = []
    if sourcetype == 'csv':
        path = soup.find('path').text
        filename = soup.find('filename').text
        fullfile = path + '/' + filename
        delimiter = soup.find('delimiter').text
        with open(fullfile, newline='') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=delimiter)
            for row in spamreader:
                custlist.append(row[p1])
                translist.append(row[p2])
    elif sourcetype == 'excel':
        qs = int(qs)
        path = soup.find('path').text
        filename = soup.find('filename').text
        fullfile = path + '/' + filename
        wb = xlrd.open_workbook(fullfile)
        sheet = wb.sheet_by_index(qs)
        for row in range(sheet.nrows):
            custlist.append(sheet.cell_value(row, p1))
            translist.append(sheet.cell_value(row, p2))
    # connection and retrieval of data from a mysql db
    elif sourcetype == 'db':
        mydb = mysql.connector.connect(
            host="localhost",
            user=soup.find('username').text,
            passwd=soup.find('password').text,
            database=soup.find('database').text,
        )
        mycursor = mydb.cursor()

        mycursor.execute(qs)
        myresult = mycursor.fetchall()
        for i in myresult:
            custlist.append(i[0])
            translist.append(i[1])
    # decode_response with the utf-8 charset is used to eliminate the need to manipulate byte streams returned by redis
    r = redis.Redis(host=rhost, port=rport, password=rpwd, decode_responses=True, charset='utf-8')
    for i in range(len(translist)):
        if d == 1:
            r.rpush(name+custlist[i], translist[i])
        elif d == 2:
            r.rpush(name+translist[i], custlist[i])
        else:
            print(type(d) + "this is the direction imported type. check it. otherwise, there's another issue")


#this method clears any data within Redis, when a clean start is desired
def redis_clear():
    # decode_response with the utf-8 charset is used to eliminate the need to manipulate byte streams returned by redis
    r = redis.Redis(host=rhost, port=rport, password=rpwd, decode_responses=True, charset='utf-8')
    r.flushdb()

def filter_klstore(name1, expression):
    r = redis.Redis(host=rhost, port=rport, password=rpwd, decode_responses=True, charset='utf-8')
    keys = r.keys(name1 + ":*")
    for key in keys:
        for value in r.lrange(key, 0, -1):
            if (eval(expression)):
                r.lrem(key, 0, value)
#Example functions that can be used in the apply_klstore method
def upper_wrapper(st):
    stt = st.upper()
    return stt

def lower_wrapper(st):
    stt = st.lower()
    return stt

def letter_stripper(st):
    stt=re.sub("[^0-9]", "", st)
    return stt




def apply_klstore(name1, func):
    r = redis.Redis(host=rhost, port=rport, password=rpwd, decode_responses=True, charset='utf-8')
    keys = r.keys(name1 + "*")
    for key in keys:
        i = 0
        for value in r.lrange(key, 0, -1):
            r.lset(key, i, eval(func+"(value)"))
            i += 1

def aggr_klstore(name1, aggr, func):
#So far, the values weren't strictly numerical.
#For the purpose of the exercise, if an aggregator is required, we will strip any non-numerical character in order to calculate it.
    r = redis.Redis(host=rhost, port=rport, password=rpwd, decode_responses=True, charset='utf-8')
    keys = r.keys(name1 + "*")
    for key in keys:
        values = r.lrange(key, 0, -1)
        nlist = [int(letter_stripper(i)) for i in values]
        if aggr == '':
            if func != '':
                r.rpush(key, func(nlist))
        elif aggr == 'avg':
            r.rpush(key, str(sum(nlist)/len(nlist)))
        elif aggr == 'max':
            r.rpush(key, str(max(nlist)))
        elif aggr == 'min':
            r.rpush(key, str(min(nlist)))
        elif aggr == 'sum':
            r.rpush(key, str(sum(nlist)))
        elif aggr == 'count':
            r.rpush(key, str(len(nlist)))

def lookup_klstore(name1, name2):
    r = redis.Redis(host=rhost, port=rport, password=rpwd, decode_responses=True, charset='utf-8')
    keys1 = r.keys(name1 + "*")
    keys2 = r.keys(name2 + "*")
    for key in keys1:
        for value in r.lrange(key, 0, -1):
            r.lrem(key, 1, value)
            for value2 in r.lrange(name2+value, 0, -1):
                r.rpush(key, value2)







#apply_klstore('cust3', 'upper_wrapper')
redis_clear()
create_klstore('csv1', 'csvsource.xml', None, 0, 1, 1)
create_klstore('csv2', 'csvsource.xml', None, 0, 1, 2)
create_klstore('excel1', 'excelsource.xml', 0, 0, 1, 2)
#create_klstore('db1', 'dbsource.xml', 'SELECT * FROM redistable', 0, 1, 1)

filter_klstore('csv2', 'len(value[4:]>1')
filter_klstore('csv2', 'value=="tran16"')

apply_klstore('csv2', 'upper_wrapper')
apply_klstore('csv2', 'letter_stripper')
#apply_klstore('csv2', 'lower_wrapper')

aggr_klstore('csv2', 'count', None)
aggr_klstore('csv2', 'avg', None)
aggr_klstore('csv2', 'count', None)

lookup_klstore('csv1', 'excel1')

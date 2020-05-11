import csv
import xlsxwriter
import mysql.connector
import random
"""The script creates a dataset in csv and excel, as well as creates a table (if not existing) and adds teh according data to a mysql DB.  
    It includes 1000 random pairs of 30 users and 30 transaction types, in order to examine the uniqueness of data input within redis (30*30 = 900) -> at least 100 duplicates
    WARNING: MySQL needs to be installed before testing, and connection data need to be correctly configured
"""
def datacreation(ch):
    data = []
    #data creation
    for i in range(0, 1000):
        custid = "cust" + str(random.randrange(30))
        transid = "tran" + str(random.randrange(30))
        data.append([custid, transid])

    #print(data[:5])
    #csv file creation
    with open('csvdata.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)

    #excel file creation
    row = 0
    col = 0
    workbook = xlsxwriter.Workbook('exceldata.xlsx')
    worksheet = workbook.add_worksheet()

    for customer, transaction in data:
        worksheet.write(row, col, customer)
        worksheet.write(row, col+1, transaction)
        row+= 1
    workbook.close()

    #connection to Database
    mydb = mysql.connector.connect(
      host="localhost",
      user="giannis",
      passwd="giannis",
      database="redis2"
    )
    if ch.upper() == 'Y':
        mycursor = mydb.cursor()
        #table creation
        mycursor.execute("DROP TABLE IF EXISTS redistable")
        mycursor.execute("CREATE TABLE redistable (customer VARCHAR(30), transaction VARCHAR(30))")
        #data insertion into table
        for i in data:
            insertion = "INSERT INTO redistable (customer, transaction) VALUES (%s, %s)"
            val = (i[0], i[1])
            mycursor.execute(insertion, val)

        mydb.commit()


datacreation('Y')
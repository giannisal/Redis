# Redis
## Goal
This repo is created to accomodate the project code for Redis data structure exploration.

## Contents
The repo is consisted of:
- csvdata.csv, exceldata.xlsx : files containing collections required as input for the code
- csvsource/excelsource/dbsource.xml - files containing information required for the creation of Key- List stores in redis
- datacreator.py : python file creating initial data in excel,csv,db-table format. (creator of the attached files as well)
- redis_methods.py : python file containing all the required functions of the assignment.
- handler.py : primitive cli that runs examples of the contained functionality of the above python files. Also provides limited custom query creation


## Requirements
Although moderate caution in the form of warnings has been iplemented, required in order for the application to run 
smoothly is a setup mysql database, as well as the appropriate configuration of its connection 
in the according files (dbsource.xml & datacreator.py). 
The same goes for the redis installation and configuration (configuration set up at redis_methods.py once, shared for all methods)

## Imported packages
These packages are used throughout the application, so any of these missing should be downloaded before running the application
- redis
- mysql.connector
- csv
- xlrd
- xlsxwriter
- re
- bs4

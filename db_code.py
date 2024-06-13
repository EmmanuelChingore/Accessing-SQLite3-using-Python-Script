#create database using python

import sqlite3
import pandas as pd

#initializing
conn = sqlite3.connect('STAFF.db')

#creating table

table_name = 'INSTRUCTOR' 
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

#read csv as dataframe using pandas
file_path = '~/Documents/IBM data engineering course/IBM Python for Data Engineering/SQLite3/INSTRUCTOR.csv'
df = pd.read_csv(file_path, names= attribute_list)

#loading table to db using pandas
df.to_sql(table_name, conn, if_exists= 'replace', index=False)
print('Table is ready')

###Running basic queries on data

#Viewing all data in the table
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

#Viewing only FNAME
query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

#Count the total number of entries
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

#Appending some data to the table
#creating new data to append
data_dict = {'ID': [100], 
             'FNAME': ['John'],
             'LNAME': ['Doe'],
             'CITY': ['Paris'],
             'CCODE': ['FR']}

data_append = pd.DataFrame(data_dict)  #saving data as dataframe table

#appending
data_append.to_sql(table_name, conn, if_exists= 'append', index=False)
print('data appended successfully')


#CREATING ANOTHER TABLE IN THE SAME db

#creating second table
table_two = 'Departments'
attribute_list = ['DEPT_ID', 'DEP_NAME','MANAGER_ID', 'LOC_ID']
file_path = '~/Documents/IBM data engineering course/IBM Python for Data Engineering/SQLite3/Departments.csv'
df = pd.read_csv(file_path, names=attribute_list) #new table as dataframe

#loading second table to db
df.to_sql(table_two, conn, if_exists= 'replace', index=False)
print('Table successfully loaded')

#Appending new data to the second table

data_dict = {'DEPT_ID': [9],
             'DEP_NAME': ['Quality Assurance'],
             'MANAGER_ID': [30010],
             'LOC_ID': ['L0010']}

data_append = pd.DataFrame(data_dict)

data_append.to_sql(table_two, conn, if_exists= 'append', index=False)
print('Data appended successfully')

#Viewing all data in the table
query_statement = f"SELECT * FROM {table_two}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

#Viewing only FNAME
query_statement = f"SELECT DEP_NAME FROM {table_two}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

#Count the total number of entries
query_statement = f"SELECT COUNT(*) FROM {table_two}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)
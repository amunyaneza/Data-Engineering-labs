# import libraries
import sqlite3
import pandas as pd

# connect to SQLIte3 using connect() function 
# pass the database name "STAFF" as an argument
conn = sqlite3.connect('STAFF.db')

# create table
table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

# read csv file using pandas
# use keys of attribute_dict dictionary as a list to assign headers
file_path = '/home/project/Project3/INSTRUCTOR.csv'
df = pd.read_csv(file_path, names = attribute_list)

# load the table
# replace existing table in the database with the same name
df.to_sql(table_name, conn, if_exists = 'replace', index =False)
print('Table is ready')

# View all data in the table
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# View only FNAME column of data
query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# View the total number of entries in the table
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# append some data to the table
# create dataframe of new data
data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}
data_append = pd.DataFrame(data_dict)

# append the data to the INSTRUCTOR table
data_append.to_sql(table_name, conn, if_exists = 'append', index =False)
print('Data appended successfully')

# close connection to the database after all queries are executed
conn.close()
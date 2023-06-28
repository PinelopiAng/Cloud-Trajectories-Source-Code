# Databricks notebook source
# MAGIC %md
# MAGIC ## Full Initial Data Ingestion
# MAGIC ###### The purpose of this first notebook is the ingestion and pre-processing of the initial data. Additionally, we set up connections to a PostgreSQL Database hosted on Azure Cloud, in order to execute SQL queries. The most basic among them, are the queries which create and the queries which populate the database's tables.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### [1] Mount our Container

# COMMAND ----------

# Checking if mount already exists
mnts = dbutils.fs.mounts()
mnt_exists = False
for mount in mnts:
    if mount.mountPoint == "/mnt/historical":
        mnt_exists = True

if mnt_exists == False:
    # Setup some parameters and keys
    account_name = "trajectoriesstorage"
    container = "historical"
    access_key = dbutils.secrets.get(scope="key-vault-connect", key="storage-key")

    # Define the connection configurations
    configs = {
        "fs.azure.account.auth.type": "key",
        "fs.azure.account.key."+account_name+".blob.core.windows.net": access_key
    }

    # Command to mount the blob storage container locally
    dbutils.fs.mount(
    source = f"wasbs://{container}@{account_name}.blob.core.windows.net",
    mount_point = "/mnt/historical",
    extra_configs = configs)
else:
    print("Mount already exists.")


# COMMAND ----------

# MAGIC %md
# MAGIC ### [2] Checking file sizes

# COMMAND ----------

directory = "/dbfs/mnt/historical"

import os
ListofFiles = []
for file in os.listdir(directory):
    with open(os.path.join(directory, file), 'r') as f:
        line_count = 0
        for line in f:
            line_count += 1
        #print(line_count)
        if line_count >= 4:
            ListofFiles.append(file)

print(len(ListofFiles))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### [3] Loading files into dataframe + adding CloudID

# COMMAND ----------

import pandas as pd
import re

from pandas.errors import ParserError

# Create an empty list to hold the dataframes
dfs = []

# Create a list to hold all the bad txts that need handling
bad_txts = []

# Initialize a CloudIDs dict
DictionaryofIDs = {}

# Iterate over the files in the directory and load each file into a dataframe
for file in ListofFiles:
    
    path = os.path.join(directory, file)
    
    try:
        new_df = pd.read_csv(path, header = None, sep =" ")
        
        num_columns = len(new_df.columns)
        
        if num_columns > 156:
            bad_txts.append(file)
            
            
        else:
            
            
                # Include Cloud ID
                # Extract the starting timestamp and ID as the CloudID
                
                match = re.search(r"-([^-]*)-", file)
                result =  match.group(1)
                result =  result.replace('.', '')
                CloudID = int(result)
            
                # Create a CloudID column
                
                DictionaryofIDs[file] = CloudID
                rows =  len(new_df) # Count the rows of the file
                cloud_id_list = [CloudID]*rows  # Create a list of the same number (the cloud ID) with #rows elements
                new_df["CloudID"] = cloud_id_list # Assign a new column in the dataframe with the CloudID
               
            
                dfs.append(new_df)
                
            
    except ParserError:
        bad_txts.append(file)

# Concatenate the dataframes into a single dataframe
CombinedDataframe = pd.concat(dfs, ignore_index=True)       

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### [4] Renaming some columns and adding timestamps

# COMMAND ----------

# Rename the year, month, day, hour, minute columns to create a single timestamp column
CombinedDataframe.rename(columns = {144: "year", 145: "month", 146: "day", 147: "hour", 148: "minute"}, inplace = True)

# More renames
CombinedDataframe.rename(columns = {0: "ID"}, inplace = True)
CombinedDataframe.rename(columns = {1: "Area_Size", 2: "Xg_Cloud", 3: "Yg_Cloud", 10: "T_Mean_B5", 11: "T_Mean_B6", 12: "T_Mean_B7", 13: "T_Mean_B9", 14: "T_Mean_B10", 15: "T_Min_B5", 16: "T_Min_B6", 17: "T_Min_B7", 18: "T_Min_B9", 19: "T_Min_B10", 20: "T_Mode_B5", 21: "T_Mode_B6", 22: "T_Mode_B7", 23: "T_Mode_B9", 24: "T_Mode_B10", 149: "M_S_Symbol", 150: "d_area", 151: "d_tempC10_B5", 152: "d_tempC10_B9", 153: "d_tempC50_B5", 154: "d_tempC50_B9", 155: "Skew_B9"}, inplace = True)

# Add the timestamp column
CombinedDataframe["Timestamp"] = pd.to_datetime(CombinedDataframe[["year", "month", "day", "hour", "minute"]])

# Drop the column with the typos
CombinedDataframe.drop([28], axis=1, inplace=True)

# Replace the symbols ## with 0 for initial rate of change value
CombinedDataframe.replace("##",0.0, inplace=True)

# COMMAND ----------

# We keep only specific columns of the CombinedDataframe
FinalDataframe = CombinedDataframe[["ID", "Area_Size", "Xg_Cloud", "Yg_Cloud", "T_Mean_B5", "T_Mean_B6","T_Mean_B7", "T_Mean_B9", "T_Mean_B10", "T_Min_B5", "T_Min_B6", "T_Min_B7", "T_Min_B9", "T_Min_B10", "T_Mode_B5", "T_Mode_B6", "T_Mode_B7", "T_Mode_B9", "T_Mode_B10", "M_S_Symbol", "d_area", "d_tempC10_B5", "d_tempC10_B9", "d_tempC50_B5", "d_tempC50_B9", "Skew_B9", "CloudID", "Timestamp"]].copy()

# COMMAND ----------

FinalDataframe

# COMMAND ----------

# Create a dataframe for the CloudIDs table as well
IDs_df = pd.DataFrame({"Filenames": list(DictionaryofIDs.keys()), "CloudID": list(DictionaryofIDs.values())})

# COMMAND ----------

IDs_df

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### [5] Creating the SQL Database
# MAGIC
# MAGIC First, we establish a connection to the default database setup while setting up the Azure PostgreSQL service, which is called `postgres`. From there, we create our own database, to be used for the purposes of the project. Note, that this can be done in a very simple way using the Azure Interface. If you choose to add the new database using the Azure Interface, you can skip this part.

# COMMAND ----------

import psycopg2

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Set parameters for initial connection to new-built database server
host = 'postgresbase-trajectories-server.postgres.database.azure.com'
database = 'postgres'
user = 'cloudadmin'
password = dbutils.secrets.get(scope="key-vault-connect", key="postgres-password")
port = '5432'
sslmode = 'require'

# Connect to the PostgreSQL server
conn_string = f"host={host} user={user} dbname={database} password={password} sslmode={sslmode}"
conn = psycopg2.connect(conn_string)

# We have to add this here
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

# Open a cursor to perform database operations
cur = conn.cursor()

# COMMAND ----------

cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", ('clouddb',))

fetched = cur.fetchone()

if fetched:
    print("clouddb database already constructed.")
else:
    # Query to create a new database
    query_create = """CREATE DATABASE clouddb
                    ENCODING 'UTF8'
                """

    # Execute the query
    cur.execute(query_create)

# Close the cursor and connection
cur.close()
conn.close()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### [6] Establish a connection to the new database and create tables

# COMMAND ----------

# Set parameters for connection to new-built database server
host = 'postgresbase-trajectories-server.postgres.database.azure.com'
database = 'clouddb'
user = 'cloudadmin'
password = dbutils.secrets.get(scope="key-vault-connect", key="postgres-password")
port = '5432'
sslmode = 'require'

# Connect to the PostgreSQL server
conn_string = f"host={host} user={user} dbname={database} password={password} sslmode={sslmode}"
conn = psycopg2.connect(conn_string)

# We have to add this here
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

# Open a cursor to perform database operations
cur = conn.cursor()

# COMMAND ----------

# Query to create filenames-cloudid database table
table1_query = """CREATE TABLE cloudids (
                    filenames TEXT,
                    cloudid BIGINT
                )"""

# Execute this query
cur.execute(table1_query)

# COMMAND ----------

# Query to create the whole dataset's database table
table2_query = """CREATE TABLE dataset (
                    id INTEGER,
                    area_size INTEGER,
                    xg_cloud REAL,
                    yg_cloud REAL,
                    t_mean_b5 REAL,
                    t_mean_b6 REAL,
                    t_mean_b7 REAL,
                    t_mean_b9 REAL,
                    t_mean_b10 REAL,
                    t_min_b5 REAL,
                    t_min_b6 REAL,
                    t_min_b7 REAL,
                    t_min_b9 REAL,
                    t_min_b10 REAL,
                    t_mode_b5 REAL,
                    t_mode_b6 REAL,
                    t_mode_b7 REAL,
                    t_mode_b9 REAL,
                    t_mode_b10 REAL,
                    m_s_symbol VARCHAR(5),
                    d_area REAL,
                    d_tempc10_b5 REAL,
                    d_tempc10_b9 REAL,
                    d_tempc50_b5 REAL,
                    d_tempc50_b9 REAL,
                    skew_b9 REAL,
                    cloudid BIGINT,
                    timestamp TIMESTAMP
                )"""

# Execute this query
cur.execute(table2_query)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### [7] Populate the tables with the dataframes data

# COMMAND ----------

# Make the list containing tuples (Filenames, CloudID)
cloudidsdata = []

for index, row in IDs_df.iterrows():
    cloudidsdata.append((row['Filenames'], row['CloudID']))
    

# Query to insert
query_to_insert = """INSERT INTO cloudids (filenames, cloudid) VALUES (%s,%s)"""

# Query to execute
cur.executemany(query_to_insert, cloudidsdata)

# COMMAND ----------

# Make the list containing tuples (.......)
datasetdata = []

for index, row in FinalDataframe.iterrows():
    datasetdata.append((row['ID'], row['Area_Size'], row['Xg_Cloud'], row['Yg_Cloud'], row["T_Mean_B5"], row["T_Mean_B6"], row["T_Mean_B7"], row["T_Mean_B9"], row["T_Mean_B10"], row["T_Min_B5"], row["T_Min_B6"], row["T_Min_B7"], row["T_Min_B9"], row["T_Min_B10"], row["T_Mode_B5"], row["T_Mode_B6"], row["T_Mode_B7"], row["T_Mode_B9"], row["T_Mode_B10"], row["M_S_Symbol"], row["d_area"], row["d_tempC10_B5"], row["d_tempC10_B9"], row["d_tempC50_B5"], row["d_tempC50_B9"], row["Skew_B9"], row['CloudID'], row['Timestamp']))
    

# Query to insert
query_to_insert = """INSERT INTO dataset (id, area_size, xg_cloud, yg_cloud, t_mean_b5, t_mean_b6, t_mean_b7, t_mean_b9, t_mean_b10, t_min_b5, t_min_b6, t_min_b7, t_min_b9, t_min_b10, t_mode_b5, t_mode_b6, t_mode_b7, t_mode_b9, t_mode_b10, m_s_symbol, d_area, d_tempc10_b5, d_tempc10_b9, d_tempc50_b5, d_tempc50_b9, skew_b9, cloudid, timestamp) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

# Query to execute
cur.executemany(query_to_insert, datasetdata)

# COMMAND ----------

# Close the cursor and database connection
cur.close()
conn.close()

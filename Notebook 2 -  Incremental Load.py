# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Load (New Data Ingestion)
# MAGIC
# MAGIC The purpose of this second notebook is the ingestion and pre-processing of any newly arriving data. Once the data are properly pre-processed, they are appended to the corresponding table of the PostgreSQL database.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### [1] Mount our Container

# COMMAND ----------

# Checking if mount already exists
mnts = dbutils.fs.mounts()
mnt_exists = False
for mount in mnts:
    if mount.mountPoint == "/mnt/ingestion":
        mnt_exists = True

if mnt_exists == False:
    # Setup some parameters and keys
    account_name = "trajectoriesstorage"
    container = "ingestion"
    access_key = dbutils.secrets.get(scope="key-vault-connect", key="storage-key")

    # Define the connection configurations
    configs = {
        "fs.azure.account.auth.type": "key",
        "fs.azure.account.key."+account_name+".blob.core.windows.net": access_key
    }

    # Command to mount the blob storage container locally
    dbutils.fs.mount(
    source = f"wasbs://{container}@{account_name}.blob.core.windows.net",
    mount_point = "/mnt/ingestion",
    extra_configs = configs)
else:
    print("Mount already exists.")


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### [2] Checking file size

# COMMAND ----------

# Add a field for a parameter that checks a given filename
dbutils.widgets.text("file_name", "")

# COMMAND ----------

import os

directory = "/dbfs/mnt/ingestion"

filename = dbutils.widgets.get("file_name")

path = os.path.join(directory, filename)

goodfile = False
    
with open(path, 'r') as file:
    num_lines = sum(1 for line in file)
    if num_lines >= 4:
        goodfile = True

# COMMAND ----------

goodfile

# COMMAND ----------

# MAGIC %md
# MAGIC ### [3] Loading the contents in a dataframe and uploading them to the cloud

# COMMAND ----------

if goodfile == True:

    import pandas as pd
    import re

    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

    from pandas.errors import ParserError
    
    try:
        df = pd.read_csv(path, sep=" ", header=None)
        num_columns = len(df.columns)    
        if num_columns == 156:
            # Include Cloud ID
            # Extract the starting timestamp and ID as the CloudID
            match = re.search(r"-([^-]*)-", filename)
            result = match.group(1)
            result = result.replace('.', '')
            CloudID = int(result)
            # Create a CloudID column
            rows = len(df) # Count the rows of the file
            cloud_id_list = [CloudID]*rows # Create a list of the same number (the cloud ID) with #rows elements
            df["CloudID"] = cloud_id_list # Assign a new column in the dataframe with the CloudID

            # Rename the year, month, day, hour, minute columns to create a single timestamp column
            df.rename(columns={144: "year", 145: "month", 146: "day", 147: "hour", 148:"minute"}, inplace=True)

            # More renames
            df.rename(columns = {0: "ID"}, inplace = True)
            df.rename(columns = {1: "Area_Size", 2: "Xg_Cloud", 3: "Yg_Cloud", 10: "T_Mean_B5", 11: "T_Mean_B6", 12: "T_Mean_B7", 13: "T_Mean_B9", 14: "T_Mean_B10", 15: "T_Min_B5", 16: "T_Min_B6", 17: "T_Min_B7", 18: "T_Min_B9", 19: "T_Min_B10", 20: "T_Mode_B5", 21: "T_Mode_B6", 22: "T_Mode_B7", 23: "T_Mode_B9", 24: "T_Mode_B10", 149: "M_S_Symbol", 150: "d_area", 151: "d_tempC10_B5", 152: "d_tempC10_B9", 153: "d_tempC50_B5", 154: "d_tempC50_B9", 155: "Skew_B9"}, inplace = True)

            # Add the timestamp column
            df["Timestamp"] = pd.to_datetime(df[['year', 'month', 'day', 'hour', 'minute']])

            # Drop the column with the typos
            df.drop([28], axis=1, inplace=True)

            # Replace the symbols ## with 0 for initial rate of change value
            df.replace("##",0.0, inplace=True)

            final_df = df[["ID", "Area_Size", "Xg_Cloud", "Yg_Cloud", "T_Mean_B5", "T_Mean_B6","T_Mean_B7", "T_Mean_B9", "T_Mean_B10", "T_Min_B5", "T_Min_B6", "T_Min_B7", "T_Min_B9", "T_Min_B10", "T_Mode_B5", "T_Mode_B6", "T_Mode_B7", "T_Mode_B9", "T_Mode_B10", "M_S_Symbol", "d_area", "d_tempC10_B5", "d_tempC10_B9", "d_tempC50_B5", "d_tempC50_B9", "Skew_B9", "CloudID", "Timestamp"]].copy()

            print("Successfully created a dataframe out of the newly appended text file.")

            # Set parameters for initial connection to new-built database server
            host = 'postgresbase-trajectories-server.postgres.database.azure.com'
            database = 'clouddb'
            user = 'cloudadmin'
            password = dbutils.secrets.get(scope="key-vault-connect", key="postgres-password")
            sslmode = 'require'

            # Connect to the PostgreSQL server
            conn_string = f"host={host} user={user} dbname={database} password={password} sslmode={sslmode}"
            conn = psycopg2.connect(conn_string)

            # We have to add this here
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            # Open a cursor to perform database operations
            cur = conn.cursor()

            print("Successfully connected to the database.")

            # query to insert the new filename-CloudID correspondence
            query_to_insert = f"INSERT INTO cloudids (filenames, cloudid) VALUES ('{filename}', {CloudID});"

            # execute the query
            cur.execute(query_to_insert)

            print("Appended the new CloudID into the cloudids table.")

            # Make the list containing tuples (...)
            dfdata = []

            for index, row in final_df.iterrows():
                dfdata.append((row['ID'], row['Area_Size'], row['Xg_Cloud'], row['Yg_Cloud'], row["T_Mean_B5"], row["T_Mean_B6"], row["T_Mean_B7"], row["T_Mean_B9"], row["T_Mean_B10"], row["T_Min_B5"], row["T_Min_B6"], row["T_Min_B7"], row["T_Min_B9"], row["T_Min_B10"], row["T_Mode_B5"], row["T_Mode_B6"], row["T_Mode_B7"], row["T_Mode_B9"], row["T_Mode_B10"], row["M_S_Symbol"], row["d_area"], row["d_tempC10_B5"], row["d_tempC10_B9"], row["d_tempC50_B5"], row["d_tempC50_B9"], row["Skew_B9"], row['CloudID'], row['Timestamp']))
                
            # query to insert
            query_to_insert = """INSERT INTO dataset (id, area_size, xg_cloud, yg_cloud, t_mean_b5, t_mean_b6, t_mean_b7, t_mean_b9, t_mean_b10, t_min_b5, t_min_b6, t_min_b7, t_min_b9, t_min_b10, t_mode_b5, t_mode_b6, t_mode_b7, t_mode_b9, t_mode_b10, m_s_symbol, d_area, d_tempc10_b5, d_tempc10_b9, d_tempc50_b5, d_tempc50_b9, skew_b9, cloudid, timestamp) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

            # execute the query
            cur.executemany(query_to_insert, dfdata)

            # Close the cursor and database connection
            cur.close()
            conn.close()
            print("Appended the new data into the dataset table.")
        else:
            print("The file is bad: detected more/less than 156 columns.")

    except ParserError:
        print("The file is bad: some lines have a different number of columns than others.")

else:
    print("The file is bad: it contains less than 4 lines.")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



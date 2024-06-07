'''
=================================================
Final Project

Nama  Team : Team 3
Anggota    : - Maulana Muhamad Priadhi 		(BTP23120610)
             - Mangara Haposan Immanuel S. 	(BTP24010818)
             - Santriana Pratama 			(BTP24010821)
             - Yohana Tambunan 			    (BTP24010823)
             - Taufiqurrahman 			    (BTP24010824)

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL. Adapun dataset yang dipakai adalah dataset mengenai data pasien rumah sakit.
=================================================
'''

from airflow import DAG
from datetime import timedelta, datetime
import psycopg2 as db
import pandas as pd
import os
import csv

from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'ucup',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


# setup configuration to postgresql database
conn_string = "dbname='final_project' host='host.docker.internal' user='postgres' password='2023'"    


## Fungsi connect to GCS tidak digunakan karena tim tidak memiliki akun GCP
# # function to extract table from posgressql local
# def sql_extract():
#     # connect to database using psycopg2.connect
#     conn = db.connect(conn_string)
#     # create cur object to execute SQL commands
#     cur = conn.cursor()
#     try:
#         sql = """SELECT table_name 
#                  FROM information_schema.tables 
#                  WHERE table_schema = 'public' AND table_name IN ('DimSalesTerritory')"""
#         cur.execute(sql)
#         df = pd.DataFrame(cur.fetchall(), columns=['table_name'])
#         print(df)
#         tbl_dict = df.to_dict('dict')
#         cur.close()
#         conn.close()
#         return tbl_dict
#     except Exception as e:
#         print("Data extract error: " + str(e))


# def gcp_load(tbl_dict: dict):
#     # connect to database using psycopg2.connect
#     conn = db.connect(conn_string)
#     # create cur object to execute SQL commands
#     cur = conn.cursor()
#     try:
#         credentials = service_account.Credentials.from_service_account_file('/home/airflow/dotted-music-357620-02b4a53537b7.json')
#         project_id = "your-gcp-account-id"
#         dataset_ref = "AdventureWorks"
#         for value in tbl_dict.values():
#             val = value.values()
#             for v in val:
#                 rows_imported = 0
#                 sql = f'SELECT * FROM {v}'
#                 df = pd.read_sql_query(sql, conn)
#                 print(f'Importing rows {rows_imported} to {rows_imported + len(df)}... for table {v}')
#                 df.to_gbq(destination_table=f'{dataset_ref}.src_{v}', project_id=project_id, credentials=credentials, if_exists="replace")
#                 rows_imported += len(df)
#         cur.close()
#         conn.close()
#     except Exception as e:
#         print("Data load error: " + str(e))


# function to create table stock obat
def create_table_stock_obat():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to create table stock obat
        sql_stock="""
                create table if not exists drug_stock (
                    id serial primary key,
                    date_buy date not null,
                    drugs varchar(50) not null,
                    quantity varchar(50) not null,
                    branch varchar(10) not null,
                    call_date timestamp
                );
            """
        # execute the sql command
        cur.execute(sql_stock)
    
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to create table admission
def create_table_admission():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to create table admission
        sql_adm="""
                create table if not exists admission (
                    id int,
                    date_in date,
                    date_out date,
                    branch varchar(10),
                    fullname varchar(100),
                    age varchar(50),
                    gender varchar(50),
                    hospital_care varchar(50),
                    room varchar(10),
                    doctor varchar(50),
                    surgery varchar(10),
                    lab varchar(50),
                    drug_types varchar(50),
                    drug_brands varchar(50),
                    drug_qty varchar(10),
                    food varchar(50),
                    admin_cost varchar(50),
                    cogs varchar(50),
                    payment varchar(50),
                    review varchar(50),
                    call_date timestamp,
                    primary key(id)
                );
            """
        # execute the sql command
        cur.execute(sql_adm)
    
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to create table logs
def create_table_logs():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to create table logs
        sql_logs = """
                create table if not exists logs (
                    id serial primary key,
                    call_date timestamp default current_timestamp,
                    error_message text
                );
            """
        # execute the sql command
        cur.execute(sql_logs)

    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to insert data to table stock obat
def insert_data_stock_obat():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # path of csv file
        csv_file_stock = os.path.join(os.path.dirname(__file__), 'Drugs_data1.csv')
        # read the csv file into dataframe
        df_stock = pd.read_csv(csv_file_stock, delimiter=';')
        # iterate over each row in dataframe
        for index, row in df_stock.iterrows():
            # get current timestamp
            call_date = datetime.now()
            # sql command to insert data into drug_stock table
            insert_sql_stock = "INSERT INTO drug_stock (date_buy, drugs, quantity, branch, call_date) VALUES (%s, %s, %s, %s, %s);"
            # define the value to be inserted into the table
            values_stock = (row['Date'], row['Drugs'], row['Qty'], row['Branch'], call_date)
            # execute the sql command with its value
            cur.execute(insert_sql_stock, values_stock)
        # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to insert data to table admission
def insert_data_admission():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # path of csv file
        csv_file_adm = os.path.join(os.path.dirname(__file__), 'Hospital_data.csv')
        # read the csv file into dataframe
        df_adm = pd.read_csv(csv_file_adm, delimiter=';') 
        # iterate over each row in dataframe
        for index, row in df_adm.iterrows():
            # get current timestamp
            call_date = datetime.now()
            # sql command to insert data into admission table
            insert_sql_adm = "INSERT INTO admission (id, date_in, date_out, branch, fullname, age, gender, hospital_care, room, doctor, surgery, lab, drug_types, drug_brands, drug_qty, food, admin_cost, cogs, payment, review, call_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            # define the value to be inserted into the table
            values_adm = (row['ID'], row['Date IN'], row['Date OUT'], row['Branch'], row['Name'], row['Age'], row['Gender'], row['Hospital Care'], row['Room'], row['Doctor'], row['Surgery'], row['Lab'], row['Drug Types'], row['Drug Brands'], row['Drug Qty'], row['Food'], row['Admin'], row['COGS'], row['Payment'], row['Review'], call_date)
            # execute the sql command with its value
            cur.execute(insert_sql_adm, values_adm)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to clean the data
def data_cleaning():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # Execute a query to select all data from the 'admission' table
        cur.execute("SELECT * FROM admission;")
        # Get the column names from the query result
        columns = [desc[0] for desc in cur.description]
        # Fetch all rows and create a pandas DataFrame with the data
        df = pd.DataFrame(cur.fetchall(), columns=columns)

        # Check missing value
        if df.isnull().values.any():
            # fill missing values with '-'
            df.fillna(value='-', inplace=True)
        else:
            # print message if there are no missing value
            print("Data doesnt have missing value")

        # replace all '-' with 0
        df.replace('-', '0', inplace=True)

        # replace "kusus" with "khusus"
        df['surgery'] = df['surgery'].replace('Kusus', 'Khusus')

        # clean the unnecessary from currency value in food, admin_cost, and cogs column
        df['food'] = df['food'].str.replace('Rp.', '').str.replace(',', '')
        df['admin_cost'] = df['admin_cost'].str.replace('Rp.', '').str.replace(',', '')
        df['cogs'] = df['cogs'].str.replace('Rp.', '').str.replace(',', '')

        # replace 'pria' with 'laki - laki' in the 'Gender' column
        df['gender'] = df['gender'].apply(lambda x: 'laki - laki' if x.lower() == 'pria' else x)

        # replace 'wanita' with 'perempuan' in the 'Gender' column
        df['gender'] = df['gender'].apply(lambda x: 'perempuan' if x.lower() == 'wanita' else x)

        # sort date-in ascending
        df = df.sort_values('date_in')

        # define the set of columns to update
        columns_to_update = ['date_in', 'date_out', 'branch', 'fullname', 'age', 'gender',
                              'hospital_care', 'room', 'doctor', 'surgery', 'lab', 'drug_types',
                              'drug_brands', 'drug_qty', 'food', 'admin_cost', 'cogs', 'payment', 
                              'review']

        # check duplicate value
        duplicate_rows = df.duplicated()

        # Drop the first duplicate row if any
        if duplicate_rows.any():
            df.drop_duplicates(inplace=True)

            # Construct the set of columns for the WHERE clause
            where_clause = " AND ".join([f"{column} = %s" for column in columns_to_update])
            where_values = [row[column] for column in columns_to_update]

            # Construct the DELETE query
            delete_sql_adm = f"""
                DELETE FROM admission
                WHERE id NOT IN (
                    SELECT MIN(id)
                    FROM admission
                    GROUP BY {', '.join(columns_to_update)}
                );
            """
            cur.execute(delete_sql_adm)
        
        # aa
        for index, row in df.iterrows():
            # Construct the SET clause using the columns_to_update set
            set_clause = ", ".join([f"{column} = %s" for column in columns_to_update])

            # Construct the update query
            update_sql_adm = f"""
                UPDATE admission
                SET {set_clause}
                WHERE id = %s;
            """

            # Get the values from the row, including the id
            values = [row[column] for column in columns_to_update]
            values.append(row['id'])

            # Execute the update query
            cur.execute(update_sql_adm, values)

    # log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()
        

# function to convert data type
def convert_datatype():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # define the columns to convert
        columns_to_convert = ['age', 'drug_qty', 'food', 'admin_cost', 'cogs']

        # convert columns to integers
        for column in columns_to_convert:
            cur.execute(f"ALTER TABLE admission ALTER COLUMN {column} TYPE int USING {column}::integer")

    # logs the error to logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to create table doctor
def create_table_doctor():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to create table doctor
        sql_doc ="""
                create table doctor (
                    id serial primary key,
                    name varchar(50) not null
                );
            """
        # execute the sql command
        cur.execute(sql_doc)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to create table payment
def create_table_payment():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to create table payment
        sql_pay ="""
                create table payment (
                    id serial primary key,
                    payment_type varchar(50) not null
                );
            """
        # execute the sql command
        cur.execute(sql_pay)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to create table review
def create_table_review():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to create table review
        sql_rev ="""
                create table review (
                    id serial primary key,
                    review varchar(50) not null
                );
            """
        # execute the sql command
        cur.execute(sql_rev)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to create table review
def create_table_review():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to create table review
        sql_rev ="""
                create table review (
                    id serial primary key,
                    review varchar(50) not null
                );
            """
        # execute the sql command
        cur.execute(sql_rev)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to create table drug category
def create_table_cat_drug():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to create table review
        sql_cat ="""
                create table drug_category (
                    id serial primary key,
                    category varchar(50) not null
                );
            """
        # execute the sql command
        cur.execute(sql_cat)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to create table lab
def create_table_lab():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to create table lab
        sql_lab ="""
                create table lab(
                    id serial primary key,
                    lab_name varchar(50) not null
                );
            """
        # execute sql command
        cur.execute(sql_lab)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to create table drug
def create_table_drug():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to create table drugs
        sql_drug ="""
                create table drugs(
                    id serial primary key,
                    drug_name varchar(50) not null,
                    drug_type varchar(50) not null
                );
            """
        # execute sql command
        cur.execute(sql_drug)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()
    

# function to create table branch
def create_table_branch():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to create table branch
        sql_branch ="""
                create table branch (
                    id serial primary key,
                    branch_name varchar(10) not null
                );
            """
        # execute sql command
        cur.execute(sql_branch)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()
    

# function to create table surgery
def create_table_surgery():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to create table surgery
        sql_surg ="""
                create table surgery (
                    id serial primary key,
                    surgery_type varchar(50) not null
                );
            """
        # execute sql command
        cur.execute(sql_surg)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()
   

# function to create table patient
def create_table_patient():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to create table patient
        sql_pat ="""
                create table patient (
                    id serial primary key,
                    patient_name varchar(50) not null,
                    gender varchar(50) not null,
                    age int not null
                );
            """
        # execute sql command
        cur.execute(sql_pat)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()
 

# function to create table type
def create_table_type():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to create table room type
        sql_type ="""
                create table room_type (
                    id serial primary key,
                    room_type varchar(50) not null,
                    food_price int not null
                );
            """
        # execute sql command
        cur.execute(sql_type)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to create table hosp_care
def create_table_hoscare():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to create table hosp_care
        sql_hosc ="""
                create table hosp_care (
                    id serial primary key,
                    hospital_care varchar(50) not null
                );
            """
        # execute sql command
        cur.execute(sql_hosc)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()
    

# function to insert data into table payment
def insert_table_payment():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to insert into table payment
        sql_ins_pay ="""
                insert into payment (payment_type)
                select distinct payment
                from admission;
            """
        # execute sql command
        cur.execute(sql_ins_pay)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to insert data into table review
def insert_table_review():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to insert into table review
        sql_ins_rev ="""
                insert into review (review)
                select distinct review
                from admission;
            """
        # execute sql command
        cur.execute(sql_ins_rev)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to insert data into table drug category
def insert_table_cat_drug():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to insert into table drug category
        sql_ins_cat ="""
                insert into drug_category (category)
                select distinct drug_types
                from admission;
            """
        # execute sql command
        cur.execute(sql_ins_cat)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()



# function to insert data into table doctor
def insert_table_doctor():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to insert into table doctor
        sql_ins_doc ="""
                insert into doctor (name)
                select distinct doctor
                from admission;
            """
        # execute sql command
        cur.execute(sql_ins_doc)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()
   

# function to insert data into table lab
def insert_table_lab():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to insert into table lab
        sql_ins_lab ="""
                insert into lab (lab_name)
                select distinct lab
                from admission
                where lab != '0';
            """
        # execute sql command
        cur.execute(sql_ins_lab)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()
    

# function to insert data into table drugs
def insert_table_drug():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to insert data into table drugs
        sql_ins_drug ="""
                insert into drugs (drug_name, drug_type)
                select distinct drug_brands, drug_types
                from admission;
            """
        # execute sql command
        cur.execute(sql_ins_drug)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()
    

# function to insert data into table branch
def insert_table_branch():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to insert data into table branch
        sql_ins_branch ="""
                insert into branch (branch_name)
                select distinct branch
                from admission;
            """
        # execute sql command
        cur.execute(sql_ins_branch)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()
    

# function to insert data into table surgery
def insert_table_surgery():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to insert data into table surgery
        sql_ins_surg ="""
                insert into surgery (surgery_type)
                select distinct surgery
                from admission
                where surgery != '0';
            """
        # execute sql command
        cur.execute(sql_ins_surg)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()
    

# function to insert data into table patient
def insert_table_patient():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql commmand to insert data into table patient
        sql_ins_patient ="""
                insert into patient (patient_name, gender, age)
                select distinct fullname, gender, age
                from admission;
            """
        # execute sql command
        cur.execute(sql_ins_patient)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()
    

# function to insert data into table room type
def insert_table_room_type():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to insert data into table room type
        sql_ins_rt ="""
                insert into room_type (room_type, food_price)
                select distinct room, food
                from admission
                where room != '0';
            """
        # execute sql command
        cur.execute(sql_ins_rt)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to insert data into table hosp_care
def insert_table_hoscare():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to insert data into table hosp_care
        sql_ins_hosc ="""
                insert into hosp_care (hospital_care)
                select distinct hospital_care
                from admission;
            """
        # execute sql command
        cur.execute(sql_ins_hosc)
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to update payment column to payment id in admission table
def update_payment_id():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to update admission table
        sql_upt_pay = """
                update admission as a
                set payment = pay.id
                from payment as pay
                where a.payment = pay.payment_type;
            """
        # execute sql command
        cur.execute(sql_upt_pay)

    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to change payment column into id_payment
def alter_payment_id():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to change column name
        sql_alt_pay = """
                alter table admission
                rename column payment to id_payment;
            """
        # execute sql command
        cur.execute(sql_alt_pay)

    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to update review column to review id in admission table
def update_review_id():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to update admission table
        sql_upt_rev = """
                update admission as a
                set review = rev.id
                from review as rev
                where a.review = rev.review;
            """
        # execute sql command
        cur.execute(sql_upt_rev)

    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to change review column into id_payment
def alter_review_id():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to change column name
        sql_alt_rev = """
                alter table admission
                rename column review to id_review;
            """
        # execute sql command
        cur.execute(sql_alt_rev)

    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to update review column to review id in admission table
def update_cat_id():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to update admission table
        sql_upt_cat = """
                update drugs
                set drug_type = cat.id
                from drug_category as cat
                where drugs.drug_type = cat.category;
            """
        # execute sql command
        cur.execute(sql_upt_cat)

    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to change drug_type column into id_category
def alter_category_id():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to change column name
        sql_alt_cat = """
                alter table drugs
                rename column drug_type to id_category;
            """
        # execute sql command
        cur.execute(sql_alt_cat)

    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()
    

# function to update doctor name column to doctor id in admission table
def update_doctor_id():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to update admission table
        sql_upt_doc = """
                update admission as a
                set doctor = d.id
                from doctor as d
                where a.doctor = d.name;
            """
        # execute sql command
        cur.execute(sql_upt_doc)

    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to change doctor name column into id_doctor
def alter_doctor_id():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to change column name
        sql_alt_doc = """
                alter table admission
                rename column doctor to id_doctor;
            """
        # execute sql command
        cur.execute(sql_alt_doc)

    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to update lab name column to lab id in admission table
def update_lab_id():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to update admission table
        sql_upt_lab = """
                update admission as a
                set lab = l.id
                from lab as l
                where a.lab = l.lab_name;
            """
        # execute sql command
        cur.execute(sql_upt_lab)
    
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to change lab name column into id_lab
def alter_lab_id():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to change column name
        sql_alt_lab = """
                alter table admission
                rename column lab to id_lab;
            """
        # execute sql command
        cur.execute(sql_alt_lab)

    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to update drug brands column to drug id in admission table
def update_drug_id():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to update admission table
        sql_upt_drg = """
                update admission as a
                set drug_brands = g.id
                from drugs as g
                where a.drug_brands = g.drug_name and a.drug_types = g.drug_type;
            """
        # execute sql command
        cur.execute(sql_upt_drg)

    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to change drug brands column into id_drug
def alter_drug_id():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to change column name
        sql_alt_drg = """
                alter table admission
                rename column drug_brands to id_drug;
            """
        # execute sql command
        cur.execute(sql_alt_drg)

    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to update branch name column to branch id in admission table
def update_branch_id():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to update admission table
        sql_upt_brch = """
                update admission as a
                set branch = b.id
                from branch as b
                where a.branch = b.branch_name;
            """
        # execute sql command
        cur.execute(sql_upt_brch)
    
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to change branch name column into id_branch
def alter_branch_id():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to change column name
        sql_alt_branch = """
                alter table admission
                rename column branch to id_branch;
            """
        # execute sql command
        cur.execute(sql_alt_branch)
    
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to update surgery type column to surgery id in admission table
def update_surg_id():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to update admission table
        sql_upt_surg = """
                update admission as a
                set surgery = s.id
                from surgery as s
                where a.surgery = s.surgery_type;
            """
        # execute sql command
        cur.execute(sql_upt_surg)

    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to change surgery column into id_surgery
def alter_surg_id():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to change column name
        sql_alt_surg = """
                alter table admission
                rename column surgery to id_surgery;
            """
        # execute sql command
        cur.execute(sql_alt_surg)

    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to update patient fullname column to patient id in admission table
def update_patient_id():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to update admission table
        sql_upt_pat = """
                update admission as a
                set fullname = p.id
                from patient as p
                where a.fullname = p.patient_name and a.gender = p.gender and a.age = p.age;
            """
        # execute sql command
        cur.execute(sql_upt_pat)
    
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to change fullname column into id_patient
def alter_pat_id():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to change column name
        sql_alt_pat = """
                alter table admission
                rename column fullname to id_patient;
            """
        # execute sql command
        cur.execute(sql_alt_pat)

    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to update room column to room id in admission table
def update_room_id():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to update admission table
        sql_upt_room = """
                update admission as a
                set room = r.id
                from room_type as r
                where a.room = r.room_type and a.food = r.food_price;
            """
        # execute sql command
        cur.execute(sql_upt_room)

    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to change room column into id_room
def alter_room_id():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to change column name
        sql_alt_room = """
                alter table admission
                rename column room to id_room;
            """
        # execute sql command
        cur.execute(sql_alt_room)
    
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to update hospital care column to hc id in admission table
def update_hoscare():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to update admission table
        sql_upt_hosc ="""
                update admission as a
                set hospital_care = hc.id
                from hosp_care as hc
                where a.hospital_care = hc.hospital_care;
            """
        # execute sql command
        cur.execute(sql_upt_hosc)
    
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to change room column into id_room
def alter_hoscare():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to update admission table
        sql_alt_room = """
                alter table admission
                rename column hospital_care to id_hospital_care;
            """
        # execute sql command
        cur.execute(sql_alt_room)
    
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to update drugs column to drug id in drug_stock table
def update_drug_stock():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to update drug_stock table
        sql_upt_dr ="""
                update drug_stock as ds
                set drugs = d.id
                from drugs as d
                where ds.drugs = d.drug_name;
            """
        # execute sql command
        cur.execute(sql_upt_dr)
    
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to change drugs column into id_drug
def alter_drug_stock():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to change column name
        sql_alt_dr = """
                alter table drug_stock
                rename column drugs to id_drug;
            """
        # execute sql command
        cur.execute(sql_alt_dr)
    
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to update branch column to branch id in drug_stock table
def update_branch():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to update drug_stock table
        sql_upt_branch ="""
                update drug_stock as ds
                set branch = b.id
                from branch as b
                where ds.branch = b.branch_name;
            """
        # excute sql command
        cur.execute(sql_upt_branch)
    
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to change branch column into id_branch
def alter_branch():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # sql command to change column name
        sql_alt_bh = """
                alter table drug_stock
                rename column branch to id_branch;
            """
        # execute sql command
        cur.execute(sql_alt_bh)
    
    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to delete column
def delete_columns():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # Delete the age, gender, drug_types, and food columns
        cur.execute("ALTER TABLE admission DROP COLUMN age, DROP COLUMN gender, DROP COLUMN drug_types, DROP COLUMN food;")

    # Log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to export normalized table into csv files
def export_tables_to_csv():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try :
        # define what table want to export
        tables = ['admission', 'drug_stock', 'doctor', 'lab', 'drugs', 'branch', 'surgery', 'patient', 'room_type', 'hosp_care', 'payment', 'review', 'drug_category']
        # Set the output directory to be the same as the DAG file directory
        output_dir = os.path.dirname(os.path.abspath(__file__))
        # loop through each table
        for table in tables:
            # execute a query to select all data from each table
            cur.execute(f"SELECT * FROM {table};")
            # fetch all rows in every table
            rows = cur.fetchall()

            # open a new CSV file for writing, specifying the file path and ensuring it is newline-separated
            with open(os.path.join(output_dir, f'{table}.csv'), 'w', newline='') as csvfile:
                # create a csv writer object
                csvwriter = csv.writer(csvfile)
                # Write the header row to the CSV file, using the column names from the cursor description
                csvwriter.writerow([desc[0] for desc in cur.description])
                # Write the data rows to the CSV file, using the 'rows' variable which contains the data
                csvwriter.writerows(rows)
    
    # log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to export merged table into csv files
def export_merged_table_to_csv():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    try:
        # Set the output directory to be the same as the DAG file directory
        output_dir = os.path.dirname(os.path.abspath(__file__))
        # execute a query to select all data from each table
        # sql command to export the merged table
        sql_merged = """
                SELECT 
                    a.id,
                    a.date_in,
                    a.date_out,
                    b.branch_name AS branch,
                    p.patient_name AS patient_name,
                    p.age AS age,
                    p.gender AS gender,
                    hc.hospital_care AS hospital_care,
                    r.room_type AS room_type,
                    d.name AS doctor,
                    s.surgery_type AS surgery_type,
                    l.lab_name AS lab_name,
                    dr.drug_type AS drug_type,
                    dr.drug_name AS drug_brands,
                    a.drug_qty,
                    r.food_price AS food,
                    a.admin_cost,
                    a.cogs,
                    pay.payment,
                    rev.review
                FROM 
                    admission a
                LEFT JOIN 
                    branch b ON a.id_branch::integer = b.id
                LEFT JOIN 
                    patient p ON a.id_patient::integer = p.id
                LEFT JOIN 
                    hosp_care hc ON a.id_hospital_care::integer = hc.id
                LEFT JOIN 
                    room_type r ON a.id_room::integer = r.id
                LEFT JOIN 
                    doctor d ON a.id_doctor::integer = d.id
                LEFT JOIN 
                    surgery s ON a.id_surgery::integer = s.id
                LEFT JOIN 
                    lab l ON a.id_lab::integer = l.id
                LEFT JOIN 
                    drugs dr ON a.id_drug::integer = dr.id
                LEFT JOIN 
                    payment pay ON a.id_payment::integer = pay.id;
                LEFT JOIN 
                    reviw rev ON a.id_review::integer = rev.id;;
            """
        # execute sql command
        cur.execute(sql_merged)
        # fetch all rows in every table
        rows = cur.fetchall()

        # open a new CSV file for writing
        csv_file_path = os.path.join(output_dir, 'merged_table.csv')
        with open(csv_file_path, 'w', newline='') as csvfile:
            # create a csv writer object
            csvwriter = csv.writer(csvfile)
            # write the header row
            csvwriter.writerow([desc[0] for desc in cur.description])
            # write the data rows
            csvwriter.writerows(rows)
    
    # log the error to the logs table
    except Exception as e:
        # convert any error message and save it in str format
        error_message = str(e)
        # return error message
        return error_message
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# function to insert every logs into table logs
def insert_table_logs():
    # connect to database using psycopg2.connect
    conn = db.connect(conn_string)
    # create cur object to execute SQL commands
    cur = conn.cursor()
    # list of every function that has error check condition
    error_messages = [create_table_logs(), create_table_stock_obat(), create_table_admission(), insert_data_stock_obat(), insert_data_admission(), data_cleaning(), create_table_doctor(), convert_datatype(), create_table_payment(), create_table_review(), create_table_cat_drug(), create_table_lab(), create_table_drug(), create_table_branch(), create_table_surgery(), create_table_patient(), create_table_type(), create_table_hoscare(), insert_table_doctor(), insert_table_payment(), insert_table_review(), insert_table_cat_drug(),insert_table_lab(), insert_table_drug(), insert_table_branch(), insert_table_surgery(), insert_table_patient(), 
                      insert_table_room_type(), insert_table_hoscare(), update_doctor_id(), alter_doctor_id(), update_payment_id(), alter_payment_id(), update_review_id(), alter_review_id(), update_lab_id(), alter_lab_id(), update_drug_id(), alter_drug_id(), update_branch_id(), alter_branch_id(), update_surg_id(), alter_surg_id(), update_patient_id(), alter_pat_id(), update_room_id(), alter_room_id(), update_hoscare(), alter_hoscare(), update_drug_stock(), alter_drug_stock(), update_branch(), alter_branch(), update_cat_id(), alter_category_id(), delete_columns(), export_tables_to_csv(), export_merged_table_to_csv()]
    # loops through every element
    for err_msg in error_messages:
        # get current datetime
        call_date = datetime.now()
        # if there is any error
        if err_msg != None:
            # define the value (call_date and error message) to be inserted into the table logs
            values_logs = (call_date, err_msg)
            # sql command to insert data into logs table
            insert_sql_logs = "INSERT INTO logs (call_date, error_message) VALUES (%s, %s);"
            # execute sql command and its value
            cur.execute(insert_sql_logs, values_logs)
        else:
            # Define the success message
            success_message = "Success"
            # Define the value (call_date and success message) to be inserted into the logs table
            values_logs = (call_date, success_message)
            # SQL command to insert data into the logs table
            insert_sql_logs = "INSERT INTO logs (call_date, error_message) VALUES (%s, %s);"
            # Execute the SQL command and its value
            cur.execute(insert_sql_logs, values_logs)
    # commit the task to database
    conn.commit()
    # close the connection to database
    conn.close()


# Define the DAG and its configurations
with DAG(
    default_args=default_args,  # Default arguments for the DAG
    dag_id='dag_with_logs_12',  # Unique identifier for the DAG
    start_date=datetime(2024, 6, 7),  # Start date of the DAG
    schedule_interval='0 0 * * *'  # Cron expression for scheduling the DAG
) as dag:

    # Task to create the 'stock_obat' table in PostgreSQL
    create_stock_obat = PythonOperator(
        task_id='create_postgres_stock_obat',  # Unique identifier for the task
        python_callable=create_table_stock_obat  # Python function to be executed
    )

    # Task to insert data into the 'stock_obat' table
    insert_stock_obat = PythonOperator(
        task_id='insert_into_table_stock_obat',
        python_callable=insert_data_stock_obat,
        provide_context=True  # Provide Airflow context to the function
    )

    # Task to create the 'admission' table in PostgreSQL
    create_admission = PythonOperator(
        task_id='create_postgres_admission',
        python_callable=create_table_admission
    )

    # Task to insert data into the 'admission' table
    insert_admission = PythonOperator(
        task_id='insert_into_table_admission',
        python_callable=insert_data_admission,
        provide_context=True
    )

    # Task to create the 'logs' table in PostgreSQL
    create_logs = PythonOperator(
        task_id='create_table_logs',
        python_callable=create_table_logs,
        provide_context=True
    )

    # Task to clean the data
    cleaning_data = PythonOperator(
        task_id='cleaning_data',
        python_callable=data_cleaning,
        provide_context=True
    )

    # Task to create the 'payment' table in PostgreSQL
    create_payment = PythonOperator(
        task_id='create_table_payment',
        python_callable=create_table_payment,
        provide_context=True
    )

    # Task to create the 'review' table in PostgreSQL
    create_review = PythonOperator(
        task_id='create_table_review',
        python_callable=create_table_review,
        provide_context=True
    )

    # Task to create the 'cat_drug' table in PostgreSQL
    create_cat_drug = PythonOperator(
        task_id='create_table_cat_drug',
        python_callable=create_table_cat_drug,
        provide_context=True
    )

    # Task to create the 'doctor' table in PostgreSQL
    create_doctor = PythonOperator(
        task_id='create_table_doctor',
        python_callable=create_table_doctor,
        provide_context=True
    )

    # Task to create the 'lab' table in PostgreSQL
    create_lab = PythonOperator(
        task_id='create_table_lab',
        python_callable=create_table_lab,
        provide_context=True
    )

    # Task to create the 'drug' table in PostgreSQL
    create_drug = PythonOperator(
        task_id='create_table_drug',
        python_callable=create_table_drug,
        provide_context=True
    )

    # Task to create the 'branch' table in PostgreSQL
    create_branch = PythonOperator(
        task_id='create_table_branch',
        python_callable=create_table_branch,
        provide_context=True
    )

    # Task to create the 'surgery' table in PostgreSQL
    create_surgery = PythonOperator(
        task_id='create_table_surgery',
        python_callable=create_table_surgery,
        provide_context=True
    )

    # Task to create the 'patient' table in PostgreSQL
    create_patient = PythonOperator(
        task_id='create_table_patient',
        python_callable=create_table_patient,
        provide_context=True
    )

    # Task to create the 'type' table in PostgreSQL
    create_type = PythonOperator(
        task_id='create_table_type',
        python_callable=create_table_type,
        provide_context=True
    )

    # Task to create the 'hoscare' table in PostgreSQL
    create_hoscare = PythonOperator(
        task_id='create_table_hoscare',
        python_callable=create_table_hoscare,
        provide_context=True
    )

    # Task to insert data into the 'payment' table
    insert_payment = PythonOperator(
        task_id='insert_table_payment',
        python_callable=insert_table_payment,
        provide_context=True
    )

    # Task to insert data into the 'review' table
    insert_review = PythonOperator(
        task_id='insert_table_review',
        python_callable=insert_table_review,
        provide_context=True
    )

    # Task to insert data into the 'cat_drug' table
    insert_cat_drug = PythonOperator(
        task_id='insert_table_cat_drug',
        python_callable=insert_table_cat_drug,
        provide_context=True
    )

    # Task to insert data into the 'doctor' table
    insert_doctor = PythonOperator(
        task_id='insert_table_doctor',
        python_callable=insert_table_doctor,
        provide_context=True
    )

    # Task to insert data into the 'lab' table
    insert_lab = PythonOperator(
        task_id='insert_table_lab',
        python_callable=insert_table_lab,
        provide_context=True
    )

    # Task to insert data into the 'drug' table
    insert_drug = PythonOperator(
        task_id='insert_table_drug',
        python_callable=insert_table_drug,
        provide_context=True
    )

    # Task to insert data into the 'branch' table
    insert_branch = PythonOperator(
        task_id='insert_table_branch',
        python_callable=insert_table_branch,
        provide_context=True
    )

    # Task to insert data into the 'surgery' table
    insert_surgery = PythonOperator(
        task_id='insert_table_surgery',
        python_callable=insert_table_surgery,
        provide_context=True
    )

    # Task to insert data into the 'patient' table
    insert_patient = PythonOperator(
        task_id='insert_table_patient',
        python_callable=insert_table_patient,
        provide_context=True
    )

    # Task to insert data into the 'room_type' table
    insert_room_type = PythonOperator(
        task_id='insert_table_room_type',
        python_callable=insert_table_room_type,
        provide_context=True
    )

    # Task to insert data into the 'hoscare' table
    insert_hoscare = PythonOperator(
        task_id='insert_table_hoscare',
        python_callable=insert_table_hoscare,
        provide_context=True
    )

    # Task to convert data types
    convert_dtype = PythonOperator(
        task_id='convert_datatype',
        python_callable=convert_datatype,
        provide_context=True
    )

    # Task to update the 'payment' table
    update_pay = PythonOperator(
        task_id='update_payment_id',
        python_callable=update_payment_id,
        provide_context=True
    )

    # Task to alter the 'payment' table
    alter_pay = PythonOperator(
        task_id='alter_payment_id',
        python_callable=alter_payment_id,
        provide_context=True
    )

    # Task to update the 'review' table
    update_rev = PythonOperator(
        task_id='update_review_id',
        python_callable=update_review_id,
        provide_context=True
    )

    # Task to alter the 'review' table
    alter_rev = PythonOperator(
        task_id='alter_review_id',
        python_callable=alter_review_id,
        provide_context=True
    )

    # Task to update the 'cat_drug' table
    update_cat = PythonOperator(
        task_id='update_cat_id',
        python_callable=update_cat_id,
        provide_context=True
    )

    # Task to alter the 'cat_drug' table
    alter_cat = PythonOperator(
        task_id='alter_category_id',
        python_callable=alter_category_id,
        provide_context=True
    )

    # Task to update the 'doctor' table
    update_doc = PythonOperator(
        task_id='update_doctor_id',
        python_callable=update_doctor_id,
        provide_context=True
    )

    # Task to alter the 'doctor' table
    alter_doc = PythonOperator(
        task_id='alter_doctor_id',
        python_callable=alter_doctor_id,
        provide_context=True
    )

    # Task to update the 'lab' table
    update_lab = PythonOperator(
        task_id='update_lab_id',
        python_callable=update_lab_id,
        provide_context=True
    )

    # Task to alter the 'lab' table
    alter_lab = PythonOperator(
        task_id='alter_lab_id',
        python_callable=alter_lab_id,
        provide_context=True
    )

    # Task to update the 'drug' table
    update_drug = PythonOperator(
        task_id='update_drug_id',
        python_callable=update_drug_id,
        provide_context=True
    )

    # Task to alter the 'drug' table
    alter_drug = PythonOperator(
        task_id='alter_drug_id',
        python_callable=alter_drug_id,
        provide_context=True
    )

    # Task to update the 'branch' table
    update_brch_id = PythonOperator(
        task_id='update_branch_id',
        python_callable=update_branch_id,
        provide_context=True
    )

    # Task to alter the 'branch' table
    alter_brch_id = PythonOperator(
        task_id='alter_branch_id',
        python_callable=alter_branch_id,
        provide_context=True
    )

    # Task to update the 'surgery' table
    update_surgery = PythonOperator(
        task_id='update_surg_id',
        python_callable=update_surg_id,
        provide_context=True
    )

    # Task to alter the 'surgery' table
    alter_surgery = PythonOperator(
        task_id='alter_surg_id',
        python_callable=alter_surg_id,
        provide_context=True
    )

    # Task to update the 'patient' table
    update_pat = PythonOperator(
        task_id='update_patient_id',
        python_callable=update_patient_id,
        provide_context=True
    )

    # Task to alter the 'patient' table
    alter_pat = PythonOperator(
        task_id='alter_pat_id',
        python_callable=alter_pat_id,
        provide_context=True
    )

    # Task to update the 'room' table
    update_room = PythonOperator(
        task_id='update_room_id',
        python_callable=update_room_id,
        provide_context=True
    )

    # Task to alter the 'room' table
    alter_room = PythonOperator(
        task_id='alter_room_id',
        python_callable=alter_room_id,
        provide_context=True
    )

    # Task to update the 'hospital care' table
    update_hos_care = PythonOperator(
        task_id='update_hoscare',
        python_callable=update_hoscare,
        provide_context=True
    )

    # Task to alter the 'hospital care' table
    alter_hos_care = PythonOperator(
        task_id='alter_hoscare',
        python_callable=alter_hoscare,
        provide_context=True
    )

    # Task to update the 'drug stock' table
    update_drugstock = PythonOperator(
        task_id='update_drug_stock',
        python_callable=update_drug_stock,
        provide_context=True
    )

    # Task to alter the 'drug stock' table
    alter_drugstock = PythonOperator(
        task_id='alter_drug_stock',
        python_callable=alter_drug_stock,
        provide_context=True
    )

    # Task to update the 'branch' table
    update_brch = PythonOperator(
        task_id='update_branch',
        python_callable=update_branch,
        provide_context=True
    )

    # Task to alter the 'branch' table
    alter_brch = PythonOperator(
        task_id='alter_branch',
        python_callable=alter_branch,
        provide_context=True
    )

    # Task to delete specific columns
    delete_col = PythonOperator(
        task_id='delete_columns',
        python_callable=delete_columns,
        provide_context=True
    )

    # Task to export tables to CSV files
    export_table = PythonOperator(
        task_id='export_tables_to_csv',
        python_callable=export_tables_to_csv,
        provide_context=True
    )

    # Task to export merged table to a CSV file
    export_merged_table = PythonOperator(
        task_id='export_merged_table-to_csv',
        python_callable=export_merged_table_to_csv,
        provide_context=True
    )

    # Task to insert logs into the 'logs' table
    insert_logs = PythonOperator(
        task_id='insert_table_logs',
        python_callable=insert_table_logs,
        provide_context=True
    )

    # Define the task dependencies
    create_stock_obat >> create_admission >> create_logs >> create_payment >> create_review >> create_cat_drug >> create_doctor >> create_lab >> create_drug >> create_branch >> create_surgery >> create_patient >> create_type >> create_hoscare >> insert_stock_obat >> insert_admission >> cleaning_data >> convert_dtype >> insert_payment >> insert_review >> insert_cat_drug >> insert_doctor >> insert_lab >> insert_drug >> insert_branch >> insert_surgery >> insert_patient >> insert_hoscare >> insert_room_type >> update_pay >> alter_pay >> update_rev >> alter_rev >> update_doc >> alter_doc >> update_lab >> alter_lab >> update_drug >> alter_drug >> update_brch_id >> alter_brch_id >> update_surgery >> alter_surgery >> update_pat >> alter_pat >> update_room >> alter_room >> update_hos_care >> alter_hos_care >> update_drugstock >> alter_drugstock >> update_brch >> alter_brch >> update_cat >> alter_cat >> delete_col >> export_table >> export_merged_table >> insert_logs

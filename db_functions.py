import sqlite3 as db
from sqlite3 import Error

def create_connection(db_name):
    """ 
    create a database connection to the SQLite database
        specified by db_name
    return: Connection object or None
    """
    conn = None
    try:
        conn = db.connect(db_name)
        return conn
    except Error as e:
        print(e)

    return conn

def conn_execute(db_name, sql_code):
    """
    excute sql code on the connection to the sqlite database
    """
    conn = create_connection(db_name)
    c = conn.cursor()
    try:
        c.execute(sql_code)
    except Error as e:
        print(e)
    
    data = c.fetchall()
    
    conn.close()
    
    return data
    
# Functions to interact with database

def check_table_exists(tbl_name, db_name): 
    """
    Check if table exists
    """
    exists = 0
    
    sql_code = "SELECT count(name) FROM sqlite_master WHERE type='table'"
    exists = conn_execute(db_name, sql_code) 
    if exists[0][0] == 0:
        print("No tables in DB.")
    else:
        try:
            exists = conn_execute(db_name, """SELECT COUNT(name) FROM sqlite_master 
                                    WHERE type = 'table' AND name='" + tbl_name +"';""")
        except Error as e:
            print(e)
        if exists[0][0] == 1:
            exists = 1
    
    return exists[0][0]


def create_data_table(tbl_name, db_name):
    """
    create data table to hold series values
    """
    #conn = create_connection(db_name)
    #c = conn.cursor()
    
    sql_code1 = ("""CREATE TABLE IF NOT EXISTS """ + tbl_name + """ (
                    date PRIMARY KEY
                    ,latest INT
                    ,value
                    ,series FORIEGN KEY
                ) WITHOUT ROWID;""")
    
    conn_execute(db_name, sql_code1) 
    
    #conn.close()
    
def create_dict_table(tbl_name, db_name):
    """
    create dictionary table to hold the attributes of the series
    """
    #conn = create_connection(db_name)
    #c = conn.cursor()
    
    sql_code1 = ("""CREATE TABLE IF NOT EXISTS """ + tbl_name + """ (
                    series_id PRIMARY KEY
                    ,series_title
                    ,seasonality
                    ,survey_long_name
                    ,survey_short_name
                    ,survey_abbreviation
                    ,measure_data_type
                    ,area
                    ,item
                ) WITHOUT ROWID;""")
    
    conn_execute(db_name, sql_code1)
    
    #conn.close()
    
def add_data_to_db(df_EntryData, tbl_name, db_name):  
    """
    Add dataframe to 
    database: db_name 
    table: tbl_name
    """
    conn = create_connection(db_name)    
    try:
        df_EntryData.to_sql(tbl_name, con=conn, if_exists = 'append')
    except ValueError as e:
        print(e)
        print("Data was not added to the DB")
    conn.close()
    

def table_to_df(tbl_name, db_name):
    conn = db.connect(db_name)
    try:
        df_fromDB = pd.read_sql_query("SELECT * FROM " + tbl_name + ";", conn)#, index_col="date")
    except Error as e:
        print(e)
    
    conn.close()
    
    return df_fromDB
from mysql.connector import connect, Error
import pandas as pd
import json
import os





try:
    if not os.path.exists("tables"):
        os.mkdir("tables")

    with open("config3.json","r") as f:
        config=json.load(f)

    db_details=config["src_db"]

    with connect(
        host=db_details["host"],
        user=db_details["user"],
        password=db_details["password"],
        database=db_details["database"]
    ) as connection:
    
        cursor=connection.cursor()
        tables='show tables'
        cursor.execute(tables)
        result=cursor.fetchall()


        for i in result: 
            table=i[0]
            content=f'select * from {table}'
            df=pd.read_sql(content,connection)
            df.to_csv(f"tables/{table}.csv",index=False)
                         
except Error as e:
    print(e)

finally:
    print("Extraction Complete!!!")


from mysql.connector import connect, Error
from pyspark.sql import SparkSession
import json
import os

spark = SparkSession.builder.config("spark.jars", "mysql-connector-java-8.0.13.jar").getOrCreate()

try:
    with open("config.json","r") as f:
        config=json.load(f)

    os.mkdir("source_parquet_files")

    db_details=config["src_db"]
    

    host=db_details.get("host")
    user=db_details.get("user")
    password=db_details.get("password")
    database=db_details.get("database")

    with connect(
        host=host,
        user=user,
        password=password,
        database=database
    ) as connection:

        cursor=connection.cursor()
        tables='show tables'
        cursor.execute(tables)
        result=cursor.fetchall()
        

    for i in result:
        table=i[0]
        df = spark.read \
            .format("jdbc") \
            .option("driver","com.mysql.jdbc.Driver") \
            .option("url", f"jdbc:mysql://{host}:3306/{database}") \
            .option("dbtable", f"{table}") \
            .option("user", f"{user}") \
            .option("password", f"{password}") \
            .load()
        df.write.parquet(f"source_parquet_files/{table}.parquet")

    print("Extraction Complete!!!")

except Error as e:
    print(e)


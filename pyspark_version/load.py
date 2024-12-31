from pyspark.sql import SparkSession
import json
import os

spark = SparkSession.builder.config("spark.jars", "mysql-connector-java-8.0.13.jar").getOrCreate()

try:
    with open("config.json","r") as f:
        config=json.load(f)

    target_details=config["target_db"]

    target_host=target_details.get("host")
    target_user=target_details.get("user")
    target_db=target_details.get("database")
    target_pass=target_details.get("password")

    transformed_files=(os.listdir("result"))
    

    for i in transformed_files:
        table=i.replace(".parquet","")
        df=spark.read.parquet(f"result/{i}")
        df.write \
        .mode("overwrite") \
        .format("jdbc") \
        .option("driver","com.mysql.jdbc.Driver") \
        .option("url", f"jdbc:mysql://{target_host}:3306/{target_db}") \
        .option("dbtable", f"{table}") \
        .option("user", f"{target_user}") \
        .option("password", f"{target_pass}") \
        .save()
    
    print("Loading Successful")
except Exception as e:
    print("An error occured!",e)
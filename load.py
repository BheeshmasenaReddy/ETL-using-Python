from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from mysql.connector import connect, Error
import pandas as pd
import json
import os

with open("config3.json","r") as f:
        config=json.load(f)
target_db=config[target_db]
engine=create_engine(target_db)

if not database_exists(engine.url):
    create_database(engine.url)

transformed_files=(os.listdir("result"))


for i in transformed_files:
    table=i.replace(".csv","")
    globals()[table]=pd.read_csv(f"result/{i}")
    globals()[table].to_sql(f"{table}",con=engine,index=False)
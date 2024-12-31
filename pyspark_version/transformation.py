import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.getOrCreate()

try:

    with open("config.json","r") as f:
        config=json.load(f)

    def concat(source_table,source_column,target_column):
        source_table=source_table.select("*",concat_ws(" ",*source_table[source_column]).alias(f"{target_column}"))
        return source_table  

    def to_camelcase(source_table,source_column):
        source_table = source_table.withColumn(source_column, initcap(col(source_column)))
        return source_table

    def drop(source_table,source_column):
        source_table=source_table.drop(source_column)
        
        return source_table

    def change_format(source_table,source_column,to_format):  
        source_table = source_table.withColumn(source_column,date_format(col(source_column), to_format))
        return source_table

    def split_column(source_table,source_column,target_column):
        for target in target_column:
            source_table = source_table.withColumn(target, split(col(source_column), ' ')[target_column.index(target)])
        return source_table

    def join_tables(tables,target_table,common_columns,type):
        for table in tables:
            locals()[f"{table}"]=spark.read.parquet(f"source_parquet_files/{table}.parquet")
            tables[tables.index(table)]=locals()[f"{table}"]
        
        target_table = tables[1]

        i=0
        for df in tables[0:1] + tables[2:]:
            target_table = target_table.join(df, common_columns[i], type)
            i+=1

        unique_columns = set()
        
        for col in target_table.columns:
            if col in unique_columns:
                continue
        unique_columns.add(col)
        target_table = target_table.drop(col)
        
        return target_table
        
    os.mkdir("result")

    for task in config["transformations"]:
        attr=[]

        for key,value in task.items():
            locals()[f"{key}"]=value
            attr.append(value)

        attr.remove(task.get("operation"))

        if operation!="join_tables":
            df=spark.read.parquet(f"parquet_files/{source_table}.parquet")
            attr[0]=df
        
        if type(operation)==str:
            func=eval(operation)#"concat"
            
            if operation!="join_tables": 
                df=func(*attr)
                df.write.parquet(f"result/res_{source_table}.parquet")
            
            elif operation=="join_tables":
                df=func(*attr)
                df.write.parquet(f"result/res_{target_table}.parquet")
        

        elif type(operation)==list:
            
            for i in operation:
                oper=[df]
                
                for k,v in i.items():
                    src_col=k
                    oper.append(src_col)
                    op=v 

                func=eval(op)
                df=func(*oper)  
            
            df.write.parquet(f"result/res_{source_table}.parquet")

    print("Transformation Complete!!")

except Exception as e:
    print("An error occured: ",e)

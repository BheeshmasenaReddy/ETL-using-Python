from mysql.connector import connect, Error
import pandas as pd
import json
import os


with open("config3.json","r") as f:
    config=json.load(f)

def concat(source_table,source_column,target_column):
    source_table[f"{target_column}"]=source_table[source_column].agg(' '.join,axis=1)

def to_camelcase(source_table,source_column):
    source_table[f"{source_column}"]=source_table[f"{source_column}"].str.title()

def drop(source_table,source_column):
    source_table.drop(columns=[source_column],inplace=True,axis=1)

def change_format(source_table,source_column,to_format):  
    source_table[f"{source_column}"]=pd.to_datetime(source_table[f"{source_column}"]).dt.strftime(f"{to_format}")

def split(source_table,source_column,target_column):
    source_table[target_column]=source_table[f"{source_column}"].str.split(" ",n=len(target_column)-1,expand=True)
    source_table.drop(columns=[f"{source_column}"],inplace=True,axis=1)


def join(tables,target_table,common_columns):

    if len(tables)==2:
        table1=pd.read_csv(f"tables/{tables[0]}.csv")
        table2=pd.read_csv(f"tables/{tables[1]}.csv")
        globals()[target_table]=table1.merge(table2,on=f"{common_columns[0]}")
           
    elif len(tables)==3:
        table1=pd.read_csv(f"tables/{tables[0]}.csv")
        table2=pd.read_csv(f"tables/{tables[1]}.csv")
        table3=pd.read_csv(f"tables/{tables[2]}.csv")
        globals()[target_table]=table1.merge(table2,on=f"{common_columns[0]}").merge(table3,on=f"{common_columns[1]}")
        
os.mkdir("result")

for task in config["transformations"]:
    attr=[]

    for key,value in task.items():
        globals()[f"{key}"]=value
        attr.append(value)

    attr.remove(task.get("operation"))

    if operation!="join":
        globals()[f"{source_table}"]=pd.read_csv(f"tables/{source_table}.csv")
        attr[0]=globals()[f"{source_table}"]
    
    if type(operation)==str:
        func=eval(operation)
        func(*attr) 

        if operation!="join":
            globals()[f"{source_table}"].to_csv(f"result/res_{source_table}.csv",index=False)

        elif operation=="join":
            globals()[f"{target_table}"].to_csv(f"result/res_{target_table}.csv",index=False)

    elif type(operation)==list:
        for i in operation:
            oper=[globals()[f"{source_table}"]]
            
            for k,v in i.items():
                src_col=k
                oper.append(src_col)
                op=v 

            func=eval(op)
            func(*oper)    
        globals()[f"{source_table}"].to_csv(f"result/res_{source_table}.csv",index=False)

print("Transformation Complete!!")
# ETL-using-Python

ETL stands for Extraction, Transformation and Loading.

This project uses various python modules such as my-sqlconnector-python, pandas, sqlalchemy for peroforming the ETL job.

**Extraction**: Reading an existing source database and copying it to our machine. Here we extract data in 1:1 unloading which means each table in the database will be convertd to one csv file.

  The details of the source database from which data needs to be extracted are stored in config3.json file. In this example we used "sakila" database that comes dafault in mysql-workbench with pre-loaded data. Using mysql-connector the script connects to database and loops through it and converts each table into a dataframe using pandas and dataframe into csv file in tables directory.

**Transformation**: Manipulating of performing the operations such as concatinating columns, drop tables, joining tables etc on the extracted data for analytics purpose.
  
  This transformation scipt will parse through transformations in config3.json and converts requires csv files into pandas dataframes. Then the transformation specified in transformations is applied on dataframe. After all the transformation the dataframe again converted to csv file in result directory.

**Loading**: Exporting the transformed data into a target database for analysis.

  The loading script converts all the transformed csv files into pandas dataframes and reads the target database from config3.json. Using sqlalchemy and sqlalchemy_utils it creates the target database and loads all the transformed dataframes into database as tables.

This project is written in two versions.
The first version uses **Pandas** to create dataframes from sql tables and csv files.
The second version uses **Pyspark**. It is fast and is particularly used for large datasets. Pyspark dataframes are fast to read and converting to other data types like parquet.
The Pyspark code is approximately 30% faster than the Pandas code for the sample dataset I used. For bigger datasets the performance difference further increases.

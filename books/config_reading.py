import configparser
import os

# Creating configParser Object

cp = configparser.ConfigParser()

# Reading the config files

cp.read('D:\Learning\config_file_1.ini')

#Assigning the parameter values

path = cp.get('Paths', 'path')
source_file1 = cp.get('Paths', 'source_file1')
source_file2 = cp.get('Paths', 'source_file2')
Target_file = cp.get('Paths', 'target_file')
join_type = cp.get('DB','type_of_join')
join_column = cp.get('DB','join_column').strip(" ")
database = cp.get('DB','Database').strip(" ")
target_table = cp.get('DB','target_table').strip(" ")
user_name = cp.get('DB','user_name').strip(" ")
password = cp.get('DB','password').strip(" ")
driver = cp.get('DB', 'driver').strip(" ")
connection_string = cp.get('DB','connection_string').strip(" ")
source_file1_path = os.path.join(path, source_file1)
source_file2_path = os.path.join(path, source_file2)
Target_file2_path = os.path.join(path, Target_file)
df_write = cp.get('DB','df_write')
config_table = cp.get('DB','config_table')
server_name = cp.get('DB','server')
sqldriver = cp.get('DB','sqldriver')


from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("Config file checkup").config("spark.driver.extraClassPath","D:\spark\spark-3.1.2-bin-hadoop2.7\jars\mssql-jdbc-9.2.1.jre11.jar").getOrCreate()

source_df1 = spark.read.csv(source_file1_path,header=True,inferSchema=True)
source_df2 = spark.read.csv(source_file2_path,header=True,inferSchema=True)

a = 'source_df1.join(source_df2,'+'source_df1.'+ join_column + " == source_df2."+ join_column+ "," +"'"+ join_type +"')"
# a = "source_df1.join(source_df2,source_df1.{0} == source_df2.{0},'{1}')".format(join_column,join_type)

b = 'join_df.write.mode("append").format("jdbc").option("url","' + connection_string + '").option("dbtable","' + target_table + '").option("user","' + user_name +'").option("password","' + password + '").option("driver","' + driver + '").save()'
# b = 'join_df.write.mode("overwrite").format("jdbc").option("url","{0}").option("dbtable","{1}").option("user","{2}").option("password","{3}").option("driver","{4}").save()'.format(connection_string,target_table,user_name,password,driver)

join_df = eval(a).select(source_df1.ID,source_df1.Sepal_Length,source_df1.Sepal_Width,source_df2.Petal_Length,source_df2.Petal_Width,source_df2.Species)

#a = join_df.select("ID","Sepal_Length","Sepal_Width","Petal_Length","Petal_Width","Species")
#a.show()
print(join_df.printSchema())

join_df.select("ID","Sepal_Length","Sepal_Width","Petal_Length","Petal_Width","Species").write.mode("append").parquet("D:\Learning\sample_file1")

df_parquet = spark.read.parquet("D:\Learning\sample_file1")

df_parquet.show(10)

eval(b)

# Following parameter is stored in config file to execute that we are using exec
#df_write = 'join_df.write.mode("overwrite").format("jdbc").option("url","' + connection_string + '").option("dbtable","' + target_table + '").option("user","' + user_name +'").option("password","' + password + '").option("driver","' + driver + '").save()'
#

exec(df_write)

'''Uploading config details into Table'''
df_config_load = spark.read.csv('D:\Learning\config_file_load.csv',header=True)
df_config_load1 = df_config_load.select(col("ID_NUM").cast('int'),"SOURCE_PATH","SOURCE_FILE_1","SOURCE_FILE_2","TARGET_FILE","DATABASE_N","CONNECTION_STRING","DRIVER","SOURCE_TABLES","TARGET_TABLE","TYPE_OF_JOIN","JOIN_COLUMN","USER_NAME_V","PASSWORD_V")
df_config_load.printSchema()

config_upload = 'df_config_load1.write.mode("overwrite").format("jdbc").option("url","' + connection_string + '").option("dbtable","' + config_table + '").option("user","' + user_name +'").option("password","' + password + '").option("driver","' + driver + '").save()'
eval(config_upload)

'''Reading Config details from Table '''
config_load = 'spark.read.format("jdbc").option("url","' + connection_string + '").option("dbtable","' + config_table + '").option("user","' + user_name +'").option("password","' + password + '").option("driver","' + driver + '").load()'
config_load_df = eval(config_load)
config_load_df.show()


#Reading data from config file

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("Config Table").config("spark.driver.extraClassPath","D:\spark\spark-3.1.2-bin-hadoop2.7\jars\mssql-jdbc-9.2.1.jre11.jar").getOrCreate()

import pyodbc

conn = pyodbc.connect('DRIVER='+sqldriver+';SERVER='+server_name+';PORT=1433;DATABASE='+database+';UID='+user_name+';PWD='+ password)

#'pyodbc.connect("DRIVER="{0}";SERVER="{1}";PORT=1433;DATABASE="{2}";UID="{3}";PWD="{4})'.format(sqldriver,server_name,database,user_name,password)


with conn.cursor() as cursor:
    cursor.execute('select * from SalesLT.config_table where ID_NUM = 1')
    row = cursor.fetchone()

ID_NUM = row[0]
SOURCE_PATH = row[1]
SOURCE_FILE_1 = row[2]
SOURCE_FILE_2 = row[3]
TARGET_FILE = row[4]
DATABASE_N = row[5]
CONNECTION_STRING = row[6]
DRIVER = row[7]
SOURCE_TABLES = row[8]
TARGET_TABLE = row[9]
TYPE_OF_JOIN = row[10]
JOIN_COLUMN = row[11]
USER_NAME_V = row[12]
PASSWORD_V = row[13]

print(ID_NUM)
print(SOURCE_PATH)
print(SOURCE_FILE_1)
print(SOURCE_FILE_2)
print(TARGET_FILE)
print(DATABASE_N)
print(CONNECTION_STRING)
print(DRIVER)
print(SOURCE_TABLES)
print(TARGET_TABLE)
print(TYPE_OF_JOIN)
print(JOIN_COLUMN)
print(USER_NAME_V)
print(PASSWORD_V)

source_df1 = spark.read.csv(source_file1_path,header=True,inferSchema=True)
source_df2 = spark.read.csv(source_file2_path,header=True,inferSchema=True)

a = 'source_df1.join(source_df2,'+'source_df1.'+ JOIN_COLUMN + " == source_df2."+ JOIN_COLUMN+ "," +"'"+ TYPE_OF_JOIN +"')"
b = 'join_df.write.mode("append").format("jdbc").option("url","' + CONNECTION_STRING + '").option("dbtable","' + TARGET_TABLE + '").option("user","' + USER_NAME_V +'").option("password","' + PASSWORD_V + '").option("driver","' + DRIVER + '").save()'

join_df = eval(a).select(source_df1.ID,source_df1.Sepal_Length,source_df1.Sepal_Width,source_df2.Petal_Length,source_df2.Petal_Width,source_df2.Species)


join_df.select("ID","Sepal_Length","Sepal_Width","Petal_Length","Petal_Width","Species").write.mode("append").parquet("D:\Learning\sample_file1")

df_parquet = spark.read.parquet("D:\Learning\sample_file1")

df_parquet.show(10)

eval(b)

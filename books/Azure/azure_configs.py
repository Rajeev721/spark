# Defining the service principal credentials for the Azure storage account
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", "<application-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret", "<service-credential>")
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

# Defining a separate set of service principal credentials for Azure Synapse Analytics (If not defined, the connector will use the Azure storage account credentials)
spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.id", "<application-id>")
spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.secret", "<service-credential>")


####################### to install ODBC driver####################################

%sh
pip install pyodbc
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install msodbcsql17
exit

# Init script

dbutils.fs.put("/databricks/scripts/pyodbc.sh","""
#!/bin/bash
pip install pyodbc
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install msodbcsql17""", True)

# Link
https://medium.com/@srijansahay/connecting-sql-server-oracle-mysql-and-postgresql-from-azure-services-using-python-789e93d879b4

https://medium.com/@srijansahay/connecting-sql-server-oracle-mysql-and-postgresql-from-azure-services-using-python-789e93d879b4


different ways to pass parameters in ADB

dbutils.notebook.run("/Users/rajeev.suravajhula@gmail.com/pyspark_read_config",60, {"ID":"1"})
%run ./pyspark_read_config $ID = 1
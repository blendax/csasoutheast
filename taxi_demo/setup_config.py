# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Taxi Demo
# MAGIC ##setup_config notebook
# MAGIC 
# MAGIC ###Requirements
# MAGIC - Blob Storage account with container named "ingest" along with key, connection string, resource group name, and subscription id
# MAGIC - ADLS Gen 2 Storage account with a container named "lake"
# MAGIC - Service Principal with client id, client secret, and Azure AD tenant_id with access to ADLS Gen 2 
# MAGIC - EventHub with connection string
# MAGIC - Synpase jdbc URL
# MAGIC - Setup the secrets using a linked Azure Key Vault secret scope: https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes
# MAGIC 
# MAGIC ###Results
# MAGIC 
# MAGIC - Variables created using the notebook are imported by %run to bring them into the other notebook scopes
# MAGIC - All Spark security config settings to access blob and lake in demos are setup here
# MAGIC - Create the taxidemos database if it doesnt exist and default to it

# COMMAND ----------

blob_name = "datasetsnortheu.blob.core.windows.net" # "fieldengdeveastus2sa.blob.core.windows.net"
blob_key = dbutils.secrets.get("taxi-demo-scope","blob-datasetsnortheu") # blob_key = dbutils.secrets.get("taxi-demo-scope","taxi-blob-key") 
blob_connection = dbutils.secrets.get("taxi-demo-scope","blobdatasetsnortheuconstr") # blob_connection = dbutils.secrets.get("taxi-demo-scope","taxi-blob-connection")  

# client_id = "1bcf0495-6572-4f0d-a35b-8a9c785836b9"
client_id = dbutils.secrets.get("taxi-demo-scope","datasetneugen2-sp-app-id")
client_secret = dbutils.secrets.get("taxi-demo-scope","datasetneugen2-sp-secret")  
# client_secret = dbutils.secrets.get("taxi-demo-scope","taxi-sp-key")  
tenant_id = dbutils.secrets.get("taxi-demo-scope","tenantId") # "72f988bf-86f1-41af-91ab-2d7cd011db47"
lake_name = "datasetsneugen2.dfs.core.windows.net" # "fieldengdeveastus2adls.dfs.core.windows.net"

resource_group_name = "RGSparkHDI" # Fr the blob storage account? "field-eng-dev-eastus2-rg"

subscription_id = dbutils.secrets.get("taxi-demo-scope","subscriptionId") # "3f2e4d32-8e8d-46d6-82bc-5bb8d962328b"

eh_connection_string = dbutils.secrets.get("taxi-demo-scope","eventhub-cdcpoc-connestion-string")#  dbutils.secrets.get("taxi-demo-scope","taxi-eh-connection")

eh_connection_encrypted = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eh_connection_string)

synapse_jdbc_url = dbutils.secrets.get("taxi-demo-scope","synapsene-jdbc-url")
#jdbc:sqlserver://synapsene.sql.azuresynapse.net:1433;database=GetStartedSQL;user=user@synapsename;password=YourPassword;encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;"

kafka_topic = "taxidemo"
kafka_bootstrap_servers = "cdcpoc.servicebus.windows.net:9093" # "fieldengdeveastus2ehb.servicebus.windows.net:9093"
kafka_sasl_jaas_config = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{eh_connection_string}\";"

# COMMAND ----------

spark.conf.set(f"fs.azure.account.key.{blob_name}", blob_key)

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{lake_name}", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{lake_name}", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{lake_name}", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{lake_name}", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{lake_name}", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------



# COMMAND ----------

def stop_all_streams():
    print("Stopping all streams")
    for s in spark.streams.active:
        try:
            s.stop()
        except:
            pass
    print("Stopped all streams")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS taxidemo;
# MAGIC USE taxidemo;

# COMMAND ----------

spark.sql("USE taxidemo")

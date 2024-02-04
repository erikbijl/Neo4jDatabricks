# Databricks notebook source
# MAGIC %md
# MAGIC # Neo4j SparkConnector Demo 

# COMMAND ----------

# MAGIC %md 
# MAGIC The following notebooks performs a demo of using the SparkConnector to a Neo4j database. In this setup the Neo4j database is an Aura instance running on https://console.neo4j.io/. The notebook connects to this instance via credentials that are stored in Azure Key Vault. 

# COMMAND ----------

from neo4j import GraphDatabase
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Get Neo4j credentials

# COMMAND ----------

# MAGIC %md Some credentials can be set here. Some are stored in Azure Key Vault using the databricks secrets and scope.

# COMMAND ----------

uri = "neo4j+s://29a95706.databases.neo4j.io:7687"
user = "neo4j"
database = "neo4j"

# COMMAND ----------

username = dbutils.secrets.get(scope="kv_db", key="neo4jUsername")
password = dbutils.secrets.get(scope="kv_db", key="neo4jPassword")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read data from Azure Storage account

# COMMAND ----------

# MAGIC %md Get credentials from the storage account from Azure Key Vault

# COMMAND ----------

storage_account_name =  dbutils.secrets.get(scope="kv_db", key="saName")
storage_account_access_key =  dbutils.secrets.get(scope="kv_db", key="saKeyAccess")

# COMMAND ----------

# MAGIC %md Set storage account in Spark

# COMMAND ----------

spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)

# COMMAND ----------

# MAGIC %md Load data. Data is coming from Kaggle Competition (https://www.kaggle.com/datasets/usdot/flight-delays?rvi=1)

# COMMAND ----------

blob_container = "flights"
filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/flights.csv"
flights_df = spark.read.format("csv").load(filePath, inferSchema = True, header = True)

# COMMAND ----------

# MAGIC %md 
# MAGIC Explore the Data

# COMMAND ----------

flights_df.count()

# COMMAND ----------

flights_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Airports Nodes to Neo4j

# COMMAND ----------

# MAGIC %md The airports are stored as origin and destination airport. Get all unique airports from both columns.

# COMMAND ----------

airports_from = (
    flights_df
    .select('origin_airport')
    .withColumnRenamed('origin_airport', 'name')
    .distinct()
)

airports_to = (
    flights_df
    .select('destination_airport')
    .withColumnRenamed('destination_airport', 'name')
    .distinct()
)

airports_df = airports_from.union(airports_to).distinct()
airports_df.count()

# COMMAND ----------

airports_df.display()

# COMMAND ----------

# MAGIC %md Write airports to Neo4j using the SparkConnector

# COMMAND ----------

(
    airports_df
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", user)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("labels", ":Airports")
    .option("node.keys", "name")
    .save()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Flights Nodes to Neo4j

# COMMAND ----------

# MAGIC %md Select the flighs with some columns and create a key column

# COMMAND ----------

flights_df = (
    flights_df
    .withColumn("key", F.monotonically_increasing_id())
    .select(['key', 'year', 'month', 'day', 'airline', 'flight_number', 'origin_airport', 'destination_airport'])
    .filter(F.col('key') < 5000)
)
flights_df.count()

# COMMAND ----------

flights_df.display()

# COMMAND ----------

# MAGIC %md Write airports to Neo4j using the SparkConnector

# COMMAND ----------

(
    flights_df
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", user)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("labels", ":Flights")
    .option("node.keys", "key")
    .save()
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Write Relations (:Flights)-[:FROM]->(:Airports)

# COMMAND ----------

# MAGIC %md Select flights and their origin airport. Relations require a source.[PROP] and target.[PROP] indicating from which node/column to which node/column the relation must be connected. 

# COMMAND ----------

flights_from_df = (    
    flights_df
    .select('key', 'origin_airport')
    .withColumnRenamed('origin_airport', 'target.name')
    .withColumn('source.key', F.col('key'))
    .drop('key')
)
flights_from_df.display()

# COMMAND ----------

# MAGIC %md Write the FROM relations to the database using the SparkConnector

# COMMAND ----------

(
    flights_from_df
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("overwrite")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", user)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("relationship", "FROM")
    .option("relationship.source.labels", ":Flights")
    .option("relationship.source.save.mode", "overwrite")
    .option("relationship.source.node.keys", "source.key:key")
    .option("relationship.target.labels", ":Airports")
    .option("relationship.target.save.mode", "overwrite")
    .option("relationship.target.node.keys", "target.name:name")
    .save()
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Write Relations (:Flights)-[:TO]->(:Airports)

# COMMAND ----------

# MAGIC %md Now select flights and their destination airport. Again, relations require a source.[PROP] and target.[PROP] indicating from which node/column to which node/column the relation must be connected. 

# COMMAND ----------

flights_to_df = (    
    flights_df
    .select('key', 'destination_airport')
    .withColumnRenamed('destination_airport', 'target.name')
    .withColumn('source.key', F.col('key'))
    .drop('key')
)
flights_to_df.display()

# COMMAND ----------

# MAGIC %md Write the TO relations to the database using the SparkConnector

# COMMAND ----------

(
    flights_to_df
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("overwrite")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", user)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("relationship", "TO")
    .option("relationship.source.labels", ":Flights")
    .option("relationship.source.save.mode", "overwrite")
    .option("relationship.source.node.keys", "source.key:key")
    .option("relationship.target.labels", ":Airports")
    .option("relationship.target.save.mode", "overwrite")
    .option("relationship.target.node.keys", "target.name:name")
    .save()
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Read from Database

# COMMAND ----------

# MAGIC %md Read the Flight nodes from the database

# COMMAND ----------

flights_nodes_df = (
    spark.read.format("org.neo4j.spark.DataSource")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", user)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("labels", "Flights")
    .load()
)

# COMMAND ----------

flights_nodes_df.display()

# COMMAND ----------

# MAGIC %md Read the FROM relations from the databbase

# COMMAND ----------

from_relations_df = (
    spark.read.format("org.neo4j.spark.DataSource")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", user)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("relationship", "FROM")
    .option("relationship.source.labels", "Flights")
    .option("relationship.target.labels", "Airports")
    .load()
)

# COMMAND ----------

from_relations_df.display()

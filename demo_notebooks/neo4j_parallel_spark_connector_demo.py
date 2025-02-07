# Databricks notebook source
# MAGIC %md
# MAGIC # Neo4j SparkConnector Demo 

# COMMAND ----------

# MAGIC %md 
# MAGIC The following notebooks performs a demo of using the SparkConnector to a Neo4j database. In this setup the Neo4j database is an Aura instance running on https://console.neo4j.io/. The notebook connects to this instance via credentials that are stored in Azure Key Vault. 

# COMMAND ----------

# MAGIC %pip install neo4j-parallel-spark-loader

# COMMAND ----------

from neo4j import GraphDatabase
from pyspark.sql import functions as F
from neo4j_parallel_spark_loader.bipartite import group_and_batch_spark_dataframe
from neo4j_parallel_spark_loader import ingest_spark_dataframe
from neo4j_parallel_spark_loader.visualize import create_ingest_heatmap

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Get Neo4j credentials

# COMMAND ----------

# MAGIC %md Some credentials can be set here. Some are stored in Azure Key Vault using the databricks secrets and scope.

# COMMAND ----------

user = "neo4j"
database = "neo4j"

# COMMAND ----------

username = dbutils.secrets.get(scope="kv_db", key="neo4jUsername")
password = dbutils.secrets.get(scope="kv_db", key="neo4jPassword")
uri = dbutils.secrets.get(scope="kv_db", key="neo4jUri")

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
    .option("authentication.basic.username", username)
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
    .mode("Append")
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
# MAGIC ## Write constraints on Nodes

# COMMAND ----------

class App:
    def __init__(self, uri, user, password, database=None):
        self.driver = GraphDatabase.driver(uri, auth=(user, password), database=database)
        self.database = database

    def close(self):
        self.driver.close()

    def query(self, query):
        return self.driver.execute_query(query)
        
    def count_nodes_in_db(self):
        query = "MATCH (n) RETURN COUNT(n)"
        result = self.query(query)
        (key, value) = result.records[0].items()[0]
        return value

    def remove_nodes_relationships(self):
        query = "MATCH (n) DETACH DELETE n"
        result = self.query(query)

    def remove_nodes_relationships(self):
        query ="""
            CALL apoc.periodic.iterate(
                "MATCH ()-[c]-() RETURN c",
                "WITH c DELETE c",
                {batchSize: 1000}
            )
        """
        result = self.query(query)

# COMMAND ----------

app = App(uri, username, password, database)

# COMMAND ----------

app.count_nodes_in_db()

# COMMAND ----------

query = "CREATE CONSTRAINT unique_flights IF NOT EXISTS FOR (f:Flights) REQUIRE f.key IS UNIQUE"
app.query(query)


# COMMAND ----------

query = "CREATE CONSTRAINT unique_airports IF NOT EXISTS FOR (a:Airports) REQUIRE a.name IS UNIQUE"
app.query(query)

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

# flights_from_df = flights_from_df.repartition(1)
# (
#     flights_from_df
#     .write
#     .format("org.neo4j.spark.DataSource")
#     .mode("overwrite")
#     .option("url", uri)
#     .option("authentication.type", "basic")
#     .option("authentication.basic.username", user)
#     .option("authentication.basic.password", password)
#     .option("database", database)
#     .option("relationship", "FROM")
#     .option("relationship.source.labels", ":Flights")
#     .option("relationship.source.save.mode", "overwrite")
#     .option("relationship.source.node.keys", "source.key:key")
#     .option("relationship.target.labels", ":Airports")
#     .option("relationship.target.save.mode", "overwrite")
#     .option("relationship.target.node.keys", "target.name:name")
#     .save()
# )

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

# flights_to_df = flights_to_df.repartition(1)
# (
#     flights_to_df
#     .write
#     .format("org.neo4j.spark.DataSource")
#     .mode("overwrite")
#     .option("url", uri)
#     .option("authentication.type", "basic")
#     .option("authentication.basic.username", user)
#     .option("authentication.basic.password", password)
#     .option("database", database)
#     .option("relationship", "TO")
#     .option("relationship.source.labels", ":Flights")
#     .option("relationship.source.save.mode", "overwrite")
#     .option("relationship.source.node.keys", "source.key:key")
#     .option("relationship.target.labels", ":Airports")
#     .option("relationship.target.save.mode", "overwrite")
#     .option("relationship.target.node.keys", "target.name:name")
#     .save()
# )

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

# COMMAND ----------

# MAGIC %md
# MAGIC # Parallel Load

# COMMAND ----------

app.remove_nodes_relationships()

# COMMAND ----------

spark_executor_count = 4

# COMMAND ----------

flights_from_df

# COMMAND ----------

flights_from_df.count()

# COMMAND ----------

flights_from_df = flights_from_df.withColumnRenamed("source.key", "key").withColumnRenamed("target.name", "name")

# COMMAND ----------

rel_batch_df = group_and_batch_spark_dataframe(spark_dataframe=flights_from_df, 
                                               source_col="key", 
                                               target_col="name", 
                                               num_groups=spark_executor_count)

# COMMAND ----------

rel_batch_df.display()

# COMMAND ----------

create_ingest_heatmap(rel_batch_df)

# COMMAND ----------

query = """
    MATCH (source:Flights {key: event.key})
    MATCH (target:Airports {name: event.name})
    MERGE (source)-[r:FROM]->(target)
    """

ingest_spark_dataframe(
    spark_dataframe=rel_batch_df,
    save_mode= "Overwrite",
    options={"query":query, "url": uri, "authentication.type": "basic", "authentication.basic.username": user, "authentication.basic.password": password, "database": database}
)

# COMMAND ----------

app.remove_nodes_relationships()

# COMMAND ----------

(
    flights_from_df.repartition(1).write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("query", query)
    .save()
)

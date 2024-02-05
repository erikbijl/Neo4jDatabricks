# Databricks notebook source
# MAGIC %md 
# MAGIC # Neo4j Python Driver Demo

# COMMAND ----------

# MAGIC %md 
# MAGIC The following notebook shows how to setup the Python driver and connect to an Neo4j database. 

# COMMAND ----------

from neo4j import GraphDatabase

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Setup Neo4j Python Driver
# MAGIC Setup the Python driver and define some basic queries like count nodes or removing nodes/relationships.

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

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Get Database credentials and start connnection
# MAGIC Get the credentials needed to connect to the database. The username and password are stored in Azure KeyVault.

# COMMAND ----------

username = dbutils.secrets.get(scope="kv_db", key="neo4jUsername")
password = dbutils.secrets.get(scope="kv_db", key="neo4jPassword")

# COMMAND ----------

uri = "neo4j+s://29a95706.databases.neo4j.io:7687"
database = "neo4j"

# COMMAND ----------

# MAGIC %md 
# MAGIC Start the connection by creating the application

# COMMAND ----------

app = App(uri, username, password, database)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Run some queries

# COMMAND ----------

# MAGIC %md Count nodes in the graph

# COMMAND ----------

app.count_nodes_in_db()

# COMMAND ----------

# MAGIC %md Potentially remove nodes and relations in the graph and count nodes again to verify...

# COMMAND ----------

#app.remove_nodes_relationships()
#app.count_nodes_in_db()

# COMMAND ----------

# MAGIC %md ## Run some queries on Flights and Airports

# COMMAND ----------

# MAGIC %md 
# MAGIC Define a query finding airports with most flights either incoming or outgoing

# COMMAND ----------

query = """
    MATCH (n:Airports)-[]-(:Flights)
    RETURN n.name as airport_name, COUNT(*) as flight_count ORDER BY flight_count DESC LIMIT 10
"""

# COMMAND ----------

# MAGIC %md 
# MAGIC Run the query and iterate on results

# COMMAND ----------

result = app.query(query)
for record in result.records:
    print(record.items())

# COMMAND ----------

# MAGIC %md Seems like Dallas Fort Worth International has the most flights now. Note that this is a sample so depending on your dataload these numbers can vary. 

# COMMAND ----------

# MAGIC %md Define a new query that finds airports with most outgoing flights.

# COMMAND ----------

query = """
    MATCH (n:Airports)-[:FROM]-(:Flights)
    RETURN n.name as airport_name, COUNT(*) as flight_start_count ORDER BY flight_start_count DESC LIMIT 10
"""

# COMMAND ----------

result = app.query(query)
for record in result.records:
    print(record.items())

# COMMAND ----------

# MAGIC %md Define a new query that finds airports with only incoming flights.

# COMMAND ----------

query = """
    MATCH (n:Airports)-[:TO]-(:Flights)
    RETURN n.name as airport_name, COUNT(*) as flight_end_count ORDER BY flight_end_count DESC LIMIT 10
"""

# COMMAND ----------

result = app.query(query)
for record in result.records:
    print(record.items())

# COMMAND ----------

# MAGIC %md Close the connection to the database when you are done

# COMMAND ----------

app.close()

# Neo4j and Databricks Demo
## Setup Databricks and Neo4j in the Cloud

The following Repo contains two notebooks on how to use the Neo4j SparkConnector and Neo4j Python Driver in Databricks. Before running these two notebooks some steps need to be taken. The following guide collects these steps that were taken to setup the Neo4j (Aura) and Databricks (in Azure). 

Some sources that were followed on how to setup both Neo4j and Databricks: 
- https://aura.support.neo4j.com/hc/en-us/articles/15882411504019-Using-Neo4j-Aura-on-Azure-Databricks
- https://towardsdatascience.com/using-neo4j-with-pyspark-on-databricks-eb3d127f2245

## Run Neo4j in Aura
Running a free Neo4j database using Aura is easy. Simply go to https://console.neo4j.io/ and follow the steps. 
For more detailed information the following steps can be followed (from https://neo4j.com/docs/aura/auradb/getting-started/create-database/).

To create an AuraDB Free instance in Neo4j AuraDB:

- Navigate to the [Neo4j Aura Console](https://console.neo4j.io/?product=aura-db&_ga=2.235966018.250702022.1707128766-1936509698.1707128766) in your browser.
- Select New Instance.
- Select Create Free instance.
- Copy and store the instance’s Username and Generated password or download the credentials as a .txt file.
- Tick the confirmation checkbox, and select Continue.

## Deploying Databricks in Azure
Deploying databricks in Azure is relatively easy. In Databricks some steps are needed to be able to connect to Neo4j. The steps are described below.

### Create Resource Group
Propably Databricks needs to be setup in a new Resource Group. The following link explains how to setup a new Resource Group: [step-by-step-guide-creating-an-azure-resource-group-on-azure](https://techcommunity.microsoft.com/t5/startups-at-microsoft/step-by-step-guide-creating-an-azure-resource-group-on-azure/ba-p/3792368). 

### Create Databricks Application
To create a databricks application in Azure the following steps can be followed:
https://learn.microsoft.com/en-us/azure/databricks/scenarios/quickstart-create-databricks-workspace-vnet-injection#create-an-azure-databricks-workspace

### Databricks setup
In Databricks several configurations need to be setup. Mainly to setup a cluster that has the SparkConnector and Python Driver installed. 

#### Create a cluster
Databricks need a cluster to execute code. The cluster can be created as follows
- Go to Compute.
- Create cluster with minimum settings (note Spark and Scala version). 

#### Install Spark Connector for Neo4j
To use the SparkConnector in Databricks this must be installed on the computational cluster. The steps are described below. 

- Go to Compute -> Select cluster -> Libraries -> Install new
- Select Maven packages
- Search for "neo4j-connector-apache-spark"
- Select and install depending on compability with Spark and Scala version (Compability can be found here: https://neo4j.com/docs/spark/current/overview/#_spark_compatibility)

Another option is to download the JAR itself and upload it manually. The JAR-file can be found here: https://spark-packages.org/?q=neo4j-connector-apache-spark

#### Install Python Driver for Neo4j
The Python Driver for Neo4j also need to installed on the cluster. This can be simply done with PIP in a Notebook. However, it might be better to install this on startup of the cluster as follows:

- Go to Compute -> Select cluster -> Libraries -> Install new
- Select PyPI packages
- Type: "neo4j" (potentially with a version number)
- Press install 

### Create Azure Storage Account
For data storage a Azure Storage Account can be deployed. For Deploying a Storage account the following can be created: [storage-account-create](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-create?toc=/azure/storage/blobs/toc.json&bc=/azure/storage/blobs/breadcrumb/toc.json). 

### Create Azure Key-Vault 
For storing my user credentials a Azure KeyVault can be used that must be connected to Databricks secrets. I just followed the following tutorial: https://youtu.be/9VzBS4OiP_A

Steps:
- Install databricks cli in local terminal: 
``` pip3 install databricks-cli```
- Create a token in Databricks
- Type: ```databricks configure --token```
- Give databricks URL: https://adb-xxxx.x.azuredatabricks.net/
- Type: ```databricks secrets list-scopes```
- Create a scope by: ```secrets create-scope --scope <YOUR_SCOPE_NAME>```
- Copy your url and modify it to the following format: https://adb-xxxx.x.azuredatabricks.net/?o=xxxx#secrets/createScope
- Create a key-vault in your resource group
- Copy url and resource id in the desired boxes.
- Verify the scope by typing the following in databricks-cli: ```databricks secrets list-scopes ```

For this demo I stored the following in the Azure KeyVault: 
- Neo4j Username: The username used for accessing the Neo4j-database.
- Neo4j Password: The password used for accessing the Neo4j-database.
- Storage Account Name: The name of the Storage Account used. 
- Storage Account Access Key: The Access key of the Storage Account used.

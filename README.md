**The Challenge: Data Warehouse and Data Pipeline**

The Dataset has several pipe delimited gzipped files of raw
data. The names of the files start with either hier or fact to signify whether they have
hierarchy (dimension) or fact data. The word following hier or fact indicates the table
name for the raw data. Each file has a header row with column names.
The hier files have id and label columns for each level in the hierarchy. For the most
part you can assume that the left most column is the primary key, but you should
ensure that you draw out a proper structure by looking at the many-to-one relationships
that the data manifests.

1. You must draw out an ER diagram showing raw table structure and any
relationships between them that you can infer using column names. You may
use schema inference tools, but you must document what you used and why.
You must add the final ER diagram and any documentation explaining it to your
submission’s Github repository.

3. You must build a pipeline that
a. Loads this raw data into the data warehouse from external storage such
as Azure Blobs, AWS S3 or the like. You must write basic checks such as
non-null, uniqueness of primary key, data types. Also check for foreign
key constraints between fact and dimension tables. Do it for at least one
hier (dimension), and one fact table.
b. Create a staging schema where the hierarchy table has been normalized
into a table for each level and the staged fact table has foreign key
relationships with those tables.
c. Create a refined table called mview_weekly_sales which aggregates
sales_units, sales_dollars, and disocunt_dollars by pos_site_id, sku_id,
fsclwk_id, price_substate_id and type

I have explained the detailed approach of the solution, that I have taken for the above use cases as follows:

**Goal:**
Load source data files (dimensional and fact tables) into SnowFlake's data warehouse using external stages (Azure) and transform(aggregating) the needed data using DBT and load the final aggregated value back to SNowflake's data warehouse.

**Tech Stack:**

**Azure Blobs:** To store source files - hierarchy (dimension) and fact data.

**Snowflake:** Data warehouse

**DBT:** For data transformation and aggregation - by means of models and materialized views

**Implementation:**

**ER Diagram:** The Entity-Relationships between dimensional and fact tables for the given dataset is available in the ER Diagram file

1. Create a Storage account in Azure to hold the dimensions and facts files

   ![image](https://github.com/hendrysophia/dbt_project/assets/22212468/1802e3eb-86bf-4f5e-91cc-0e8c9c2b01f2)

2. Create a data warehouse in snowflake:

   I have created a trial snowflake account with XS DWH size

   ![image](https://github.com/hendrysophia/dbt_project/assets/22212468/0030a3e3-3dfc-4c4d-aa9b-32e4592dfbd5)

3.Create a database to store data, also setup external stages and file formats 

  ![image](https://github.com/hendrysophia/dbt_project/assets/22212468/ead2baee-0275-4cb5-a18d-13df4d6f75f1)

4. Loading the stages data to the database:
Now that the data is staged to external Azure blob, used the COPY COMMAND to load all the files into snowflake

  ![image](https://github.com/hendrysophia/dbt_project/assets/22212468/4902d99b-d43d-402c-b3be-99fe35d64afa)

5. Establish the connection to DBT from snowflake using 'Partner Connect'

   ![image](https://github.com/hendrysophia/dbt_project/assets/22212468/fb111b48-4329-484f-b038-85c4621fec42)

6. Create Database and Schema to hold DBT Models -- Tables/Views in SNOWFLAKE

   ![image](https://github.com/hendrysophia/dbt_project/assets/22212468/663bfe41-860a-4e61-9444-49d543b8cfa8)

7. Create an trail account in DBT Cloud and setup the project for Data Transformations based on snowflake stage data

   ![image](https://github.com/hendrysophia/dbt_project/assets/22212468/200ffc6b-fe53-4214-a61b-462126b9eff2)

8. Create models for source staging (views) and transformed target aggregation (tables) and push back to snowflake

   ![image](https://github.com/hendrysophia/dbt_project/assets/22212468/6ec86395-b4a8-465e-8ac6-d30b4d59117c)

9. Ran tests for DBT models using **'dbt test'** command and dbt docs for the project is also generated using **'dbt docs generate'
**
   https://wx587.us1.dbt.com/accounts/70403103919197/develop/70403103976353/docs/index.html#!/overview 

11. Query the final aggregated table **(MVIEW_WEEKLY_SALES)** in snowflake

    ![image](https://github.com/hendrysophia/dbt_project/assets/22212468/e149374c-01f9-4a28-89a6-4ed8cfa22cca)

**Code Repo:**

All codes used for setting up snowflakes datapoints (DWH,DB,Schema,Tables) and Queries to validate all are available in the **"SNOWFLAKE_CODE" ** folder

And, for DBT its available in the respective folders of the repo.



 

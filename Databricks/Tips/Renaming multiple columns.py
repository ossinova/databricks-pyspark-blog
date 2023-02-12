# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Renaming multiple columns in Databricks using PySpark
# MAGIC 
# MAGIC As a Data Engineer, a common task is to rename columns from the source system into more clear and readable names.  
# MAGIC Often multiple renames are required. In this demo I will walk through a few options that aim to solve this. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Creating demo data
# MAGIC 
# MAGIC Lets first create some dummy data

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]
schema = StructType([ \
    StructField("fname",StringType(),True), \
    StructField("mname",StringType(),True), \
    StructField("lname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gen", StringType(), True), \
    StructField("sal", IntegerType(), True) \
  ])

df = spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Assuming you want to rename the following columns:
# MAGIC - fname -> FirstName
# MAGIC - mname -> MiddleName
# MAGIC - lname -> LastName
# MAGIC - id -> ID
# MAGIC - gen -> Gender
# MAGIC - sal -> Salary
# MAGIC 
# MAGIC There are numerous ways do so, some more suitable for more columns. I have listed a few alternatives that only add one step to the physical execution plan. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **Option 1)** Using .withColumnRenamed
# MAGIC 
# MAGIC - Quick and easy for one or more columns. Tedious for multiple. 

# COMMAND ----------

df_renamed_opt1 = (df
                   .withColumnRenamed('fname', 'FirstName')
                   .withColumnRenamed('mname', 'MiddleName')
                   .withColumnRenamed('lname', 'LastName')
                   .withColumnRenamed('id', 'ID')
                   .withColumnRenamed('gen', 'Gender')
                   .withColumnRenamed('sal', 'Salary')
                  )
# Display data
display(df_renamed_opt1)
# Explain physical execution plan
df_renamed_opt1.explain()

# COMMAND ----------

# MAGIC %md 
# MAGIC **Option 2.a)** Using a zipped dictionary; key, value lists
# MAGIC 
# MAGIC - Great for a list of columns

# COMMAND ----------

from pyspark.sql.functions import col

mapping = dict(zip(['fname', 'mname', 'lname', 'id', 'gen', 'sal'], # keys: original col(s) name
                   ['FirstName', 'MiddleName', 'LastName', 'ID', 'Gender', 'Salary'] # value: new col(s) name
                  ))

# Transform
df_renamed_opt2a = df.select([col(c).alias(mapping.get(c, c)) for c in df.columns])
# Display data
display(df_renamed_opt2a)
# Explain physical execution plan
df_renamed_opt2a.explain()

# COMMAND ----------

# MAGIC %md 
# MAGIC **Option 2.b)** Using a dictionary
# MAGIC - Great for multiple columns. Readable. 

# COMMAND ----------

mapping = {
    #'original' : 'renamed',
    'fname' : 'FirstName',
    'mname' : 'MiddleName',
    'lname' : 'LastName',
    'id' : 'ID',
    'gen' : 'Gender',
    'sal' : 'Salary'
}

# Transform
df_renamed_opt2b = df.select([col(c).alias(mapping[c]) for c in df.columns])
# Display data
display(df_renamed_opt2b)
# Explain physical execution plan
df_renamed_opt2b.explain()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC **Option 3.a)** Using SQL
# MAGIC 
# MAGIC If you are familiar with SQL you can query the data with the appropriate renaming of the columns.

# COMMAND ----------

# Create a temp view to query against
df.createOrReplaceTempView('employees')

# Query data
df_renamed_opt3a = spark.sql(
    """
    SELECT
        fname AS FirstName,
        mname AS MiddleName,
        lname AS LastName,
        id AS ID,
        gen AS Gender,
        sal AS Salary
    FROM 
        employees
    """
)

# Display data
display(df_renamed_opt3a)
# Explain physical execution plan
df_renamed_opt3a.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **Option 3.b)** 

# COMMAND ----------

df_renamed = df.withColumnRenamed('fname', 'FirstName')
df_renamed_2 = df_renamed.withColumnRenamed('mname', 'MiddleName')



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **Bonus)** 

# COMMAND ----------

#from pyspark.sql import DataFrame

# def transform(self, f):
#     return f(self)

# DataFrame.transform = transform


def renameColumns(df, cols):

    
#      rename_dict = {
#         'fname' : 'FirstName',
#         'mname' : 'MiddleName',
#         'lname' : 'LastName',
#         'id' : 'ID',
#         'gen' : 'Gender',
#         'sal' : 'Salary'
#         }

     return df.select([col(c).alias(cols.get(c, c)) for c in df.columns])

rename_dict = {
    'fname' : 'FirstName',
    'mname' : 'MiddleName',
    'lname' : 'LastName',
    'id' : 'ID',
    'gen' : 'Gender',
    'sal' : 'Salary'
    }
    
df_renamed = df.transform(renameColumns(df = df, cols = rename_dict))
display(df_renamed)

df_renamed.explain()

# COMMAND ----------

from pyspark.sql import DataFrame

def transform(self, f):
    return f(self)

DataFrame.transform = transform

def renameColumns(df, cols):

    
#      rename_dict = {
#         'fname' : 'FirstName',
#         'mname' : 'MiddleName',
#         'lname' : 'LastName',
#         'id' : 'ID',
#         'gen' : 'Gender',
#         'sal' : 'Salary'
#         }

     return df.select([col(c).alias(cols.get(c, c)) for c in df.columns])

rename_dict = {
    'fname' : 'FirstName',
    'mname' : 'MiddleName',
    'lname' : 'LastName',
    'id' : 'ID',
    'gen' : 'Gender',
    'sal' : 'Salary'
    }
    
df_renamed = df.transform(
    renameColumns(cols = rename_dict),
    #addColumns(cols = add_dict)
                         )
display(df_renamed)

df_renamed.explain()

# COMMAND ----------

transform

#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Importing SparkSession
from  pyspark.sql import SparkSession
from  pyspark.sql.functions import *
#Creating Spark Session Object
spark = SparkSession.builder.appName('data manipulation').getOrCreate()


# In[18]:


#Creating Data Frame for Sales Table
df_sales=spark.read.option('header','TRUE').csv('C:\\Users\\kanch\\Downloads\\Products\\Products\\sales.csv',inferSchema='TRUE')
df_sales.show()
df_sales.createOrReplaceTempView("sales")


# In[17]:


#Creating Data Frame for products 
df_products=spark.read.option('header','TRUE').csv('C:\\Users\\kanch\\Downloads\\Products\\Products\\products.csv',inferSchema='TRUE')
df_products.show()
df_products.createOrReplaceTempView("products")


# In[87]:


df_products.show()
df_products = df_products.withColumn('product sales',df_products.price*sales.num_pieces_sold)
df_products.show()


# In[113]:


# Creating a dataframe by using join and grouping with necessity columns 
from pyspark.sql.functions import max
df_prosales=df_sales.join(df_products,df_sales.product_id == df_products.product_id,"inner").groupby(df_products.product_id,df_products.price).agg(sum(df_sales.num_pieces_sold).alias('sales'))
df_prosales.show()


# In[129]:


# generate new column as total sales with the multiple of sales and price
df_prosales = df_prosales.withColumn('total_sales', 
                   df_prosales.price*df_prosales.sales)
df_prosales.show()


# In[136]:


#create a temp table as psa
df_prosales.show()
df_prosales.createOrReplaceTempView("psa")


# In[138]:


highest_sales = df_prosales.select("product_id","price","sales","total_sales").orderBy(desc("total_sales")).first()
highest_sales


# In[140]:


spark.sql(""" SELECT * 
              FROM psa 
              ORDER BY psa.total_sales DESC 
              LIMIT 5""").show()


# In[141]:


highest_sales = spark.sql(""" SELECT MAX(total_sales) 
                              FROM psa
                              """)
highest_sales.show()


# In[ ]:





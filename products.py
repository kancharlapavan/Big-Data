#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Importing SparkSession
from  pyspark.sql import SparkSession
from  pyspark.sql.functions import *
#Creating Spark Session Object
spark = SparkSession.builder.appName('data manipulation').getOrCreate()


# In[18]:


df_sales=spark.read.option('header','TRUE').csv('C:\\Users\\kanch\\Downloads\\Products\\Products\\sales.csv',inferSchema='TRUE')
df_sales.show()
df_sales.createOrReplaceTempView("sales")


# In[17]:


df_products=spark.read.option('header','TRUE').csv('C:\\Users\\kanch\\Downloads\\Products\\Products\\products.csv',inferSchema='TRUE')
df_products.show()
df_products.createOrReplaceTempView("products")


# In[ ]:


##df_products.with column('product sales',df_products['sales.num'])


# In[47]:


product_sales = spark.sql(""" SELECT sales.product_id,SUM(sales.num_pieces_sold * products.price) AS product_wise_sales 
                               FROM sales 
                               JOIN products ON sales.product_id = products.product_id 
                               GROUP BY sales.product_id""")


# In[48]:



product_sales.createOrReplaceTempView("psales")
product_sales.show()


# In[23]:


highest_sales = product_sales.select("product_id","product_wise_sales").orderBy(desc("product_wise_sales")).first()
highest_sales


# In[44]:


highest_sales = spark.sql(""" SELECT MAX(product_wise_sales) 
                              FROM psales
                              """)
highest_sales.show()


# In[60]:
changed by mahesh



highest_sales2 = product_sales.select("product_id","product_wise_sales").orderBy(desc("product_wise_sales")).limit(5)
highest_sales2.show()
highest_sales3 = spark.sql(""" SELECT * 
                                FROM psales 
                                ORDER BY psales.product_wise_sales DESC 
                                LIMIT 5""")
highest_sales3.show()


# In[40]:





# In[ ]:





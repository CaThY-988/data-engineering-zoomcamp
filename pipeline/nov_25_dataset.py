#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine
import click


# In[2]:


pg_user = 'root'
pg_pass = 'root'
pg_host = 'localhost'
pg_port = 5432
pg_db = 'ny_taxi'


# In[6]:


green_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'
zone_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'


# In[7]:


zone_df = pd.read_csv(zone_url)


# In[8]:


zone_df.dtypes


# In[10]:


zone_df.shape


# In[16]:


green_df = pd.read_parquet(green_url)


# In[19]:


green_df.to_csv("green_tripdata_2025-11.csv", index=False)


# In[20]:


green_df.dtypes


# In[23]:


engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')


# In[24]:


zone_df.to_sql(name='taxi_zone_lookup',con=engine,if_exists="replace")


# In[25]:


green_df.shape


# In[27]:


green_df.to_sql(name='green_tripdata_2025_11',con=engine,if_exists="replace")


# In[ ]:





# In[ ]:





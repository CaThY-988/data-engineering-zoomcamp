#!/usr/bin/env python
# coding: utf-8

# In[29]:


import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine
import click


# In[36]:


# Engine Vars
pg_user = 'root'
pg_pass = 'root'
pg_host = 'localhost'
pg_port = 5432
pg_db = 'ny_taxi'


# In[37]:


# Trip data URL vars
prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
year = 2025
month = 11
colour = 'green'
url = f'{prefix}{colour}_tripdata_{year}-{month:02d}.parquet'
target_table = f'{colour}_tripdata_{year}_{month:02d}'
csv_name = f'{target_table}.csv'


# In[38]:


# Create connection engine
engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')


# In[39]:


# Zone taxi data (CSV)
zone_df = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
zone_df = pd.read_csv(zone_df)
zone_df.to_sql(name='taxi_zone_lookup',con=engine,if_exists="replace")


# In[40]:


# Get trip data, check size, if small convert to csv, if big iterate chunks
df = pd.read_parquet(url, engine="pyarrow")
df.to_csv(csv_name, index=False)
zone_df.to_sql(name=target_table,con=engine,if_exists="replace")


# In[ ]:


uv run jupyter nbconvert --to=script notebook.ipynb
mv notebook.py ingest_data.py


# In[ ]:





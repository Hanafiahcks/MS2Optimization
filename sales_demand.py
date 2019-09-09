#%%
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
from psycopg2 import extras
from functools import partial


conn = psycopg2.connect(
    #host='172.17.224.27',
    host='localhost',
    database='ms2',
    user='postgres',
    #password='P@ssw0rd'
    password='D3waDB110'
    )

 df_t_stok = pd.read_sql("""SELECT id_spbu, datetime_stock,date(datetime_stock)tgl, 
                                (case when id_product = 2 then 7 else id_product end) id_product , stock 
                                FROM  opt.t_stock """, conn)

#%%
print(datama)
df_to_table(datama,'opt','t_sales_est_new')
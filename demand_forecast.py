#%%
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql,extras
from datetime import timedelta



conn = psycopg2.connect(
    #host='172.17.224.27',
    host='localhost',
    database='ms2',
    user='postgres',
    #password='P@ssw0rd'
    password='D3waDB110'
    )
dt='7/2/2019'
idtbbm=10

#def stok(dt,idtbbm):
df_t_stok = pd.read_sql(f"""select distinct on (id_spbu,id_product,datetime_stock::date) {idtbbm} id_tbbm,*,round(extract(epoch from interval '24h 1m'-datetime_stock::time)/3600) dt_1 from (
SELECT distinct on (id_spbu,id_product,datetime_stock)id_spbu,(case when id_product = 2 then 7 else id_product end) id_product , datetime_stock, upper(trim(to_char(datetime_stock,'day'))) day_stok,(datetime_stock)::time tm_stock,  
stock,stock/1000 stok  FROM  opt.t_stock where datetime_stock::date='{dt}' and  id_spbu in(select id_spbu from opt.v_spbu_distance where id_tbbm ={idtbbm})
order by id_spbu,id_product,datetime_stock desc) ashiap""", conn)
df_t_shift = pd.read_sql(f"""select *,round(extract(epoch from start_time/3600))dt_2 from opt.t_shift where id_tbbm={idtbbm}""", conn)
df_stokshift = pd.merge(df_t_stok,df_t_shift,on='id_tbbm',how='inner')
df_stokshift['dt']= df_stokshift.apply(
    lambda row : row['dt_1'] + row['dt_2'],axis =1)
del df_t_stok
del df_t_shift
#%%
df_t_sales_est = pd.read_sql(f"""SELECT upper(ac_date) day_stok,  id_spbu, id_product, sales_est FROM opt.t_sales_est_new""", conn)
df_ct = pd.merge(df_stokshift,df_t_sales_est,on=['id_spbu','id_product','day_stok'],how='left')
del df_stokshift
del df_t_sales_est
df_depot_spbu= pd.read_sql(f"""SELECT node_end as id_spbu, distance, depart_travel_time AS travel_time, shift
FROM opt.t_distance_travel_time WHERE node_start = {idtbbm} AND node_end IS NOT NULL""",conn)
df_ct2=pd.merge(df_ct,df_depot_spbu,on=['id_spbu','shift'],how='left')
df_ct2['travel_time']= df_ct2.apply(
    lambda row: 2 if (np.isnan(row['travel_time']) or row['travel_time']<0) else row['travel_time'],
    axis=1)
df_ct2['sales_est']= df_ct2.apply(
    lambda row: 0.33333 if (np.isnan(row['sales_est']) or row['sales_est']<0) else row['sales_est'],
    axis=1)
df_ct2['critical_time'] = df_ct2.apply(lambda row:(row['datetime_stock'] + timedelta(seconds=(((row['stok']/row['sales_est'])*3600)- (row['travel_time']*3600)))),axis=1)
df_spbu_cap= pd.read_sql(f"""select id_spbu, id_product, tank_capacity from opt.t_spbu_capacity""",conn)
df_demand_forecast=pd.merge(df_ct2,df_spbu_cap,on=['id_spbu','id_product'],how='left')
del df_ct
del df_ct2
del df_spbu_cap
del df_depot_spbu
df_demand_forecast = df_demand_forecast[['datetime_stock','id_spbu' , 'id_product' , 'critical_time' , 'stok' , 'sales_est' , 'shift', 'dt', 'tank_capacity']]
df_demand_forecast['tank_capacity']= df_demand_forecast.apply(
    lambda row: 45 if (np.isnan(row['tank_capacity']) or row['tank_capacity']<=0) else row['tank_capacity'],
    axis=1)
df_demand_forecast['stock']= df_demand_forecast.apply(
    lambda row: (0.9*row['tank_capacity']) if (row['stok'] >=row['tank_capacity']) else row['stok'],
    axis=1)
df_demand_forecast['max_request']= df_demand_forecast.apply(
    lambda row: row['tank_capacity'] - (row['stock'] - row['sales_est']*row['dt']),axis=1)
df_demand_forecast['max_request2']= df_demand_forecast.apply(
    lambda row: row['tank_capacity'] - (row['stock'] - row['sales_est']*(row['dt']+24)),axis=1)
df_demand_forecast['max_request3']= df_demand_forecast.apply(
    lambda row: row['tank_capacity'] - (row['stock'] - row['sales_est']*(row['dt']+48)),axis=1)
df_demand_forecast['max_request']= df_demand_forecast.apply(
    lambda row: row['tank_capacity'] if row['tank_capacity'] < row['max_request'] else row['max_request'],axis=1)
df_demand_forecast['max_request2']= df_demand_forecast.apply(
    lambda row: row['tank_capacity'] if row['tank_capacity'] < row['max_request2'] else row['max_request2'],axis=1)
df_demand_forecast['max_request3']= df_demand_forecast.apply(
    lambda row: row['tank_capacity'] if row['tank_capacity'] < row['max_request3'] else row['max_request3'],axis=1)
df_demand_forecast = df_demand_forecast[['datetime_stock','id_spbu' , 'id_product' , 'critical_time' , 'shift' , 'stock' , 'tank_capacity', 'max_request', 'max_request3']]
print(df_demand_forecast)
#%%

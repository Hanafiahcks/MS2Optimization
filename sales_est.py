import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
from psycopg2 import extras
from functools import partial


conn = psycopg2.connect(
    host='172.17.224.27',
    database='ms2',
    user='postgres',
    password='P@ssw0rd')

def ambil_kunci(skema,tabel):
    select_stmt= f"""select kc.column_name 
    from information_schema.table_constraints tc join information_schema.key_column_usage kc 
    on kc.table_name = tc.table_name and kc.table_schema = tc.table_schema and kc.constraint_name = tc.constraint_name
    where tc.constraint_type = 'PRIMARY KEY'
    and tc.table_name='{tabel}'
    and tc.table_schema='{skema}'
    and kc.ordinal_position is not null
    order by tc.table_schema,tc.table_name,kc.position_in_unique_constraint;"""
    cur = conn.cursor()
    cur.execute(select_stmt)
    hasil = zip(*cur.fetchall())
    cur.close()
    nilai = ','.join([str(i) for i in hasil])
    return nilai

def df_to_table(datfram,skema,tabel):
    df_columns = list(datfram)
    columns = ",".join(df_columns)
    values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 
    updat = ', '.join([f'{col} = EXCLUDED.{col}' for col in df_columns])
    hasil = ambil_kunci(skema,tabel)
    kunci = hasil.replace("'","")
    insert_stmt = "INSERT INTO {}.{} ({}) {} on conflict {} do update set {}".format(skema,tabel,columns,values,kunci,updat)
    cur = conn.cursor()
    print(f"input data ke {tabel}")
    psycopg2.extras.execute_batch(cur, insert_stmt, datfram.values)
    conn.commit()
    cur.close()

        
def dayaverage(t_gi_lag):
    # recreate t_gi_lag
    print("Proses daysaverage")
    t_gi_lag['gi_date'] = pd.to_datetime(t_gi_lag['gi_date'])
    t_gi_lag['ac_date_day'] = t_gi_lag['gi_date'].dt.strftime("%A")
    t_gi_lag['opr_time_'] = t_gi_lag['opr_time']
    t_gi_lag['ac_date'] = t_gi_lag['ac_date_day']
    t_gi_lag['lag'] = t_gi_lag.groupby(['id_spbu' , 'id_product'])['gi_date'].shift(1)
    t_gi_lag['dif'] = (t_gi_lag.gi_date-t_gi_lag.lag).apply(lambda x: x.days)
    # recreate t_gi_lag1
    t_gi_lag1tmp = t_gi_lag.dropna(subset=['dif']).drop(columns=['gi_date'])
    t_gi_lag1tmp = t_gi_lag1tmp[['opr_time_' , 'ac_date_day' , 'lag' , 'dif' , 'id_spbu' , 'id_product' , 'quantity' , 'ac_date' , 'opr_time']]
    t_gi_lag1tmp.rename(columns = {'lag':'ac_dt'} , inplace = True)
    t_gi_lag1_ = t_gi_lag1tmp.assign(**{'{}'.format(col ): t_gi_lag1tmp[col].shift(1)
        for col in ('id_spbu' , 'id_product' , 'quantity' , 'ac_date' , 'opr_time')})
    t_gi_lag1 = t_gi_lag1_.dropna(subset = ['id_spbu'])
    t_gi_lag1['sales'] = t_gi_lag1tmp.quantity/(t_gi_lag1tmp.dif*t_gi_lag1tmp.opr_time)
    del t_gi_lag1tmp
    del t_gi_lag1_
    #recreate t_sales_est
    t_sales_est_ = t_gi_lag1[(t_gi_lag1.dif<= 7)]
    t_sales_est = t_sales_est_.groupby([ 'id_spbu' , 'id_product' , 'ac_date_day']).agg({'sales':'mean' , 'opr_time':'min'}).reset_index()
    t_sales_est.rename(columns = {'ac_date_day':'ac_date' , 'sales':'sales_est'} , inplace=True)
    del t_sales_est_
    t_sales_all = t_gi_lag[['id_spbu' , 'id_product' , 'quantity' , 'opr_time']]
    t_sales_all = t_sales_all.groupby([ 'id_spbu' , 'id_product']).agg({'quantity':'mean' , 'opr_time':'min'}).reset_index()
    t_sales_all['rata2_qu'] = t_sales_all.quantity/t_sales_all.opr_time
    t_sales_all['koljoin'] = 1
    t_sales_all = t_sales_all[['id_spbu' , 'id_product' , 'rata2_qu' , 'koljoin']]
    hari = pd.DataFrame({"ac_date":["Wednesday" , "Friday" , "Saturday" , "Thursday" , "Sunday" , "Monday" , "Tuesday"] , "koljoin":[1 , 1 , 1 , 1 , 1 , 1 , 1]})
    t_sales_allx = pd.merge(t_sales_all , hari , on = 'koljoin' , how = 'inner')
    #recreate t_sales_all_
    t_sales_all_ = t_sales_allx.drop(columns = ['koljoin'])
    del hari
    del t_sales_allx
    #recreate t_sales_all_j
    sales_est_j = pd.merge(t_sales_est , t_sales_all_ , on = ['id_spbu' , 'id_product' , 'ac_date'])
    sales_est_j = sales_est_j.dropna(subset = ['sales_est' , 'rata2_qu'])
    sales_est_jall = sales_est_j[sales_est_j['sales_est'].notnull()]
    sales_est_jall['sales_est']  =  sales_est_jall.apply(
        lambda row: row['rata2_qu'] if (np.isnan(row['sales_est']) or row['sales_est'] == 0 ) else row['sales_est'] , 
        axis = 1)
    sales_est_jall['src'] =  'DA'
    sales_est_jall = sales_est_jall.drop(columns = ['rata2_qu'])
    print("Proses day avarage done")
    return sales_est_jall

def moveavg(src_df):
    print("proses moving average mulai ")
    to_datetime_fmt = partial(pd.to_datetime , format='%m/%d/%Y %H:%M')
    src_df['date_time'] = src_df['date_time'].apply(to_datetime_fmt)
    src_df['ac_date'] = src_df['date_time'].dt.strftime("%A")
    src_df['tanggal'] = pd.to_datetime(src_df['date_time'])
    src_df['tanggal'] = src_df['tanggal'].dt.date
    src_df = src_df[['id_spbu' , 'id_product' , 'tanggal' , 'ac_date' , 'sales' , 'opr_time']]
    src_df = src_df.sort_values(by=['id_spbu' , 'id_product' , 'tanggal' , 'ac_date'])
    grdata = src_df.groupby(['id_spbu' , 'id_product' , 'tanggal' , 'ac_date'])['sales'].sum().reset_index()
    grdata2 = grdata.groupby(['id_spbu' , 'id_product'])['sales'].rolling(3).mean().reset_index()
    madata = pd.merge(grdata , grdata2.iloc[: , 3] , left_index=True , right_index=True , how='inner')
    madata.rename(columns={'sales_y':'sales_est'} , inplace=True)
    madata['src']='MA'
    madata['opr_time']=18
    madata = madata.dropna(subset=['sales_est'])
    madata = madata.groupby(['id_spbu' , 'id_product']).tail(7)
    datama = madata.drop(columns=['sales_x' , 'tanggal'])
    print("Proses Moving Average selesai")
    return datama


dfdaysavg = pd.read_sql(
            f"SELECT a.id_spbu, a.id_product, a.gi_date, SUM(a.quantity) AS quantity,min(b.opr_time) AS opr_time"
	        +" FROM opt.t_gi a LEFT JOIN opt.t_spbu b ON a.id_spbu=b.id_spbu"
            +" GROUP BY a.id_spbu, a.id_product, a.gi_date"
            +" ORDER BY a.id_spbu, a.id_product, a.gi_date", conn)

df_to_table(dayaverage(dfdaysavg),'opt','t_sales_est_new')
del dfdaysavg

src_df = pd.read_sql(
            f"select date_time, b.id id_spbu,b.allday opr_time, dispenser, nozzle, c.id_jenis_bbm id_product, tipe_pembayaran, shift, sales from public.atg_stock a left join web.m_spbu b on a.no_spbu = replace(b.spbu_no,'.','') "
            +" left join web.ref_jenis_bbm c on upper(replace(a.produk,'_','')) = upper(c.material_name)" , conn)
df_to_table(moveavg(src_df),'opt','t_sales_est_new')
print("selesai")


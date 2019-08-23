# To add a new cell, type '#%%'
# To add a new markdown cell, type '#%% [markdown]'
## membuat estimasi berdasaran moving average
# created By H.RuslanHadi


# estimasi sales based on moving avarage
# sumber data atg (belum selesai) sementara dari csv yang ada -- belum selesai
# load data 
# hitung moving avarage
# insert data temporary ke table public.t_salesest_ma -- untuk kebutuhan checking pak hisyam
# insert data to table opt.t_salesestimate dengan kode ma
# 
# a
# ambil data


#%%

import csv
import os
import glob
import pandas as pd
from initialization.dataloader import DataLoader

#basePath = os.path.dirname(os.path.abspath(__file__))
#pathfile = basePath + "/sumber/*.csv"
pathfile = "/home/hanhan/Project/MS2Optimization/sumber/*.csv"
daftarfile = glob.glob(pathfile)

#print(daftarfile)  # for check listing csv

# kombinasi semua csv
combicsv = pd.concat([pd.read_csv(f) for f in daftarfile])
# formating dan buat kolom tanggal untuk grouping

combicsv['date_time'] = pd.to_datetime(combicsv['date_time'], format='%m/%d/%Y %H:%M')
combicsv['tanggal'] = combicsv['date_time'].dt.date
combicsv = combicsv[['no_spbu', 'produk', 'tanggal','sales']]
combicsv = combicsv.sort_values(by=['no_spbu', 'produk', 'tanggal'])
grdata = combicsv.groupby(['no_spbu', 'produk', 'tanggal'])['sales'].sum().reset_index()
#grdata['moveavg'] = grdata.groupby(['no_spbu', 'produk'])['sales'].rolling(3).mean().reset_index()
#grdate['sumber']="ma"
print(grdata)
pd.merge(grdata, grdata2.iloc[:, 3],
         left_index=True, right_index=True, how='inner')

# spbuind=grdata.no_spbu.unique()
# for nospbu in spbuind:
#     grdatafilter=grdata[grdata.no_spbu==nospbu]
#     produkind = grdatafilter.produk.unique()
# #%%
#     for produk in produkind:
#         grdatafilter2 = grdatafilter[grdatafilter.produk == produk]
#         grdatafilter2['moveavg'] = grdatafilter2['sales'].rolling(3).mean()
#         grdataakhir = pd.concat([grdata, grdatafilter2]).drop_duplicates(['no_spbu', 'produk', 'tanggal','sales','moveavg'],keep='last')

# print(grdata)
#%%

# CLASS : DATALOADER
# DESCRIPTION : load all input table required in optimisation process to Class
# author : KHOIRUSH AKBAR
import psycopg2
import pandas as pd
import pandas.io.sql as psql
import datetime as dt
import os
import json

basePath = os.path.dirname(os.path.abspath(__file__))
config_file_path = basePath + "/config.json"
assump_file_path = basePath + '/assumption.json'
time_variable_file_path = basePath + '/time_variable.json'
with open(config_file_path, 'r') as datafile:
    CONFIG = json.load(datafile)
with open(assump_file_path, 'r') as datafile:
    ASSUMPTION = json.load(datafile)
with open(time_variable_file_path, 'r') as datafile:
    TIMEVARIABLE = json.load(datafile)

print("initialize DataLoader package")


def get_date(str_date):
    obj_date = dt.datetime.strptime(str_date, '%Y-%m-%d')
    return obj_date.date()


def get_time(str_time):
    obj_date = dt.datetime.strptime(str_time, '%H:%M:%S')
    return obj_date.time()


class DataLoader():
    id_tbbm = ''
    plan_date = ''
    timevariable = {}

    def __init__(self, con=CONFIG):
        # read parameter
        self.DL_CONFIG = con
        self.timevariable = TIMEVARIABLE
        self.read_parameter()
        # open connection to DB
        conn = psycopg2.connect(database=con['db'], user=con['user'],
                                password=con['password'], host=con['host'], port=con['port'])
        # self.df_spbu = pd.read_sql(
        #     "select * from opt.t_spbu", conn)
        self.df_spbu = pd.read_excel(basePath+"/clean_data_spbu.xlsx") # for development purpose spbu master read from xlsx 
        self.df_spbu_tank_capacity = pd.read_sql(
            "select * from opt.t_spbu_capacity", conn)
        self.df_good_issues = pd.read_sql(
            f"select * from opt.t_gi where id_tbbm = {self.id_tbbm} and plan_date = '{self.plan_date}'", conn)
        self.df_tbbm_shift = pd.read_sql(
            f"select * from opt.t_shift where id_tbbm = {self.id_tbbm}", conn, index_col='shift')
        self.df_initial_stock = pd.read_sql(
            "select * from opt.t_initial_stock", conn)
        self.df_historical_stock = pd.read_sql(
            "select * from opt.t_historical_stock", conn)
        self.df_distance_traveltime = pd.read_sql(
            f"select * from opt.t_distance_travel_time where id_tbbm={self.id_tbbm}", conn)
        # -----------Sampai sini kebutuhan data untuk demand forecast---------
        self.df_demand = pd.read_sql(
            f"select * from opt.t_demand where id_tbbm = {self.id_tbbm} and plan_date = '{self.plan_date}'", conn)
        # query ke demand forecast perlu di ganti nanti, sekarang ambil dari histori demand forecast for testing purpose
        # self.df_demand_forecast = pd.read_sql(
        #     "select * from opt.t_demand_forecast10", conn)
        self.df_demand_forecast = pd.read_sql(
            f"select * from opt.t_historical_demand_forecast where forecast_date='{self.plan_date}'", conn)

        # dataframe demand forecast bisa baca dari existing table,
        # tapi lebih baik langsung dari hasil proses demand forecast yang di run saat optimasi
        # harus buat table baru untuk menampung demand forecast, semua demand forecast dalam 1 table

        # pembentukan sales estimate lama based on script exorty
        self.df_data_gi = pd.read_sql(
            f"SELECT a.id_tbbm, a.id_spbu, a.id_product, a.gi_date, SUM(a.quantity) AS quantity,min(b.opr_time) AS opr_time"
	        +" FROM opt.t_gi a LEFT JOIN opt.t_spbu b ON a.id_spbu=b.id_spbu"
            +" GROUP BY a.id_tbbm, a.id_spbu, a.id_product, a.gi_date"
            +" ORDER BY a.id_tbbm, a.id_spbu, a.id_product, a.gi_date", conn)


        self.df_truck = pd.read_sql(
            f"select * from opt.t_truck where id_tbbm = {self.id_tbbm}", conn)
        self.df_tbbm_capacity = pd.read_sql(
            f"select * from opt.t_supply_point where id_tbbm={self.id_tbbm} and date='{self.plan_date}'", conn)
        self.df_tbbm_filling_bay = pd.read_sql(
            f"select * from opt.t_filling_point where id_tbbm={self.id_tbbm}", conn)
        self.df_travel_path = pd.read_sql(
            "select * from opt.travel_path_tegal", conn)
        self.df_truck_compartment = pd.read_sql(
            f"select * from web.m_truck where id_tbbm ={self.id_tbbm}", conn)
        self.DF_DICT = {            # this dictionary purpose is to call all df in iteration loop
            "demand": self.df_demand,
            "demand_forecast": self.df_demand_forecast,
            "distance_traveltime": self.df_distance_traveltime,
            "good_issues": self.df_good_issues,
            "historical_stock": self.df_historical_stock,
            "initital_stock": self.df_initial_stock,
            "spbu": self.df_spbu,
            "spbu_tank_capacity": self.df_spbu_tank_capacity,
            "tbbm_capacity": self.df_tbbm_capacity,
            "tbbm_filling_bay": self.df_tbbm_filling_bay,
            "tbbm_shift": self.df_tbbm_shift,
            "travel_path": self.df_travel_path,
            "truck": self.df_truck,
            "truck_compartment": self.df_truck_compartment
        }
        self.data_treatment()

    def read_parameter(self):
        if self.DL_CONFIG['id_tbbm'].upper() != 'X':  # testing mode
            self.id_tbbm = self.DL_CONFIG['id_tbbm']
        else:
            # read parameter id_tbbm dari rest api (trigger dari web app)
            pass

        if self.DL_CONFIG['plan_date'].upper() != 'X':  # testing mode
            self.plan_date = self.DL_CONFIG['plan_date']
        else:
            # read parameter plan_date dari rest api (trigger dari web app)
            pass
        # print(f"TBBM : {self.id_tbbm}, plan_date : {self.plan_date}")

    def handle_missing_data(self, export=False, fillna=True):
        # dd_ = dirty data nomenklatur
        list_dd = []
        for df_name, df_col in ASSUMPTION.items():
            # print(f"key : {df_name}, col_df:{df_col}")
            for col, ass_value in df_col.items():
                # print(
                #     f"key : {df_name}, key:{col}, assumption value:{ass_value}")
                # get each dataframe from assumption json to be filtered and filled
                df = self.DF_DICT[df_name]
                dd = df[df[col].isna()]  # dd is dirty data
                if export:
                    dd.to_excel(f"dirty_data_{col}.xlsx")
                if fillna:
                    # special handling on open_time dan close_time
                    if col == 'open_time' or col == 'close_time':
                        pass
                    else:
                        df[col].fillna(value=ass_value, inplace=True)

    def data_treatment(self):
        # treatment to CONFIG : convert plan date type from string to date object
        str_plan_date = self.DL_CONFIG['plan_date']
        obj_plan_date = dt.datetime.strptime(str_plan_date, '%Y-%m-%d')
        self.DL_CONFIG['plan_date'] = obj_plan_date.date()
        self.plan_date = obj_plan_date.date()
        # treatment to df if needed, for example add column from datetime -> date only
        self.df_demand_forecast['date'] = self.df_demand_forecast['datetime_stock'].dt.date

    def spbu_data_treatment(self, export=False):
        # treatment for open time and close time based on operation time
        for idx, rows in self.df_spbu.iterrows():
            if rows['open_time'] == None or rows['close_time'] == None:
                if rows['opr_time'] == 24:
                    self.df_spbu.loc[idx, 'open_time'] = '00:00:00'
                    self.df_spbu.loc[idx, 'close_time'] = '23:59:00'
                elif rows['opr_time'] <= 18:
                    self.df_spbu.loc[idx, 'open_time'] = '06:00:00'
                    close = 5+int(rows['opr_time'])
                    self.df_spbu.loc[idx, 'close_time'] = f"{close}:59:00"
                else:
                    open_time = 24+int(rows['opr_time'])
                    self.df_spbu.loc[idx, 'open_time'] = f'{open_time}:00:00'
                    self.df_spbu.loc[idx, 'close_time'] = f"23:59:00"
            if rows['opr_time'] < 0:
                self.df_spbu.loc[idx, 'opr_time'] = 24+rows['opr_time']
        if export:
            self.df_spbu.to_excel("clean_data_spbu.xlsx")

    def __str__(self):
        for key, df in self.DF_DICT.items():
            print(f"{key} size : {len(df.index)}")
            print(df.head())
        return ""

#         # testing class
# if __name__ == "__main__":
#     str_time = '00:00:00'
#     print("time :", get_time(str_time))
#     print('type: ', type(get_time(str_time)))

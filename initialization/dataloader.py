# CLASS : DATALOADER
# DESCRIPTION : load all input table required in optimisation process to Class
# author : KHOIRUSH AKBAR
import psycopg2
import pandas as pd
import pandas.io.sql as psql
from __init__ import CONFIG


class DataLoader():
    DL_CONFIG = CONFIG
    id_tbbm = ''
    plan_date = ''

    def __init__(self, con=CONFIG):
        # read parameter
        # self.DL_CONFIG = CONFIG
        self.read_parameter()
        # open connection to DB
        conn = psycopg2.connect(database=CONFIG['db'], user=CONFIG['user'],
                                password=CONFIG['password'], host=CONFIG['host'], port=CONFIG['port'])
        self.df_spbu = pd.read_sql(
            "select * from opt.t_spbu", conn)
        self.df_spbu_tank_capacity = pd.read_sql(
            "select * from opt.t_spbu_capacity", conn)
        self.df_good_issues = pd.read_sql(
            f"select * from opt.t_gi where id_tbbm = {self.id_tbbm} and plan_date = '{self.plan_date}'", conn)
        self.df_tbbm_shift = pd.read_sql(
            f"select * from opt.t_shift where id_tbbm = {self.id_tbbm}", conn)
        self.df_initial_stock = pd.read_sql(
            "select * from opt.t_initial_stock", conn)
        self.df_historical_stock = pd.read_sql(
            "select * from opt.t_historical_stock", conn)
        self.df_distance_traveltime = pd.read_sql(
            f"select * from opt.t_distance_travel_time where id_tbbm={self.id_tbbm}", conn)
        # -----------Sampai sini kebutuhan data untuk demand forecast---------
        self.df_demand = pd.read_sql(
            f"select * from opt.t_demand where id_tbbm = {self.id_tbbm} and plan_date = '{self.plan_date}'", conn)
        self.df_demand_forecast = pd.read_sql(
            "select * from opt.t_demand_forecast10", conn)
        # dataframe demand forecast bisa baca dari existing table,
        # tapi lebih baik langsung dari hasil proses demand forecast yang di run saat optimasi
        # harus buat table baru untuk menampung demand forecast, semua demand forecast dalam 1 table

        self.df_truck = pd.read_sql(
            f"select * from opt.t_truck where id_tbbm = {self.id_tbbm}", conn)
        self.df_tbbm_capacity = pd.read_sql(
            f"select * from opt.t_supply_point where id_tbbm={self.id_tbbm}", conn)
        self.df_tbbm_filling_bay = pd.read_sql(
            f"select * from opt.t_filling_point where id_tbbm={self.id_tbbm}", conn)
        self.df_travel_path = pd.read_sql(
            "select * from opt.travel_path_tegal", conn)
        pass

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
        print(f"TBBM : {self.id_tbbm}, plan_date : {self.plan_date}")


# testing class
if __name__ == "__main__":
    DL = DataLoader(CONFIG)
    print(DL.df_tbbm_shift)

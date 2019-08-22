# CLASS : TRUCK
# DESCRIPTION : data structure to contain truck and detail truck compartment
# author : KHOIRUSH AKBAR

from .dataloader import DataLoader
import pandas as pd


class Truck():

    def __init__(self, id, nopol, cap, n_comp, l_comp, s_comp, pto=False):
        self.id_truck = id
        self.nopol = nopol
        self.capacity = cap
        self.n_compartment = n_comp
        self.l_compartment = l_comp
        self.s_compartment = s_comp
        self.b_PTO = pto
        self.rit_count = 0

    def __str__(self):
        return f"[{self.id_truck},{self.nopol},{self.capacity},{self.n_compartment},{self.l_compartment},{self.s_compartment},{self.b_PTO},{self.rit_count}]"


class TruckPool():

    def __init__(self, source=DataLoader):
        self.df_truck = source.df_truck
        self.df_truck_compartment = source.df_truck_compartment
        list_truck = []
        list_compartment = []
        for idx, rows in self.df_truck_compartment.iterrows():
            if rows['capacity'] > 0:
                list_compartment = [rows[f"capacity_compartment_{i}"] for i in range(
                    1, 7) if rows[f"capacity_compartment_{i}"] > 0]
                bool_compartment = [0 for i in range(
                    1, 7) if rows[f"capacity_compartment_{i}"] > 0]
                list_truck.append(Truck(
                    rows['id'], rows['no_truck'], rows['capacity'], rows['compartment'], list_compartment, bool_compartment))
        self.truck_pool = list_truck

    def __str__(self):
        str_output = ""
        for tr in self.truck_pool:
            str_output += "\n" + str(tr)
        return str_output

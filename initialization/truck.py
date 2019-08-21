# CLASS : TRUCK
# DESCRIPTION : data structure to contain truck and detail truck compartment
# author : KHOIRUSH AKBAR

from .dataloader import DataLoader


class Truck():

    def __init__(self, source=DataLoader):
        self.df_truck = source.df_truck
        self.df_truck['rit_count'] = 0
        self.df_truck_compartment = source.df_truck_compartment
        self.dict_queue = {}
        # self.dict_
        pass

from initialization.order import Order
from initialization.truck import TruckPool
from initialization.dataloader import DataLoader


class VehicleRouter():

    def __init__(self, order=Order, truck=TruckPool):
        self.list_order = order
        self.list_truck = truck
        self.rit_record = []
        print(self.list_order)
        print(self.list_truck)
        print(self.list_order.df_order.columns)
        pass

    def find_truck(self, dataloader=DataLoader):
        # df_spbu
        for idx, order in self.list_order.df_order.iterrows():

            # if order.
            pass

    def find_truck_prio(self):
        pass

    def add_order_to_truck(self):
        pass

    def truck_processing(self):
        pass

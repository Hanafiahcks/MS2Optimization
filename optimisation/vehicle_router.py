from initialization.order import Order
from initialization.truck import TruckPool, Truck
from initialization.dataloader import DataLoader
import pandas as pd


class VehicleRouter():

    def __init__(self, order=Order, truck=TruckPool):
        self.list_order = order
        self.list_truck = truck
        self.rit_record = pd.DataFrame(
            columns=['id_tbbm', 'order_id', 'id_spbu', 'id_product', 'quantity', 'id_truck', 'depart_time', 'return_time'])
        print(self.list_order)
        print(self.list_truck)
        print(self.list_order.df_order.columns)
        pass

    def find_truck(self, dataloader=DataLoader):
        # df_spbu
        truck = None
        for idx, order in self.list_order.df_order.iterrows():
            truck = self.list_truck.get_truck_same_cap(order['quantity'])
            if truck == None:  # means not found
                id_truck = self.list_truck.get_truck_more_cap(
                    order['quantity'])
            assign_truck(order['order_id'], truck)
            if truck.remain == 0:
                self.truck_processing(truck)
            else:
                self.find_order(truck)

            # if order.
            pass

    def find_truck_prio(self):
        pass

    def assign_truck(self, order_id, truck=Truck):
        pass

    def find_order(self, truck=Truck):
        pass

    def truck_processing(self, truck=Truck):
        truck.go()
        self.remove_order_from_list(truck)
        pass

    def remove_order_from_list(self, truck=Truck):
        pass

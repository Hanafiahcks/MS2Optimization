import initialization.dataloader as dl
import initialization.order as order
from initialization.truck import Truck, TruckPool
from optimisation.vehicle_router import VehicleRouter

if __name__ == "__main__":
    DL = dl.DataLoader()
    DL.handle_missing_data(export=False, fillna=True)
    demand = order.Order(DL)
    TP = TruckPool(DL)
    VRouter = VehicleRouter(demand, TP)
    # print(truck_pool)
    print(DL.df_truck.columns)

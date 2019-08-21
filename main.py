import pandas as pd
import initialization.dataloader as dl
import initialization.order as order


if __name__ == "__main__":
    DL = dl.DataLoader()
    DL.handle_missing_data(export=False, fillna=True)
    demand = order.Order(DL)

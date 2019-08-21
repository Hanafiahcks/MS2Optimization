import pandas as pd
import initialization.dataloader as dl
import initialization.order as order


if __name__ == "__main__":
    DL = dl.DataLoader()
    DL.handle_missing_data(export=False, fillna=True)
    # demand = order.Order(DL)
    print("opr_time: ", len(DL.df_spbu[DL.df_spbu['opr_time'].isna()]))
    print("open_time: ", len(DL.df_spbu[DL.df_spbu['open_time'].isna()]))
    print("close_time: ", len(DL.df_spbu[DL.df_spbu['close_time'].isna()]))
    print("max_cap: ", len(DL.df_spbu[DL.df_spbu['max_cap'].isna()]))
    print("from total rows of : ", len(DL.df_spbu))

    print(DL.df_spbu[DL.df_spbu['opr_time'] > 16])
    DL.df_spbu.info()

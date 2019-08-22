# CLASS : ORDER
# DESCRIPTION : data structure to contain order from t_demand and all the processing function
# author : KHOIRUSH AKBAR
from .dataloader import DataLoader, pd
from datetime import datetime


class Order():
    def __init__(self, source=DataLoader):
        self.df_order = source.df_demand[['plan_date',
                                          'order_id', 'id_spbu', 'id_product', 'recommended_shift', 'quantity', 'stock']]
        self.merge_critical_time(source)
        self.merge_distance(source)
        self.merge_max_cap(source)
        self.merge_max_truck(source)
        # print(self.df_order.columns)
        self.sort_by()
        # filter and separate order from priority spbu and outstanding
        # self.df_prio_order = self.split_prio_order()

        # cumulative order
        # self.df_cum_order = self.df_order
        # self.aggregate_order()
    def __str__(self):
        return str(self.df_order)

    def __get__(self):
        return self.df_order

    def merge_max_truck(self, source=DataLoader):
        # df_tr = source.df_truck[]
        max_truck = source.df_truck['capacity'].max()
        self.df_order['max_truck'] = max_truck

    def merge_max_cap(self, source=DataLoader):
        self.df_order = pd.merge(self.df_order, source.df_spbu[['id_spbu', 'max_cap']],
                                 how='left', on=['id_spbu'])
        pass

    def merge_critical_time(self, source=DataLoader):
        self.df_order = pd.merge(self.df_order, source.df_demand_forecast[['id_spbu', 'id_product', 'critical_time']],
                                 how='left', on=['id_spbu', 'id_product'])

    def merge_distance(self, source=DataLoader):
        def get_distance(self, id_tbbm=10, id_spbu=0):
            mask = (source.df_distance_traveltime['node_start'] == float(id_tbbm)) & (
                source.df_distance_traveltime['node_end'] == float(id_spbu))
            try:
                return source.df_distance_traveltime[mask].iloc[0]['distance']
            except IndexError:
                speed = source.timevariable[id_tbbm]["speed"]
                traveltime = source.timevariable[id_tbbm]["distance_hours"]
                return speed*traveltime  # return 2jam * speed

        self.df_order['distance'] = 0
        for idx, rows in self.df_order.iterrows():
            dist = get_distance(self,
                                id_tbbm=source.id_tbbm, id_spbu=rows['id_spbu'])
            # print(
            #     f"from {source.id_tbbm} | to {rows['id_spbu']} | dist: {dist}")
            self.df_order.loc[idx, 'distance'] = dist

    def sort_by(self, export=False):
        self.df_order = self.df_order.sort_values(
            by=['recommended_shift', 'distance', 'critical_time'], ascending=[True, True, True])
        if export:
            self.df_order.to_excel("sorted_order.xlsx")
    # def aggregate_order(self):
    #     # self.df_cum_order.groupby
    #     pass

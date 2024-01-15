# from sklearn.cluster import KMeans
import pandas as pd
import numpy as np
import requests
import joblib
# import json
import utils.config as cfg
import os


class Load:
    def __init__(self):

        self.routine_model = joblib.load(os.path.join(cfg.root_path, 'models', 'routine_customers_model_95.pkl'))
        self.nroutine_model = joblib.load(os.path.join(cfg.root_path, 'models', 'nroutine_customers_model_95.pkl'))
        self.after_sales_model = joblib.load(os.path.join(cfg.root_path, 'models', 'after_sales_model_95_5.pkl'))

        # get the cluster centers
        routine_centers = self.routine_model.cluster_centers_
        nroutine_centers = self.nroutine_model.cluster_centers_
        after_sales_centers = self.after_sales_model.cluster_centers_

        # create the rank maps
        self.routine_rank_map = self.create_rank_map(routine_centers)
        self.nroutine_rank_map = self.create_rank_map(nroutine_centers)
        self.after_sales_rank_map = self.create_rank_map(after_sales_centers)

    @staticmethod
    def create_rank_map(centers):
        # sort the cluster centers by their values
        sorted_indices = np.argsort(centers.sum(axis=1))[::-1]
        # create the rank map
        rank_map = {i: rank + 1 for rank, i in enumerate(sorted_indices)}
        # return the rank map
        return rank_map

    def predict_class(self, customer_type, routine_customers=None, nroutine_customers=None, after_sales_customers=None):

        if customer_type == 2756:
            if not routine_customers.empty:
                routine_customers['cluster'] = self.routine_model.predict(routine_customers[['normalizedCLV']])
                routine_customers['Rank'] = routine_customers['cluster'].map(self.routine_rank_map)
            if not nroutine_customers.empty:
                nroutine_customers['cluster'] = self.nroutine_model.predict(nroutine_customers[['normalizedCLV']])
                nroutine_customers['Rank'] = nroutine_customers['cluster'].map(self.nroutine_rank_map)
            return routine_customers, nroutine_customers

        if customer_type == 2757:
            if not after_sales_customers.empty:
                after_sales_customers['cluster'] = self.after_sales_model.predict(
                    after_sales_customers[['normalizedCLV']])
                after_sales_customers['Rank'] = after_sales_customers['cluster'].map(self.after_sales_rank_map)

            return after_sales_customers

    def load_data(self, result):

        url = "https://api.havayar.com/Crm/UpdateCustomerRank"

        payload = f"{result}"
        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        if response.status_code == 200:
            return response

    def run(self, customer_type, routine_customers=None, nroutine_customers=None, after_sales_customers=None):

        if customer_type == 2756:
            routine_customers, nroutine_customers = self.predict_class(customer_type, routine_customers,
                                                                       nroutine_customers)
            data = pd.concat([routine_customers, nroutine_customers])
        elif customer_type == 2757:
            after_sales_customers = self.predict_class(customer_type, None, None, after_sales_customers)
            data = after_sales_customers

        result = []
        for index, row in data.iterrows():
            result.append({'CustomerCode': int(row['Customer_Code']), 'CustomerLength': row['LengthDays'],
                           'CustomerRecency': row['RecencyDays'], 'CustomerFrequency': row['Frequency'],
                           'CustomerMoney': row['MoneyDollar'], 'CustomerClv': row['normalizedCLV'],
                           'CustomerNormalizedClv': row['normalizedCLV'], 'RankId': int(row['Rank']),
                           'IsRoutine': row['IsRoutine'], 'SalesTypeId': int(row['SalesTypeId'])})

        return self.load_data(result)

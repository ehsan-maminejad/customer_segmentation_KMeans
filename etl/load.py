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
        # self.routine_model = joblib.load(cfg.root_path + '/models/routine_customers_model.pkl')
        # self.nroutine_model = joblib.load(cfg.root_path + '/models/nroutine_customers_model.pkl')
        # # self.routine_rank_map = {2: 1, 4: 2, 5: 3, 1: 4, 3: 5, 0: 6}
        # # self.nroutine_rank_map = {3: 1, 1: 2, 0: 3, 2: 4}
        # self.routine_rank_map = {5: 1, 2: 2, 1: 3, 4: 4, 0: 5, 3: 6}
        # self.nroutine_rank_map = {0: 3, 1: 1, 2: 4, 3: 2}
        self.routine_model = joblib.load(os.path.join(cfg.root_path, 'models', 'routine_customers_model.pkl'))
        self.nroutine_model = joblib.load(os.path.join(cfg.root_path, 'models', 'nroutine_customers_model.pkl'))

        # get the cluster centers
        routine_centers = self.routine_model.cluster_centers_
        nroutine_centers = self.nroutine_model.cluster_centers_

        # create the rank maps
        self.routine_rank_map = self.create_rank_map(routine_centers)
        self.nroutine_rank_map = self.create_rank_map(nroutine_centers)

    @staticmethod
    def create_rank_map(centers):
        # sort the cluster centers by their values
        sorted_indices = np.argsort(centers.sum(axis=1))[::-1]
        # create the rank map
        rank_map = {i: rank + 1 for rank, i in enumerate(sorted_indices)}
        # return the rank map
        return rank_map

    def predict_class(self, routine_customers, nroutine_customers):

        if not routine_customers.empty:
            routine_customers['cluster'] = self.routine_model.predict(routine_customers[['normalizedCLV']])
            routine_customers['Rank'] = routine_customers['cluster'].map(self.routine_rank_map)
        if not nroutine_customers.empty:
            nroutine_customers['cluster'] = self.nroutine_model.predict(nroutine_customers[['normalizedCLV']])
            nroutine_customers['Rank'] = nroutine_customers['cluster'].map(self.nroutine_rank_map)

        return routine_customers, nroutine_customers

    def load_data(self, result):

        url = "https://api.havayar.com/Crm/UpdateCustomerRank"

        payload = f"{result}"
        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        if response.status_code == 200:
            return response

    def run(self, routine_customers=None, nroutine_customers=None):

        routine_customers, nroutine_customers = self.predict_class(routine_customers, nroutine_customers)

        data = pd.concat([routine_customers, nroutine_customers])
        result = []
        for index, row in data.iterrows():
            result.append({'CustomerCode': int(row['Customer_Code']), 'CustomerLength': row['LengthDays'],
                           'CustomerRecency': row['RecencyDays'], 'CustomerFrequency': row['Frequency'],
                           'CustomerMoney': row['MoneyDollar'], 'CustomerClv': row['normalizedCLV'],
                           'CustomerNormalizedClv': row['normalizedCLV'], 'RankId': int(row['Rank']),
                           'IsRoutine': row['IsRoutine'], 'SalesTypeId': int(row['SalesTypeId'])})

        return self.load_data(result)

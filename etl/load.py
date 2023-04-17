# from sklearn.cluster import KMeans
import pandas as pd
import requests
import joblib
# import json
import utils.config as cfg


class Load:
    def __init__(self):
        self.routine_model = joblib.load(cfg.root_path+'/models/routine_customers_model.pkl')
        self.nroutine_model = joblib.load(cfg.root_path+'/models/nroutine_customers_model.pkl')
        self.routine_rank_map = {5: 1, 2: 2, 1: 3, 4: 4, 0: 5, 3: 6}
        self.nroutine_rank_map = {0: 3, 1: 1, 2: 4, 3: 2}

    def predict_class(self, routine_customers, nroutine_customers):

        if not routine_customers.empty:
            routine_customers['cluster'] = self.routine_model.predict(routine_customers[['normalizedCLV']])
            routine_customers['rank'] = routine_customers['cluster'].map(self.routine_rank_map)
        if not nroutine_customers.empty:
            nroutine_customers['cluster'] = self.nroutine_model.predict(nroutine_customers[['normalizedCLV']])
            nroutine_customers['rank'] = nroutine_customers['cluster'].map(self.nroutine_rank_map)

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
            result.append({'CustomerCode': int(row['customer_Code']), 'CustomerLength': row['lengthDays'],
                           'CustomerRecency': row['recencyDays'], 'CustomerFrequency': row['frequency'],
                           'CustomerMoney': row['moneyDollar'], 'CustomerClv': row['normalizedCLV'],
                           'CustomerNormalizedClv': row['normalizedCLV'], 'RankId': row['rank'],
                           'IsRoutine': row['IsRoutine']})


        return self.load_data(result)
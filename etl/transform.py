import pandas as pd
import utils.config as cfg
from etl.extract import to_date
from utils.date_conversion import convert_date as dc
from utils.normalizer import Normalizer


# import utils.normalizer as norm


class Transform:
    def __init__(self):
        self.routine_weights = pd.read_excel(
            cfg.root_path+'/components/WEIGHT.xlsx', sheet_name='routine')

        self.normoutine_weights = pd.read_excel(
            cfg.root_path+'/components/WEIGHT.xlsx', sheet_name='non-routine')

        self.base_date = dc(to_date)

        self.routine_max_values = {'lengthDays': 6916, 'recencyDays': 6906, 'frequency': 145,
                                   'moneyDollar': 6456486.86824}

        self.routine_min_values = {'lengthDays': 1, 'recencyDays': 1, 'frequency': 1,
                                   'moneyDollar': 1.24644}

        self.nroutine_max_values = {'lengthDays': 6896, 'recencyDays': 5469, 'frequency': 282,
                                    'moneyDollar': 36119915.05504}

        self.nroutine_min_values = {'lengthDays': 169, 'recencyDays': 1, 'frequency': 1,
                                    'moneyDollar': 511544.20762}

    def discriminate_rn_type(self, grouped, customers_money):
        routinetype_max = grouped['routineType'].max().rename(columns={'routineType': 'routineTypeMax'})

        df_join = pd.merge(routinetype_max,
                           customers_money, on='customer_Code')

        routine_customers = df_join[(df_join['routineTypeMax'] == 0) | ((df_join['routineTypeMax']
                                                                         != 0) & (df_join['moneyDollar'] <= 501960.0))]

        nroutine_customers = df_join[(df_join['routineTypeMax']
                                      != 0) & (df_join['moneyDollar'] > 501960.0)]

        return routine_customers['customer_Code'], nroutine_customers['customer_Code']

    def calculate_money(self, grouped):
        customers_money = grouped['netPriceInDollar'].sum().rename(columns={'netPriceInDollar': 'moneyDollar'})

        return customers_money

    def calculate_length(self, grouped):
        min_date = grouped['factorDate'].min().rename(columns={'factorDate': 'minDate'})

        # change type of minDate to datetime
        # min_date['minDate'] = pd.to_datetime(min_date['minDate'])

        min_date['lengthDays'] = (pd.Timestamp(
            self.base_date)-min_date['minDate']).dt.days

        return min_date.drop('minDate', axis=1)

    def calculate_recency(self, grouped):
        max_date = grouped['factorDate'].max().rename(columns={'factorDate': 'maxDate'})
        # change type of maxDate to datetime
        # max_date['maxDate'] = pd.to_datetime(max_date['maxDate'])

        max_date['recencyDays'] = (pd.Timestamp(
            self.base_date)-max_date['maxDate']).dt.days

        return max_date.drop('maxDate', axis=1)

    def calculate_frequency(self, grouped):
        return grouped.size().rename(columns={'size': 'frequency'})

    def cal_normclv_routine(self, routine_customers=None):
            norm_result = routine_customers[
                ['normalizedLengthDays', 'normalizedRecencyDays', 'normalizedMoneyDollar', 'normalizedFrequency']].mul(
                self.routine_weights.values.flatten(), axis=1)
            norm_result['normalizedCLV'] = norm_result.sum(axis=1)
            routine_customers = pd.concat([routine_customers, norm_result['normalizedCLV']], axis=1)
            return routine_customers

    def cal_normclv_nroutine(self, nroutine_customers=None):
            norm_result = nroutine_customers[
                ['normalizedLengthDays', 'normalizedRecencyDays', 'normalizedMoneyDollar', 'normalizedFrequency']].mul(
                self.normoutine_weights.values.flatten(), axis=1)
            norm_result['normalizedCLV'] = norm_result.sum(axis=1)
            nroutine_customers = pd.concat([nroutine_customers, norm_result['normalizedCLV']], axis=1)
            return nroutine_customers

    def run(self, data):
        customers_df = pd.DataFrame.from_records(data)
        customers_df['factorDate'] = pd.to_datetime(customers_df['factorDate'])

        grouped = customers_df.groupby('customer_Code', as_index=False)

        customers_money = self.calculate_money(grouped)

        # discriminate routine and non-routine customers
        routine_customers, nroutine_customers = self.discriminate_rn_type(
            grouped, customers_money)

        customers_length = self.calculate_length(grouped)

        customers_recency = self.calculate_recency(grouped)

        customers_frequency = self.calculate_frequency(grouped)

        cols_to_normalize = {'lengthDays': 'normalizedLengthDays', 'frequency': 'normalizedFrequency',
                             'moneyDollar': 'normalizedMoneyDollar'}

        ### Normalizing section ###
        norm = Normalizer()
        if not routine_customers.empty:
            routin_join = pd.merge(routine_customers, customers_money, on='customer_Code')
            routin_join = pd.merge(routin_join, customers_length, on='customer_Code')
            routin_join = pd.merge(routin_join, customers_frequency, on='customer_Code')
            routin_join = pd.merge(routin_join, customers_recency, on='customer_Code')

            routine_customers = pd.merge(routin_join, grouped.first().reset_index(), on='customer_Code',
                                         how='left').loc[:,
                                ['customer_Code', 'fullname', 'lengthDays', 'recencyDays', 'frequency', 'moneyDollar']]

            # normalizing lfm parameters for routine customers
            for col_name, new_name in cols_to_normalize.items():
                routine_customers[new_name] = routine_customers[
                    col_name].apply(norm.normalize_lfm, args=(
                    self.routine_max_values[col_name], self.routine_min_values[col_name])).round(3)

            # normalizing recency
            routine_customers[['normalizedRecencyDays']] = routine_customers[
                ['recencyDays']].apply(norm.normalize_recency, args=(
                self.routine_max_values['recencyDays'], self.routine_min_values['recencyDays'])).round(3)

            # calculate normalized CLV of each customer
            routine_customers = self.cal_normclv_routine(routine_customers)

            routine_customers['IsRoutine'] = 'true'

        if not nroutine_customers.empty:
            normoutine_join = pd.merge(nroutine_customers, customers_money, on='customer_Code')
            normoutine_join = pd.merge(normoutine_join, customers_length, on='customer_Code')
            normoutine_join = pd.merge(normoutine_join, customers_frequency, on='customer_Code')
            normoutine_join = pd.merge(normoutine_join, customers_recency, on='customer_Code')

            nroutine_customers = pd.merge(normoutine_join, grouped.first().reset_index(), on='customer_Code',
                                          how='left').loc[
                                 :,
                                 ['customer_Code', 'fullname', 'lengthDays', 'recencyDays', 'frequency', 'moneyDollar']]

            # normalizing lfm parameters for routine customers
            for col_name, new_name in cols_to_normalize.items():
                nroutine_customers[new_name] = \
                    nroutine_customers[
                        col_name].apply(norm.normalize_lfm, args=(
                        self.nroutine_max_values[col_name], self.nroutine_min_values[col_name])).round(3)

            # normalizing recency
            nroutine_customers[['normalizedRecencyDays']] = nroutine_customers[
                ['recencyDays']].apply(norm.normalize_recency, args=(
                self.nroutine_max_values['recencyDays'], self.nroutine_min_values['recencyDays'])).round(3)

            # calculate normalized CLV of each customer
            nroutine_customers = self.cal_normclv_nroutine(nroutine_customers)

            nroutine_customers['IsRoutine'] = 'false'


        # # calculate normalized CLV of each customer
        # routine_customers, nroutine_customers = self.calculate_normalized_clv(routine_customers, nroutine_customers)

        return routine_customers, nroutine_customers

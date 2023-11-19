import pandas as pd
import utils.config as cfg
from etl.extract import to_date
from utils.date_conversion import convert_date as dc
from utils.normalizer import Normalizer
from dataclasses import dataclass


@dataclass
class CustomerRange:
    lengthDays: int
    recencyDays: int
    frequency: int
    moneyDollar: float


class Transform:
    def __init__(self):
        self._load_weights()
        self._set_base_date()
        self._set_max_min_values()

    def _load_weights(self):
        weights = pd.read_excel(f"{cfg.root_path}/components/WEIGHT.xlsx", sheet_name=None)
        self.routine_weights = weights.get('routine')
        self.normoutine_weights = weights.get('non-routine')

    def _set_base_date(self):
        self.base_date = dc(to_date)

    def _set_max_min_values(self):
        self.routine_max_values = CustomerRange(7043, 7033, 90, 6456486.86824)
        self.routine_min_values = CustomerRange(1, 1, 1, 7.51073)
        self.nroutine_max_values = CustomerRange(7023, 5596, 282, 36119915.05504)
        self.nroutine_min_values = CustomerRange(268, 16, 1, 511544.20762)

    def discriminate_rn_type(self, grouped, customers_money):
        df_join = (grouped['routineType']
                   .max()
                   .rename(columns={'routineType': 'routineTypeMax'})
                   .pipe(pd.merge, customers_money, on='customer_Code'))

        routine_customers = df_join.query("routineTypeMax == 0 or (routineTypeMax != 0 and moneyDollar <= 501960.0)")
        nroutine_customers = df_join.query("routineTypeMax != 0 and moneyDollar > 501960.0")

        return routine_customers['customer_Code'], nroutine_customers['customer_Code']

    def calculate_money(self, grouped):
        customers_money = grouped['netPriceInDollar'].sum().rename(columns={'netPriceInDollar': 'moneyDollar'})
        return customers_money

    def calculate_length(self, grouped):
        min_date = grouped['factorDate'].min().rename(columns={'factorDate': 'minDate'})
        min_date['lengthDays'] = (pd.Timestamp(
            self.base_date) - min_date['minDate']).dt.days
        return min_date.drop('minDate', axis=1)

    def calculate_recency(self, grouped):
        max_date = grouped['factorDate'].max().rename(columns={'factorDate': 'maxDate'})
        max_date['recencyDays'] = (pd.Timestamp(
            self.base_date) - max_date['maxDate']).dt.days
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

    def process_customers(self, data):
        customers_df = pd.DataFrame.from_records(data)
        customers_df['factorDate'] = pd.to_datetime(customers_df['factorDate'])
        grouped = customers_df.groupby('customer_Code', as_index=False)
        customers_money = self.calculate_money(grouped)
        routine_customers, nroutine_customers = self.discriminate_rn_type(grouped, customers_money)
        customers_length = self.calculate_length(grouped)
        customers_recency = self.calculate_recency(grouped)
        customers_frequency = self.calculate_frequency(grouped)

        if not routine_customers.empty:
            routine_join = pd.merge(routine_customers, customers_money, on='customer_Code')
            routine_join = pd.merge(routine_join, customers_length, on='customer_Code')
            routine_join = pd.merge(routine_join, customers_frequency, on='customer_Code')
            routine_join = pd.merge(routine_join, customers_recency, on='customer_Code')
            routine_customers = pd.merge(routine_join, grouped.first().reset_index(), on='customer_Code',
                                         how='left').loc[:,
                                ['customer_Code', 'fullname', 'lengthDays', 'recencyDays', 'frequency', 'moneyDollar']]

        if not nroutine_customers.empty:
            nroutine_join = pd.merge(nroutine_customers, customers_money, on='customer_Code')
            nroutine_join = pd.merge(nroutine_join, customers_length, on='customer_Code')
            nroutine_join = pd.merge(nroutine_join, customers_frequency, on='customer_Code')
            nroutine_join = pd.merge(nroutine_join, customers_recency, on='customer_Code')
            nroutine_customers = pd.merge(nroutine_join, grouped.first().reset_index(), on='customer_Code',
                                          how='left').loc[:,
                                 ['customer_Code', 'fullname', 'lengthDays', 'recencyDays', 'frequency', 'moneyDollar']]

        return routine_customers, nroutine_customers

    def normalize_customer_data(self, customers, lrfm_max_values, lrfm_min_values, norm):
        cols_to_normalize = {'lengthDays': 'normalizedLengthDays', 'frequency': 'normalizedFrequency',
                             'moneyDollar': 'normalizedMoneyDollar'}

        for col_name, new_name in cols_to_normalize.items():
            max_value = getattr(lrfm_max_values, col_name)
            min_value = getattr(lrfm_min_values, col_name)
            customers[new_name] = customers[col_name].apply(norm.normalize_lfm,
                                                            args=(max_value, min_value)
                                                            ).round(3)
        customers[['normalizedRecencyDays']] = customers[['recencyDays']].apply(norm.normalize_recency,
                                                                                args=(lrfm_max_values.recencyDays,
                                                                                      lrfm_min_values.recencyDays)
                                                                                ).round(3)
        return customers

    def calculate_normalized_clv(self, customers, weights):
        norm_result = customers[
            ['normalizedLengthDays', 'normalizedRecencyDays', 'normalizedMoneyDollar', 'normalizedFrequency']
        ].mul(weights.values.flatten(), axis=1)
        norm_result['normalizedCLV'] = norm_result.sum(axis=1)
        customers = pd.concat([customers, norm_result['normalizedCLV']], axis=1)
        return customers

    def normalize_and_calculate_clv(self, customers, max_values, min_values, weights, norm):
        customers = self.normalize_customer_data(customers, max_values, min_values, norm)
        customers = self.calculate_normalized_clv(customers, weights)
        return customers

    def run(self, data):
        routine_customers, nroutine_customers = self.process_customers(data)

        norm = Normalizer()
        if not routine_customers.empty:
            routine_customers = self.normalize_and_calculate_clv(routine_customers, self.routine_max_values,
                                                                 self.routine_min_values, self.routine_weights, norm)
            routine_customers['IsRoutine'] = 'true'

        if not nroutine_customers.empty:
            nroutine_customers = self.normalize_and_calculate_clv(nroutine_customers, self.nroutine_max_values,
                                                                  self.nroutine_min_values, self.normoutine_weights,
                                                                  norm)
            nroutine_customers['IsRoutine'] = 'false'

        return routine_customers, nroutine_customers

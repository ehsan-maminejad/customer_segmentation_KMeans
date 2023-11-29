import pandas as pd
import utils.config as cfg
from etl.extract import to_date
from utils.date_conversion import convert_date as dc
from utils.normalizer import Normalizer
from dataclasses import dataclass


@dataclass
class CustomerRange:
    LengthDays: int
    RecencyDays: int
    Frequency: int
    MoneyDollar: float


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
        df_join = (grouped['RoutineType']
                   .max()
                   .rename(columns={'RoutineType': 'RoutineTypeMax'})
                   .pipe(pd.merge, customers_money, on='Customer_Code'))

        routine_customers = df_join.query("RoutineTypeMax == 0 or (RoutineTypeMax != 0 and MoneyDollar <= 501960.0)")
        nroutine_customers = df_join.query("RoutineTypeMax != 0 and MoneyDollar > 501960.0")

        return routine_customers['Customer_Code'], nroutine_customers['Customer_Code']

    def calculate_money(self, grouped):
        customers_money = grouped['NetPriceInDollar'].sum().rename(columns={'NetPriceInDollar': 'MoneyDollar'})
        return customers_money

    def calculate_length(self, grouped):
        min_date = grouped['FactorDate'].min().rename(columns={'FactorDate': 'MinDate'})
        min_date['MinDate'] = pd.to_datetime(min_date['MinDate'], utc=True)
        min_date['MinDate'] = min_date['MinDate'].dt.tz_localize(None)
        min_date['LengthDays'] = (pd.Timestamp(
            self.base_date) - min_date['MinDate']).dt.days
        return min_date.drop('MinDate', axis=1)

    def calculate_recency(self, grouped):
        max_date = grouped['FactorDate'].max().rename(columns={'FactorDate': 'MaxDate'})
        max_date['MaxDate'] = pd.to_datetime(max_date['MaxDate'], utc=True)
        max_date['MaxDate'] = max_date['MaxDate'].dt.tz_localize(None)
        max_date['RecencyDays'] = (pd.Timestamp(
            self.base_date) - max_date['MaxDate']).dt.days
        return max_date.drop('MaxDate', axis=1)

    def calculate_frequency(self, grouped):
        return grouped.size().rename(columns={'size': 'Frequency'})

    def cal_normclv_routine(self, routine_customers=None):
        norm_result = routine_customers[
            ['NormalizedLengthDays', 'NormalizedRecencyDays', 'NormalizedMoneyDollar', 'NormalizedFrequency']].mul(
            self.routine_weights.values.flatten(), axis=1)
        norm_result['normalizedCLV'] = norm_result.sum(axis=1)
        routine_customers = pd.concat([routine_customers, norm_result['normalizedCLV']], axis=1)
        return routine_customers

    def cal_normclv_nroutine(self, nroutine_customers=None):
        norm_result = nroutine_customers[
            ['NormalizedLengthDays', 'NormalizedRecencyDays', 'NormalizedMoneyDollar', 'NormalizedFrequency']].mul(
            self.normoutine_weights.values.flatten(), axis=1)
        norm_result['normalizedCLV'] = norm_result.sum(axis=1)
        nroutine_customers = pd.concat([nroutine_customers, norm_result['normalizedCLV']], axis=1)
        return nroutine_customers

    def process_customers(self, data):
        customers_df = pd.DataFrame.from_records(data)
        customers_df['FactorDate'] = pd.to_datetime(customers_df['FactorDate'])
        grouped = customers_df.groupby('Customer_Code', as_index=False)
        customers_money = self.calculate_money(grouped)
        routine_customers, nroutine_customers = self.discriminate_rn_type(grouped, customers_money)
        customers_length = self.calculate_length(grouped)
        customers_recency = self.calculate_recency(grouped)
        customers_frequency = self.calculate_frequency(grouped)

        if not routine_customers.empty:
            routine_join = pd.merge(routine_customers, customers_money, on='Customer_Code')
            routine_join = pd.merge(routine_join, customers_length, on='Customer_Code')
            routine_join = pd.merge(routine_join, customers_frequency, on='Customer_Code')
            routine_join = pd.merge(routine_join, customers_recency, on='Customer_Code')
            routine_customers = pd.merge(routine_join, grouped.first().reset_index(), on='Customer_Code',
                                         how='left').loc[:,
                                ['Customer_Code', 'Fullname', "SalesTypeId", 'LengthDays', 'RecencyDays', 'Frequency',
                                 'MoneyDollar']]

        if not nroutine_customers.empty:
            nroutine_join = pd.merge(nroutine_customers, customers_money, on='Customer_Code')
            nroutine_join = pd.merge(nroutine_join, customers_length, on='Customer_Code')
            nroutine_join = pd.merge(nroutine_join, customers_frequency, on='Customer_Code')
            nroutine_join = pd.merge(nroutine_join, customers_recency, on='Customer_Code')
            nroutine_customers = pd.merge(nroutine_join, grouped.first().reset_index(), on='Customer_Code',
                                          how='left').loc[:,
                                 ['Customer_Code', 'Fullname', "SalesTypeId", 'LengthDays', 'RecencyDays', 'Frequency',
                                  'MoneyDollar']]

        return routine_customers, nroutine_customers

    def normalize_customer_data(self, customers, lrfm_max_values, lrfm_min_values, norm):
        cols_to_normalize = {'LengthDays': 'NormalizedLengthDays', 'Frequency': 'NormalizedFrequency',
                             'MoneyDollar': 'NormalizedMoneyDollar'}

        for col_name, new_name in cols_to_normalize.items():
            max_value = getattr(lrfm_max_values, col_name)
            min_value = getattr(lrfm_min_values, col_name)
            customers[new_name] = customers[col_name].apply(norm.normalize_lfm,
                                                            args=(max_value, min_value)
                                                            ).round(3)
        customers[['NormalizedRecencyDays']] = customers[['RecencyDays']].apply(norm.normalize_recency,
                                                                                args=(lrfm_max_values.RecencyDays,
                                                                                      lrfm_min_values.RecencyDays)
                                                                                ).round(3)
        return customers

    def calculate_normalized_clv(self, customers, weights):
        norm_result = customers[
            ['NormalizedLengthDays', 'NormalizedRecencyDays', 'NormalizedMoneyDollar', 'NormalizedFrequency']
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

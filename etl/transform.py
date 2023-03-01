import pandas as pd
import utils.config as cfg
from etl.extract import to_date
from utils.date_conversion import convert_date as dc
import utils.normalizer as nr


class Transform:
    def __init__(self):
        self.routine_weights = pd.read_excel(
            cfg.root_path+'/components/WEIGHT.xlsx', sheet_name='routine')

        self.nroutine_weights = pd.read_excel(
            cfg.root_path+'/components/WEIGHT.xlsx', sheet_name='non-routine')

        self.base_date = dc(to_date)

    def discriminate_rn_type(self, grouped, customers_money):
        routineType_max = grouped['routineType'].max().rename(columns={'routineType': 'routineTypeMax'})

        df_join = pd.merge(routineType_max,
                           customers_money, on='customer_Code')

        routine_customers = df_join[(df_join['routineTypeMax'] == 0) | ((df_join['routineTypeMax']
                                                                         != 0) & (df_join['moneyDollar'] <= 501960.0))]

        nroutin_customers = df_join[(df_join['routineTypeMax']
                                     != 0) & (df_join['moneyDollar'] > 501960.0)]

        return routine_customers['customer_Code'], nroutin_customers['customer_Code']

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

    def calculate_normalized_clv(self, routine_customers, nroutine_customers):

        ### normalized CLV for routine customers
        r_result = routine_customers[
            ['normalizedLengthDays', 'normalizedRecencyDays', 'normalizedMoneyDollar', 'normalizedFrequency']].mul(
            self.routine_weights.values.flatten(), axis=1)
        r_result['normalizedCLV'] = r_result.sum(axis=1)
        routine_customers = pd.concat([routine_customers, r_result['normalizedCLV']], axis=1)

        ### normalized CLV for nroutine customers
        nr_result = nroutine_customers[
            ['normalizedLengthDays', 'normalizedRecencyDays', 'normalizedMoneyDollar', 'normalizedFrequency']].mul(
            self.nroutine_weights.values.flatten(), axis=1)
        nr_result['normalizedCLV'] = nr_result.sum(axis=1)
        nroutine_customers = pd.concat([nroutine_customers, nr_result['normalizedCLV']], axis=1)

        return routine_customers, nroutine_customers

    def run(self, data):
        customers_df = pd.DataFrame.from_records(data)
        customers_df['factorDate'] = pd.to_datetime(customers_df['factorDate'])

        grouped = customers_df.groupby('customer_Code', as_index=False)

        customers_money = self.calculate_money(grouped)

        # discriminate routine and non-routine customers
        routine_customers, nroutin_customers = self.discriminate_rn_type(
            grouped, customers_money)

        customers_length = self.calculate_length(grouped)

        customers_recency = self.calculate_recency(grouped)

        customers_frequency = self.calculate_frequency(grouped)

        routin_join = pd.merge(routine_customers, customers_money, on='customer_Code')
        routin_join = pd.merge(routin_join, customers_length, on='customer_Code')
        routin_join = pd.merge(routin_join, customers_frequency, on='customer_Code')
        routin_join = pd.merge(routin_join, customers_recency, on='customer_Code')

        routine_customers = pd.merge(routin_join, grouped.first().reset_index(), on='customer_Code', how='left').loc[:,
                            ['customer_Code', 'fullname', 'lengthDays', 'recencyDays', 'frequency', 'moneyDollar']]

        cols_to_normalize = ['lengthDays', 'frequency', 'moneyDollar']
        # normalizing lfm parameters
        routine_customers[['normalizedLengthDays', 'normalizedFrequency', 'normalizedMoneyDollar']] = routine_customers[
            cols_to_normalize].apply(nr.normalize_col).round(3)

        # normalizing recency
        routine_customers[['normalizedRecencyDays']] = routine_customers[
            ['recencyDays']].apply(nr.normalize_recency).round(3)

        nroutin_join = pd.merge(nroutin_customers, customers_money, on='customer_Code')
        nroutin_join = pd.merge(nroutin_join, customers_length, on='customer_Code')
        nroutin_join = pd.merge(nroutin_join, customers_frequency, on='customer_Code')
        nroutin_join = pd.merge(nroutin_join, customers_recency, on='customer_Code')

        nroutine_customers = pd.merge(nroutin_join, grouped.first().reset_index(), on='customer_Code', how='left').loc[
                             :,
                             ['customer_Code', 'fullname', 'lengthDays', 'recencyDays', 'frequency', 'moneyDollar']]

        nroutine_customers[['normalizedLengthDays', 'normalizedFrequency', 'normalizedMoneyDollar']] = \
            nroutine_customers[
                cols_to_normalize].apply(nr.normalize_col)

        nroutine_customers[['normalizedRecencyDays']] = nroutine_customers[
            ['recencyDays']].apply(nr.normalize_recency).round(3)

        # calculate normalized CLV of each customer
        routine_customers, nroutine_customers = self.calculate_normalized_clv(routine_customers, nroutine_customers)

        return routine_customers, nroutine_customers

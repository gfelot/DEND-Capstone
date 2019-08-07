from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
from sqlalchemy import create_engine


class DumpCsvFileToPostgres(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, file_path, datatype, colsname, db_connect, table_name, *args, **kwargs):
        super(DumpCsvFileToPostgres, self).__init__(*args, **kwargs)
        self.file_path = file_path
        self.datatype = datatype
        self.colsname = colsname
        self.db_connect = db_connect
        self.table_name = table_name

    @staticmethod
    def converting_str_currency_to_int(col):
        col = col.str.slice(1)
        col = pd.to_numeric(col, downcast='float')
        col = col * 100
        col = col.round(0)
        return pd.to_numeric(col, downcast='unsigned')

    def execute(self):
        # step 1
        # Open and read CSV into a Dataframe
        self.log.info('Open and read CSV into a Dataframe')
        df = pd.read_csv(self.file_path, dtype=self.datatype, usecols=self.colsname)

        # step 2
        # Set 'Invoice/Item Number' as the index
        self.log.info('Set Invoice/Item Number as the index')
        df.rename(columns = {'Invoice/Item Number': 'id'}, inplace=True)
        df.set_index("id", inplace=True)

        # step 3
        # Convert column Date from str to datetime
        self.log.info('Convert column Date from str to datetime')
        df['Date'] = pd.to_datetime(df['Date'], infer_datetime_format=True)

        # step 4
        # Convert Zip Code to numeric and NaN mistyped values
        self.log.info('Convert Zip Code to numeric and NaN mistyped values')
        df['Zip Code'] = pd.to_numeric(df['Zip Code'], errors='coerce', downcast='float')

        # step 5
        # Dropping the column 'Store Location', I will geocode in the later
        self.log.info('Dropping the column Store Location')
        df = df.drop(columns=['Store Location'])

        # step 6
        # Converting Category to numeric
        self.log.info('Converting Category to numeric')
        df['Category'] = pd.to_numeric(df['Category'], errors='coerce', downcast='float')

        # step 7.1
        # Converting State Bottle Cost
        self.log.info('Converting State Bottle Cost')
        df['State Bottle Cost'] = self.converting_str_currency_to_int(df['State Bottle Cost'])

        # step 7.2
        # Converting State Bottle Retail
        self.log.info('Converting State Bottle Retail')
        df['State Bottle Retail'] = self.converting_str_currency_to_int(df['State Bottle Retail'])

        # step 7.3
        # Converting Sale (Dollars)
        self.log.info('Converting Sale (Dollars)')
        df['Sale (Dollars)'] = self.converting_str_currency_to_int(df['Sale (Dollars)'])
        df.rename(columns={'Sale (Dollars)': 'sales'}, inplace=True)

        # step 8
        # Convert Volume Sold (Liters)
        self.log.info('Convert Volume Sold (Liters)')
        df['Volume Sold (Liters)'] = df['Volume Sold (Liters)'] * 100
        df['Volume Sold (Liters)'] = df['Volume Sold (Liters)'].round(0)
        df['Volume Sold (Liters)'] = pd.to_numeric(df['Volume Sold (Liters)'], downcast='unsigned')
        df.rename(columns={'Volume Sold (Liters)': 'Volume Sold ml'}, inplace=True)

        # Last step -> dump file into postgres
        engine = create_engine(self.db_connect, echo=False)

        df.to_sql(self.table_name, con=engine, if_exists='replace', chunksize=4000)
import json
import datetime as dt
import unicodedata
import pandas as pd
import numpy as np

from io import StringIO
from abc import abstractmethod


class Csv_Transform:
    """
    Data pre-processing and transformation of extracted file.
    """

    def __init__(self, config):
        try:
            self.source = config.source_path
            self.destination = config.destination_path
        except Exception as e:
            print("Missing Path Exception: " + e)
            raise e

    def _transform(self):
        print("Transforming data!")
        df = pd.read_csv(self.source,  encoding="cp1252")

        new_columns = ['sales_date', 'address', 'county', 'sales_value', 'not_full_market_price_ind',
                       'vat_exclusive_ind', 'new_home_ind', 'quarantine_ind', 'quarantine_code', 'month_start']

        new_df = pd.DataFrame(columns=new_columns)

        # String standardization
        new_df['address'] = df['Address'].str.lower()
        new_df['county'] = df['County'].str.lower()

        for (columnName, columnData) in df.iteritems():

            # get rid of euros
            if columnName == 'Price (€)':
                new_df['sales_value'] = df['Price (€)'].str.replace(
                    ('[€,]'), '').astype('float')

            # date check
            if columnName == 'Date of Sale (dd/mm/yyyy)':
                arr = []
                for value in columnData.values:
                    dt_obj = dt.datetime.strptime(value, '%d/%m/%Y')
                    new_date = """{}/{}/{}""".format(dt_obj.day,
                                                     dt_obj.month, dt_obj.year)
                    arr.append(new_date)
                new_df['sales_date'] = arr

            # convert to numeric values
            if columnName == 'Not Full Market Price':
                arr = [1 if value.lower() ==
                       'yes' else 0 for value in columnData.values]
                new_df['not_full_market_price_ind'] = arr

            if columnName == 'VAT Exclusive':
                arr = [1 if value.lower() ==
                       'yes' else 0 for value in columnData.values]
                new_df['vat_exclusive_ind'] = arr

            # new home condition
            if columnName == 'Description of Property':
                new_df["new_home_ind"] = [1 if i == "New Dwelling house /Apartment" else 0 for i in
                                          df["Description of Property"]]

            # month Start
            month_start = []
            for date_value in new_df["sales_date"]:
                dt_obj = dt.datetime.strptime(date_value, '%d/%m/%Y')
                new_date = """1/{}/{}""".format(dt_obj.month, dt_obj.year)
                month_start.append(new_date)
            new_df["month_start"] = month_start

            # quarantine index
            # Create list of counties in Ireland
            county_list = ['cork', 'galway', 'mayo', 'donegal', 'kerry', 'tipperary', 'clare', 'tyrone', 'antrim', 'limerick', 'roscommon', 'down', 'wexford', 'meath', 'londonderry', 'kilkenny',
                           'wicklow', 'offaly', 'cavan', 'waterford', 'westmeath', 'sligo', 'laois', 'kildare', 'fermanagh', 'leitrim', 'armagh', 'monaghan', 'longford', 'dublin', 'carlow', 'louth']
            non_duplicate_records = new_df.drop_duplicates(
                subset=["address", "county", "sales_value"])
            duplicate_records = new_df[~new_df.isin(non_duplicate_records)]

            new_df["quarantine_ind"] = np.where(
                np.logical_and(new_df['not_full_market_price_ind']
                               == 1, new_df['new_home_ind'] == 1), 1,
                np.where(new_df['address'].isin(duplicate_records['address']), 1,
                         (np.where(~new_df['county'].isin(county_list), 1, 0))))

            # Quarantine_code
            # non unique =  Duplicate
            # invalid county = Invalid County
            new_df["quarantine_code"] = np.where(new_df['address'].isin(duplicate_records['address']), "Duplicate",
                                                 (np.where(~new_df['county'].isin(county_list), "Invalid County", "")))

        #new_df.reset_index(level=0, inplace=True)
        print("Transformation done on data")
        # print(new_df.head())
        new_df.to_csv(self.destination)
        print("Transformed data file is saved!")

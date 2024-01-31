import json

import pandas as pd
from tabulate import tabulate

from constants import KEYS_TICKER


def print_dataframe(p_dataframe):
    print(tabulate(p_dataframe, headers="keys", tablefmt="psql"))


def transform():
    # reading json file with raw data and converting to dataframe
    # f = open('data/raw.json')
    # data_companies = json.load(f)
    # df = pd.DataFrame(data_companies)
    # print(df.info())

    # reading csv file into dataframe
    df = pd.read_csv('data/raw.csv')

    # to display all columns
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    # prints top 5 columns
    # print(df.head())

    # Shows object type of data in each row
    # print(df.info())

    # shows count, unique, top, freq for non-numeric data
    # print(df.describe())

    print(df[KEYS_TICKER.RATING_OWNERSHIP_VALUE], df.dtypes[KEYS_TICKER.RATING_OWNERSHIP_VALUE])

    # Columns to clean:
    # RATING_FINSTAR,
    # RATING_OWNERSHIP_VALUE,RATING_VALUATION_VALUE, RATING_EFFICIENCY_VALUE, RATING_FINANCIALS_VALUE,
    # FV, DIV_YIELD,PROMOTER_HOLDING,
    # CARD4, CARD5, CARD6, CARD7

    df[KEYS_TICKER.RATING_FINSTAR] = df[KEYS_TICKER.RATING_FINSTAR].str.slice(20, 21).astype('float')
    df[KEYS_TICKER.RATING_OWNERSHIP_VALUE] = df[KEYS_TICKER.RATING_OWNERSHIP_VALUE].str.slice(20, 23).astype('float')
    df[KEYS_TICKER.RATING_VALUATION_VALUE] = df[KEYS_TICKER.RATING_VALUATION_VALUE].str.slice(20, 23).astype('float')
    df[KEYS_TICKER.RATING_EFFICIENCY_VALUE] = df[KEYS_TICKER.RATING_EFFICIENCY_VALUE].str.slice(20, 23).astype('float')
    df[KEYS_TICKER.RATING_FINANCIALS_VALUE] = df[KEYS_TICKER.RATING_FINANCIALS_VALUE].str.slice(20, 23).astype('float')

    df[KEYS_TICKER.FV] = df[KEYS_TICKER.FV].str.strip()
    df[KEYS_TICKER.FV] = df[KEYS_TICKER.FV].str[2:]
    df[KEYS_TICKER.FV] = df[KEYS_TICKER.FV].astype('float')

    df[KEYS_TICKER.DIV_YIELD] = df[KEYS_TICKER.DIV_YIELD].str.strip()
    df[KEYS_TICKER.DIV_YIELD] = df[KEYS_TICKER.DIV_YIELD].str[:-1]
    df[KEYS_TICKER.DIV_YIELD] = df[KEYS_TICKER.DIV_YIELD].str.strip()
    df[KEYS_TICKER.DIV_YIELD] = df[KEYS_TICKER.DIV_YIELD].astype('float')

    df[KEYS_TICKER.PROMOTER_HOLDING] = df[KEYS_TICKER.PROMOTER_HOLDING].str.strip()
    df[KEYS_TICKER.PROMOTER_HOLDING] = df[KEYS_TICKER.PROMOTER_HOLDING].str[:-2]
    df[KEYS_TICKER.PROMOTER_HOLDING] = df[KEYS_TICKER.PROMOTER_HOLDING].astype('float')

    df[KEYS_TICKER.CARD4] = df[KEYS_TICKER.CARD4].map('{:.2f}'.format).astype('float')
    df[KEYS_TICKER.CARD5] = df[KEYS_TICKER.CARD5].map('{:.2f}'.format).astype('float')
    df[KEYS_TICKER.CARD6] = df[KEYS_TICKER.CARD6].map('{:.2f}'.format).astype('float')
    df[KEYS_TICKER.CARD7] = df[KEYS_TICKER.CARD7].map('{:.2f}'.format).astype('float')

    print(df[KEYS_TICKER.CARD4], df.dtypes[KEYS_TICKER.CARD4])

    print(df.info())

    print(df.columns)

    df[KEYS_TICKER.PERCENT_FROM_52_HIGH] = (df[KEYS_TICKER.WEEK_52_HIGH] - df[KEYS_TICKER.PRICE]) / df[KEYS_TICKER.WEEK_52_HIGH] * 100
    df[KEYS_TICKER.PERCENT_FROM_52_LOW] = (df[KEYS_TICKER.PRICE] - df[KEYS_TICKER.WEEK_52_LOW]) / df[KEYS_TICKER.WEEK_52_LOW] * 100

    # t_dictionary[KEYS_TICKER.PERCENT_FROM_52_HIGH] = float("{:.2f}".format((float(t_dictionary[KEYS_TICKER.WEEK_52_HIGH]) - float(t_dictionary[KEYS_TICKER.PRICE])) / float(t_dictionary[KEYS_TICKER.WEEK_52_HIGH]) * 100))

    df.to_csv('data/transformed.csv', sep=',')


transform()

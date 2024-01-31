import json

import pandas as pd
from tabulate import tabulate

from constants import KEYS_SCREENER


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

    df.reset_index()
    print(df.info())

    df[KEYS_SCREENER.PRICE] = df[KEYS_SCREENER.PRICE].str[2:]
    df[KEYS_SCREENER.PRICE] = df[KEYS_SCREENER.PRICE].str.replace(',', '')
    df[KEYS_SCREENER.PRICE] = df[KEYS_SCREENER.PRICE].astype('float')

    df[KEYS_SCREENER.INTRINSIC_VALUE] = df[KEYS_SCREENER.INTRINSIC_VALUE].str[2:]
    df[KEYS_SCREENER.INTRINSIC_VALUE] = df[KEYS_SCREENER.INTRINSIC_VALUE].str.replace(',', '')
    df[KEYS_SCREENER.INTRINSIC_VALUE] = df[KEYS_SCREENER.INTRINSIC_VALUE].astype('float')

    df[KEYS_SCREENER.GRAHAM_NUMBER] = df[KEYS_SCREENER.GRAHAM_NUMBER].str[2:]
    df[KEYS_SCREENER.GRAHAM_NUMBER] = df[KEYS_SCREENER.GRAHAM_NUMBER].str.replace(',', '')
    df[KEYS_SCREENER.GRAHAM_NUMBER] = df[KEYS_SCREENER.GRAHAM_NUMBER].astype('float')

    print_dataframe(df)

    print(df.info())


transform()

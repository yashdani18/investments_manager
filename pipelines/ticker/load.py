import pandas as pd
import mysql.connector

from pipelines.ticker.constants import KEYS_TICKER

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="mark_1"
)
cursor = mydb.cursor()

cursor.execute('SELECT * FROM daily')

result = cursor.fetchall()

for x in result:
    print(x)

df_transformed = pd.read_csv('data/transformed.csv')
# print(df_transformed['Ticker'])

for index, row in df_transformed.iterrows():
    print(row[KEYS_TICKER.TICKER])
    sql = f'REPLACE INTO tbl_ticker_main(' \
          f'{KEYS_TICKER.TICKER}, {KEYS_TICKER.NAME}, ' \
          f'{KEYS_TICKER.PRICE}, {KEYS_TICKER.WEEK_52_HIGH}, {KEYS_TICKER.WEEK_52_LOW}, ' \
          f'{KEYS_TICKER.SECTOR}, ' \
          f'{KEYS_TICKER.RATING_FINSTAR}, ' \
          f'{KEYS_TICKER.RATING_OWNERSHIP_STATUS}, {KEYS_TICKER.RATING_OWNERSHIP_VALUE}, ' \
          f'{KEYS_TICKER.RATING_VALUATION_STATUS}, {KEYS_TICKER.RATING_VALUATION_VALUE}, ' \
          f'{KEYS_TICKER.RATING_EFFICIENCY_STATUS}, {KEYS_TICKER.RATING_EFFICIENCY_VALUE}, ' \
          f'{KEYS_TICKER.RATING_FINANCIALS_STATUS}, {KEYS_TICKER.RATING_FINANCIALS_VALUE}, ' \
          f'{KEYS_TICKER.MARKET_CAP}, {KEYS_TICKER.ENTERPRISE_VALUE}, {KEYS_TICKER.NUMBER_OF_SHARES}, ' \
          f'{KEYS_TICKER.PE}, {KEYS_TICKER.PB}, {KEYS_TICKER.FV}, {KEYS_TICKER.DIV_YIELD}, {KEYS_TICKER.BOOK_VALUE}, ' \
          f'{KEYS_TICKER.CASH_NII}, {KEYS_TICKER.DEBT_CTI}, ' \
          f'{KEYS_TICKER.PROMOTER_HOLDING}, {KEYS_TICKER.EPS}, {KEYS_TICKER.SALES_CAR}, ' \
          f'{KEYS_TICKER.ROE}, {KEYS_TICKER.ROCE}, {KEYS_TICKER.PROFIT}, ' \
          f'{KEYS_TICKER.CARD4}, {KEYS_TICKER.CARD5}, {KEYS_TICKER.CARD6}, {KEYS_TICKER.CARD7}) ' \
          f'VALUES (' \
          f'%s, %s, ' \
          f'%s, %s, %s, ' \
          f'%s, ' \
          f'%s, ' \
          f'%s, %s, ' \
          f'%s, %s, ' \
          f'%s, %s, ' \
          f'%s, %s, ' \
          f'%s, %s, %s, ' \
          f'%s, %s, %s, %s, %s, ' \
          f'%s, %s, ' \
          f'%s, %s, %s, ' \
          f'%s, %s, %s, ' \
          f'%s, %s, %s, %s)'
    val = (row[KEYS_TICKER.TICKER], row[KEYS_TICKER.NAME],
           row[KEYS_TICKER.PRICE], row[KEYS_TICKER.WEEK_52_HIGH], row[KEYS_TICKER.WEEK_52_LOW],
           row[KEYS_TICKER.SECTOR], row[KEYS_TICKER.RATING_FINSTAR],
           row[KEYS_TICKER.RATING_OWNERSHIP_STATUS], row[KEYS_TICKER.RATING_OWNERSHIP_VALUE],
           row[KEYS_TICKER.RATING_VALUATION_STATUS], row[KEYS_TICKER.RATING_VALUATION_VALUE],
           row[KEYS_TICKER.RATING_EFFICIENCY_STATUS], row[KEYS_TICKER.RATING_EFFICIENCY_VALUE],
           row[KEYS_TICKER.RATING_FINANCIALS_STATUS], row[KEYS_TICKER.RATING_FINANCIALS_VALUE],
           row[KEYS_TICKER.MARKET_CAP], row[KEYS_TICKER.ENTERPRISE_VALUE], row[KEYS_TICKER.NUMBER_OF_SHARES],
           row[KEYS_TICKER.PE], row[KEYS_TICKER.PB], row[KEYS_TICKER.FV], row[KEYS_TICKER.DIV_YIELD],
           row[KEYS_TICKER.BOOK_VALUE],
           row[KEYS_TICKER.CASH_NII], row[KEYS_TICKER.DEBT_CTI],
           row[KEYS_TICKER.PROMOTER_HOLDING], row[KEYS_TICKER.EPS], row[KEYS_TICKER.SALES_CAR],
           row[KEYS_TICKER.ROE], row[KEYS_TICKER.ROCE], row[KEYS_TICKER.PROFIT],
           row[KEYS_TICKER.CARD4], row[KEYS_TICKER.CARD5], row[KEYS_TICKER.CARD6], row[KEYS_TICKER.CARD7])
    cursor.execute(sql, val)

mydb.commit()

print('rows inserted')

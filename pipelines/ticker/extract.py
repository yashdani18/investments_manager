from constants import KEYS_TICKER

import requests
from bs4 import BeautifulSoup
import pandas as pd

import time
import json


def prepare_url(p_ticker):
    return 'https://ticker.finology.in/company/' + p_ticker


def get_html_from_ticker(p_url):
    t_response = requests.get(p_url).text
    # print(t_response)
    return t_response


def parse_html_from_ticker(p_ticker, p_response):
    t_dictionary = {}
    soup = BeautifulSoup(p_response, 'lxml')

    std_con_element = soup.find(id="mainContent_switchCon")
    if std_con_element is not None:
        # print('Consolidated Available')
        # print('')
        if p_ticker[0] == '/':
            url = 'https://ticker.finology.in' + p_ticker + '?mode=C'
            ticker_manipulation = p_ticker.split('/')[2]
            print(ticker_manipulation)
            p_ticker = ticker_manipulation
        else:
            url = 'https://ticker.finology.in/company/' + p_ticker + '?mode=C'
        temp_response = requests.get(url).text
        soup = BeautifulSoup(temp_response, 'lxml')
    else:
        print('', end="")
        # print('Consolidated not available')

    if p_ticker[0] == '/':
        p_ticker = p_ticker.split('/')[2]

    try:
        t_dictionary[KEYS_TICKER.NAME] = soup.find(id="mainContent_ltrlCompName").text
        t_dictionary[KEYS_TICKER.PRICE] = soup.find(class_="currprice").text
        t_dictionary[KEYS_TICKER.WEEK_52_HIGH] = soup.find(id="mainContent_ltrl52WH").text
        t_dictionary[KEYS_TICKER.WEEK_52_LOW] = soup.find(id="mainContent_ltrl52WL").text

        t_dictionary[KEYS_TICKER.SECTOR] = soup.find(id="mainContent_compinfoId").find('a').text
        t_dictionary[KEYS_TICKER.TICKER] = p_ticker

        t_dictionary[KEYS_TICKER.RATING_FINSTAR] = soup.find(id="mainContent_ltrlOverAllRating")['aria-label']

        t_dictionary[KEYS_TICKER.RATING_OWNERSHIP_STATUS] = soup.find(id="mainContent_divOwner").find('span').text
        t_dictionary[KEYS_TICKER.RATING_OWNERSHIP_VALUE] = soup.find_all('div', id="mainContent_ManagementRating")[0]['aria-label']

        t_dictionary[KEYS_TICKER.RATING_VALUATION_STATUS] = soup.find(id="mainContent_divValuation").find('span').text
        t_dictionary[KEYS_TICKER.RATING_VALUATION_VALUE] = soup.find_all('div', id="mainContent_ValuationRating")[0]['aria-label']

        t_dictionary[KEYS_TICKER.RATING_EFFICIENCY_STATUS] = soup.find(id="mainContent_divEff").find('span').text
        t_dictionary[KEYS_TICKER.RATING_EFFICIENCY_VALUE] = soup.find_all('div', id="mainContent_EfficiencyRating")[0]['aria-label']

        t_dictionary[KEYS_TICKER.RATING_FINANCIALS_STATUS] = soup.find(id="mainContent_divFinance").find('span').text
        t_dictionary[KEYS_TICKER.RATING_FINANCIALS_VALUE] = soup.find_all('div', id="mainContent_FinancialsRating")[0]['aria-label']

        t_dictionary[KEYS_TICKER.MARKET_CAP] = soup.select('div.col-6.col-md-4.compess')[0].select('span.Number')[0].text
        t_dictionary[KEYS_TICKER.ENTERPRISE_VALUE] = soup.select('div.col-6.col-md-4.compess')[1].select('span.Number')[0].text

        t_dictionary[KEYS_TICKER.NUMBER_OF_SHARES] = soup.select('div.col-6.col-md-4.compess')[2].select('span.Number')[0].text

        t_dictionary[KEYS_TICKER.PE] = soup.select('div.col-6.col-md-4.compess')[3].select('p')[0].text
        t_dictionary[KEYS_TICKER.PB] = soup.select('div.col-6.col-md-4.compess')[4].select('p')[0].text
        t_dictionary[KEYS_TICKER.FV] = soup.select('div.col-6.col-md-4.compess')[5].select('p')[0].text

        t_dictionary[KEYS_TICKER.DIV_YIELD] = soup.select('div.col-6.col-md-4.compess')[6].select('p')[0].text
        t_dictionary[KEYS_TICKER.BOOK_VALUE] = soup.select('div.col-6.col-md-4.compess')[7].select('span.Number')[0].text

        try:
            t_dictionary[KEYS_TICKER.CASH_NII] = soup.select('div.col-6.col-md-4.compess')[8].select('span.Number')[0].text
        except:
            t_dictionary[KEYS_TICKER.CASH_NII] = 0

        try:
            t_dictionary[KEYS_TICKER.DEBT_CTI] = soup.select('div.col-6.col-md-4.compess')[9].select('span.Number')[0].text
        except:
            t_dictionary[KEYS_TICKER.DEBT_CTI] = 0

        t_dictionary[KEYS_TICKER.PROMOTER_HOLDING] = soup.select('div.col-6.col-md-4.compess')[10].select('p')[0].text

        t_dictionary[KEYS_TICKER.EPS] = soup.select('div.col-6.col-md-4.compess')[11].select('span.Number')[0].text

        try:
            t_dictionary[KEYS_TICKER.SALES_CAR] = soup.select('div.col-6.col-md-4.compess')[12].select('p')[0].text.strip() if (t_dictionary[KEYS_TICKER.SECTOR].strip())[:4] == 'Bank' else soup.select('div.col-6.col-md-4.compess')[12].select('span.Number')[0].text.strip()
        except:
            t_dictionary[KEYS_TICKER.SALES_CAR] = 0

        try:
            t_dictionary[KEYS_TICKER.ROE] = soup.select('div.col-6.col-md-4.compess')[13].select('span.Number')[0].text
        except:
            t_dictionary[KEYS_TICKER.ROE] = 0

        try:
            t_dictionary[KEYS_TICKER.ROCE] = soup.select('div.col-6.col-md-4.compess')[14].select('span.Number')[0].text
        except:
            t_dictionary[KEYS_TICKER.ROCE] = 0

        try:
            t_dictionary[KEYS_TICKER.PROFIT] = soup.select('div.col-6.col-md-4.compess')[15].select('span.Number')[0].text
        except:
            t_dictionary[KEYS_TICKER.PROFIT] = 0

        t_dictionary[KEYS_TICKER.CARD4] = soup.find_all(class_='card cardscreen cardsmall')[4].find_all('span')[-1].text
        t_dictionary[KEYS_TICKER.CARD5] = soup.find_all(class_='card cardscreen cardsmall')[5].find_all('span')[-1].text
        t_dictionary[KEYS_TICKER.CARD6] = soup.find_all(class_='card cardscreen cardsmall')[6].find_all('span')[-1].text
        t_dictionary[KEYS_TICKER.CARD7] = soup.find_all(class_='card cardscreen cardsmall')[7].find_all('span')[-1].text

        # t_dictionary[KEYS_TICKER.DIFF_52] = float(t_dictionary[KEYS_TICKER.WEEK_52_HIGH]) - float(t_dictionary[KEYS_TICKER.PRICE])
        # t_dictionary[KEYS_TICKER.PERCENT_FROM_52_HIGH] = float("{:.2f}".format((float(t_dictionary[KEYS_TICKER.WEEK_52_HIGH]) - float(t_dictionary[KEYS_TICKER.PRICE])) / float(t_dictionary[KEYS_TICKER.WEEK_52_HIGH]) * 100))
        # t_dictionary[KEYS_TICKER.PERCENT_FROM_52_LOW] = float("{:.2f}".format((float(t_dictionary[KEYS_TICKER.PRICE]) - float(t_dictionary[KEYS_TICKER.WEEK_52_LOW])) / float(t_dictionary[KEYS_TICKER.PRICE]) * 100))

    except:
        return None
    return t_dictionary


def get_data_from_ticker_using_bs4(p_ticker):
    if p_ticker[0] == '/':
        url = 'https://ticker.finology.in' + p_ticker
    else:
        url = prepare_url(p_ticker)
    response = get_html_from_ticker(url)
    dictionary = parse_html_from_ticker(p_ticker, response)
    # print(dictionary)
    return dictionary


def update_progress_bar(index, length):
    print('Progress: |', end="")
    for i in range(length):
        if i <= index:
            print('#', end="")
        else:
            print('-', end="")
    print(f'| ({str(index + 1).zfill(2)}/{length})')


def extract():
    f = open('../../data/companies_portfolio.json')
    companies = json.load(f)['companies']
    length = len(companies)
    update_progress_bar(-1, length)
    out_companies = []
    for index, company in enumerate(companies):
        dictionary = get_data_from_ticker_using_bs4(company['symbol'])
        out_companies.append(dictionary)
        update_progress_bar(index, length)
        time.sleep(1)

    # Convert array of dictionaries to pandas dataframe
    df = pd.DataFrame(out_companies)

    # Write the dataframe to csv
    df.to_csv('data/raw.csv', sep=',')

    # Write the array of dictionaries as a json file
    with open("data/raw.json", 'w') as outfile:
        json.dump(out_companies, outfile)


extract()

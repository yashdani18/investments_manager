import json
import os
from datetime import datetime

import pandas as pd
from selenium import webdriver
import time

from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait

from common.progress_bar import update_progress_bar
from pipelines.screener.selenium.constants import KEYS_SCREENER

from pipelines.screener.selenium.config.keys import USERNAME, PASSWORD


def get_data_from_screener_using_selenium(p_companies, arr_dict):
    driver = webdriver.Chrome()
    driver.get('https://www.screener.in/login/')

    field_username = driver.find_element(By.NAME, 'username')
    ActionChains(driver).send_keys_to_element(field_username, USERNAME).perform()

    field_password = driver.find_element(By.NAME, 'password')
    ActionChains(driver).send_keys_to_element(field_password, PASSWORD).perform()

    field_password.send_keys(Keys.RETURN)

    try:
        for index, company in enumerate(p_companies):
            ticker = company['symbol']
            search_dashboard = WebDriverWait(driver, 10).until(
                expected_conditions.presence_of_element_located((By.ID, "desktop-search"))
            )
            field_company = search_dashboard.find_element(By.CLASS_NAME, "u-full-width")
            field_company.clear()
            field_company.send_keys(ticker)
            time.sleep(1)
            field_company.send_keys(Keys.RETURN)

            try:
                # Wait for the data (top ratios) to load
                results = WebDriverWait(driver, 10).until(
                    expected_conditions.presence_of_element_located((By.ID, "top-ratios"))
                )
                time.sleep(2)

                # Find all fundamental ratios
                elements = results.find_elements(By.CSS_SELECTOR, "li.flex.flex-space-between")

                index_desired_elements = [1, 12, 13]
                data = []
                for i in index_desired_elements:
                    values = elements[i].text.split('\n')
                    if len(values) > 1:
                        data.append(values[1])
                    else:
                        data.append(0)

                time.sleep(1)

                val_price = data[0]
                val_intrinsic_value = data[1]
                val_graham_number = data[2]

                # if len(val_price) > 1 and len(val_intrinsic_value) > 1 and len(val_graham_number) > 1:
                #     val_price = float(val_price[2:].replace(',', ''))
                #     val_intrinsic_value = val_intrinsic_value[2:]
                #     val_intrinsic_value = float(val_intrinsic_value.replace(',', ''))
                #     val_graham_number = float(val_graham_number[2:].replace(',', ''))

                dictionary = {
                    KEYS_SCREENER.TICKER: ticker,
                    KEYS_SCREENER.PRICE: val_price,
                    KEYS_SCREENER.INTRINSIC_VALUE: val_intrinsic_value,
                    KEYS_SCREENER.GRAHAM_NUMBER: val_graham_number
                }

                arr_dict.append(dictionary)

                update_progress_bar(index, len(p_companies))
                time.sleep(1)

                # return dictionary
            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                print(message)
                print('exception')
                driver.quit()
    except:
        pass
        # driver.quit()


def extract():
    f = open('../../../data/companies_portfolio.json')
    companies = json.load(f)['companies']
    length = len(companies)
    update_progress_bar(-1, length)
    arr_dict = []
    get_data_from_screener_using_selenium(companies, arr_dict)

    # Convert array of dictionaries to pandas dataframe
    df = pd.DataFrame(arr_dict)

    # Write the dataframe to csv
    df.to_csv('data/raw.csv', sep=',')

    # Write the array of dictionaries as a json file
    with open("data/raw.json", 'w') as outfile:
        json.dump(arr_dict, outfile)


extract()

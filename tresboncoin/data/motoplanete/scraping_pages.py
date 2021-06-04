from parameters import *
from utils import *
from datetime import datetime
from scraper import *
import os
import requests
from bs4 import BeautifulSoup
import time
import random
import pandas as pd


def scraping_pages():

    # log file name
    csv_name = "../log_motoplanete.csv"

    # init scrap
    page_number = 1
    scrap = True

    # site to scrap
    source = 'motoplanete'

    # log update
    log_new = pd.DataFrame({'source': [source],
                            'step': ['scrap pages'],
                            'status': ['started'],
                            'time': [datetime.now()],
                            'details': [""]})

    # init log
    if os.path.isfile(csv_name) is False:
        log_new.to_csv(csv_name, index=False)
    else:
        log_import = pd.read_csv(csv_name)
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv(csv_name, index=False)


    try:
        # Start time of the pages scrapping
        start_time = datetime.now()

        while scrap == True:
            html_file = get_source_selenium(motoplanete_page_url + str(page_number))
            last_saved_file = save_page_list(source, html_file, page=page_number)
            time.sleep(random.randint(1, 3))
            page_number += 1

            with open(last_saved_file, 'r') as f:
                readable_html = f.read()

                soup_ = BeautifulSoup(readable_html, 'html.parser')
                page_scrap = motoplanete_page_scraper(page=soup_)
                prices = page_scrap.get_prices_from_pages()

            if len(prices) == 0:
                scrap=False

        # End time
        end_time = datetime.now()
        td = end_time - start_time

        # log update
        log_import = pd.read_csv(csv_name)
        log_new = pd.DataFrame({'source': [source],
                                'step': ['scrap pages'],
                                'status': ['completed'],
                                'time': [datetime.now()],
                                'details': [f"{td.seconds/60} minutes elapsed, {page_number} pages scrapped"]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv(csv_name, index=False)

    except (ValueError, TypeError, NameError, KeyError, RuntimeWarning) as err:
        # log update
        log_import = pd.read_csv(csv_name)
        log_new = pd.DataFrame({'source': [source],
                                'step': ['scrap pages'],
                                'status': ['error'],
                                'time': [datetime.now()],
                                'details': [err]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv(csv_name, index=False)


if __name__ == "__main__":
    scraping_pages()

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
    csv_name = "../log_fulloccaz.csv"

    # init scrap
    scrap = True
    page_ = 1
    page_total = 1

    # site to scrap
    source = 'fulloccaz'

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

        for motobrand in full_occaz_brand_list:
            scrap = True
            page_ = 1
            while scrap:
                html_file = requests.get(fulloccaz_page_url + f"Marque={motobrand}&page={page_}", headers=headers)
                last_saved_file = save_page_list(source, html_file, page=page_total)
                time.sleep(random.randint(1, 3))
                page_ += 1
                page_total += 1

                with open(last_saved_file, 'r') as f:
                    readable_html = f.read()

                    soup_ = BeautifulSoup(readable_html, 'html.parser')
                    page_scrap = fulloccaz_page_scraper(page=soup_)
                    prices = page_scrap.get_prices_from_pages()

                if len(prices) == 0:
                    scrap = False

        # End time
        end_time = datetime.now()
        td = end_time - start_time

        # log update
        log_import = pd.read_csv(csv_name)
        log_new = pd.DataFrame({'source': [source],
                                'step': ['scrap pages'],
                                'status': ['completed'],
                                'time': [datetime.now()],
                                'details': [f"{td.seconds/60} minutes elapsed, {page_total} pages scrapped"]})
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

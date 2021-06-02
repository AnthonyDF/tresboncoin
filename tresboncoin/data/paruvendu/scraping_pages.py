from parameters import *
from utils import *
from datetime import datetime
import os
import requests
from bs4 import BeautifulSoup
import time
import random
import pandas as pd


def scraping_pages():

    # log file name
    csv_name = "../log_pv.csv"

    # init scrap
    page_number = 1
    scrap = True

    # site to scrap
    source = 'paruvendu'

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

            req_ = requests.get(paruvendu_page_url + str(page_number), headers = headers)
            save_page_list(source, req_, page=page_number)
            time.sleep(random.randint(4, 5))
            page_number += 1

            # condition that will stop while loop
            end_scrap = BeautifulSoup(req_.text, 'html.parser')
            if len(end_scrap.select('div[class*="lazyload_bloc"]')) == 0:
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

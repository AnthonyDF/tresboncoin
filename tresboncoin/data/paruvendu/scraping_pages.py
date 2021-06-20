from parameters import *
from utils import *
from datetime import datetime
import os
import requests
from bs4 import BeautifulSoup
import time
import random
import pandas as pd

PATH_TO_CSV = os.path.dirname(os.path.abspath(__file__)).replace('/paruvendu', '') + '/scraping_outputs/paruvendu.csv'
PATH_TO_LOG = os.path.dirname(os.path.abspath(__file__)).replace('/paruvendu', '') + "/log.csv"
PATH_TO_FOLDER = os.path.dirname(os.path.abspath(__file__))
PATH_TO_IMG_FOLDER = os.path.dirname(os.path.abspath(__file__)) + '/img'
PATH_TO_PAGES_FOLDER = os.path.dirname(os.path.abspath(__file__)) + ('/pages')
PATH_TO_ANNONCES_FOLDER = os.path.dirname(os.path.abspath(__file__)) + ('/annonces')
PATH_TO_INDEX = os.path.dirname(os.path.abspath(__file__)) + '/index.csv'


def scraping_pages():

    # init scrap
    page_number = 1
    scrap = True

    # site to scrap
    source = 'paruvendu'

    # Start time
    start_time = datetime.now()

    # log update
    log_import = pd.read_csv(PATH_TO_LOG)
    log_new = pd.DataFrame({'source': [source],
                            'step': ['scrap annonces'],
                            'status': ['started'],
                            'time': [datetime.now()],
                            'details': [""]})
    log = log_import.append(log_new, ignore_index=True)
    log.to_csv(PATH_TO_LOG, index=False)

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
        log_import = pd.read_csv(PATH_TO_LOG)
        log_new = pd.DataFrame({'source': [source],
                                'step': ['scrap pages'],
                                'status': ['completed'],
                                'time': [datetime.now()],
                                'details': [f"{td.seconds/60} minutes elapsed"]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv(PATH_TO_LOG, index=False)

    except (ValueError, TypeError, NameError, KeyError, RuntimeWarning) as err:
        # log update
        log_import = pd.read_csv(PATH_TO_LOG)
        log_new = pd.DataFrame({'source': [source],
                               'status': ['error'],
                                'time': [datetime.now()],
                                'details': [err]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv(PATH_TO_LOG, index=False)


if __name__ == "__main__":
    scraping_pages()

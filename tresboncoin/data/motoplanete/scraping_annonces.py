from parameters import *
from scraper import *
from utils import *
import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import time
import pandas as pd

PATH_TO_CSV = os.path.dirname(os.path.abspath(__file__)).replace('/motoplante', '') + '/scraping_outputs/motoplante.csv'
PATH_TO_LOG = os.path.dirname(os.path.abspath(__file__)).replace('/motoplante', '') + "/log.csv"
PATH_TO_FOLDER = os.path.dirname(os.path.abspath(__file__))
PATH_TO_IMG_FOLDER = os.path.dirname(os.path.abspath(__file__)) + '/img'
PATH_TO_PAGES_FOLDER = os.path.dirname(os.path.abspath(__file__)) + ('/pages')
PATH_TO_ANNONCES_FOLDER = os.path.dirname(os.path.abspath(__file__)) + ('/annonces')
PATH_TO_INDEX = os.path.dirname(os.path.abspath(__file__)) + '/index.csv'


def scraping_annonces():

    # init scrap
    directory = PATH_TO_PAGES_FOLDER
    #scrap = True

    # site to scrap
    source = 'motoplanete'

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

    # load csv if exists or starting from template
    data_motoplanete = "../motoplanete.csv"
    #
    if os.path.isfile(data_motoplanete) is False:
        df = motoplanete_announce_template.copy()
    else:
        data = pd.read_csv(data_motoplanete)
        df = motoplanete_announce_template.copy()

    try:

        # counter
        count = 0

        # Start time
        start_time = datetime.datetime.now()

        # iterate over html files in pages directory
        for html_file in [file for file in os.listdir(directory) if file.endswith(".html")]:

            # define soup
            #############

            # opening file
            with open(os.path.join(directory, html_file), 'r') as f:
                readable_html = f.read()

            # get soup
            soup_ = BeautifulSoup(readable_html, 'html.parser')

            scrap = motoplanete_page_scraper(page=soup_)

            if scrap.get_urls_from_pages() != None:
                url_list_ = scrap.get_urls_from_pages()
                for url_single, uniq_id, price_ in zip(url_list_, scrap.get_references_from_url_list(url_list_), scrap.get_prices_from_pages()):
                    if add_the_announce(data, uniq_id, price_):
                        req_uniq = requests.get(url_single, headers = headers)
                        save_page_uniq(source, req_uniq, uniq_id)
                        time.sleep(1)
                        count += 1

            # delete html file
            os.remove(directory+"/"+html_file)

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
    scraping_annonces()

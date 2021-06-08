from parameters import *
from scraper import *
from utils import *
import requests
from bs4 import BeautifulSoup
import os
import codecs
from datetime import datetime
import time
import random
import pandas as pd


def scraping_annonces():

    # log file name
    csv_name = "../log_fulloccaz.csv"

    # init scrap
    directory = 'pages'
    #scrap = True

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


    # load csv if exists or starting from template
    data_fulloccaz = "../fulloccaz.csv"
    #
    if os.path.isfile(data_fulloccaz) is False:
        df = fulloccaz_announce_template.copy()
    else:
        data = pd.read_csv(data_fulloccaz)
        df = fulloccaz_announce_template.copy()

    try:

        # counter
        count = 0

        # Start time
        start_time = datetime.now()

        # iterate over html files in pages directory
        for html_file in [file for file in os.listdir(directory) if file.endswith(".html")]:

            # define soup
            #############

            # opening file
            with open(os.path.join(directory, html_file), 'r') as f:
                readable_html = f.read()

            # get soup
            soup_ = BeautifulSoup(readable_html, 'html.parser')

            scrap = fulloccaz_page_scraper(page=soup_)

            if scrap.get_urls_from_pages() != None:
                url_list_ = scrap.get_urls_from_pages()
                for url_single, uniq_id, price_ in zip(url_list_, scrap.get_references_from_url_list(url_list_), scrap.get_prices_from_pages()):
                    if add_the_announce(data, uniq_id, price_):
                        req_uniq = requests.get(url_single, headers=headers)
                        save_page_uniq(source, req_uniq, uniq_id)
                        time.sleep(1)
                        count += 1

            # delete html file
            os.remove(directory+"/"+html_file)

        # End time
        end_time = datetime.now()
        td = end_time - start_time

        # log update
        log_import = pd.read_csv(csv_name)
        log_new = pd.DataFrame({'source': [source],
                                'step': ['scrap annonces'],
                                'status': ['completed'],
                                'time': [datetime.now()],
                                'details': [f"{td.seconds/60} minutes elapsed, {count} annonces scrapped"]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv(csv_name, index=False)

    except (ValueError, TypeError, NameError, KeyError, RuntimeWarning) as err:
        # log update
        log_import = pd.read_csv(csv_name)
        log_new = pd.DataFrame({'source': [source],
                                'step': ['scrap annonces'],
                                'status': ['error'],
                                'time': [datetime.now()],
                                'details': [err]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv(csv_name, index=False)
    return


if __name__ == "__main__":
    scraping_annonces()

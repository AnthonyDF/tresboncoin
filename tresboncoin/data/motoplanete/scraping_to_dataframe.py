from parameters import *
from scraper import *
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import subprocess


PATH_TO_CSV = os.path.dirname(os.path.abspath(__file__)).replace('/motoplante', '') + '/scraping_outputs/motoplante.csv'
PATH_TO_LOG = os.path.dirname(os.path.abspath(__file__)).replace('/motoplante', '') + "/log.csv"
PATH_TO_FOLDER = os.path.dirname(os.path.abspath(__file__))
PATH_TO_IMG_FOLDER = os.path.dirname(os.path.abspath(__file__)) + '/img'
PATH_TO_PAGES_FOLDER = os.path.dirname(os.path.abspath(__file__)) + ('/pages')
PATH_TO_ANNONCES_FOLDER = os.path.dirname(os.path.abspath(__file__)) + ('/annonces')
PATH_TO_INDEX = os.path.dirname(os.path.abspath(__file__)) + '/index.csv'


def scraping_to_dataframe():

    # directory of html annonces
    directory = PATH_TO_ANNONCES_FOLDER

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
    data_motoplanete = PATH_TO_CSV
    #
    if os.path.isfile(data_motoplanete) is False:
        df = motoplanete_announce_template.copy()
    else:
        data = pd.read_csv(data_motoplanete)
        df = motoplanete_announce_template.copy()

    try:
        # Start time of the pages scrapping
        start_time = datetime.now()

        for html_file in [file for file in os.listdir(directory) if file.endswith(".html")]:

            # define soup
            #############

            # opening file
            with open(os.path.join(directory, html_file), 'r') as f:
                readable_html = f.read()

            # get soup
            soup_ = BeautifulSoup(readable_html, 'html.parser')

            # fill df with defined functions
            announce_scrap = motoplanete_page_scraper(announce=soup_)

            df["url"] = announce_scrap.get_url()
            df["unique id"] = announce_scrap.get_uniq_id(announce_scrap.get_url())
            df["date_scrapped"] = datetime.now().strftime("%Y/%m/%d - %Hh%M")
            df["announce_publication_date"] = announce_scrap.get_publication_date()
            df["vehicle brand"] = announce_scrap.get_brand_and_model()
            df["vehicle type"] = announce_scrap.get_type()
            df["color"] = announce_scrap.get_color()
            df["hand"] = announce_scrap.get_hand()
            df["price"] = announce_scrap.get_price()
            df["city"] = announce_scrap.get_city()
            df["postal code"] = announce_scrap.get_postal()
            df["seller"] = announce_scrap.get_seller()
            df["seller_name"] = announce_scrap.get_seller_name()
            df["vehicle release date"] = announce_scrap.get_release()
            df["mileage"] = announce_scrap.get_mileage()
            df["engine capacity [CC]"] = announce_scrap.get_capa()
            df["comments"] = announce_scrap.get_comment()

            # save images
            announce_scrap.get_images(announce_scrap.get_uniq_id(announce_scrap.get_url()))

            # concatenate to csv and write
            try:
                data = pd.concat([data, df], axis=0)
                data.to_csv(path_or_buf=data_motoplanete, index=False)
            except:
                df.to_csv(path_or_buf=data_motoplanete, index=False)

            # deplacer le fichier html traité dans le vault
            subprocess.run(["mv", os.path.join(directory, html_file), "vault"])

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
    scraping_to_dataframe()

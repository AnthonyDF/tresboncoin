import requests
from parameters import *
from scraper import *
from bs4 import BeautifulSoup
import requests
from PIL import Image
import os
import glob
import pandas as pd
import time
import random
import codecs
import shutil
from datetime import datetime
import subprocess


def scraping_to_dataframe():

    # log file name
    csv_name = "../log_fulloccaz.csv"

    # directory of html annonces
    directory = 'annonces'

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
            soup_clear = BeautifulSoup(readable_html, features="lxml")



            # fill df with defined functions
            announce_scrap = fulloccaz_page_scraper(announce=soup_)
            announce_scrap_clear = fulloccaz_page_scraper(announce=soup_clear)

            url_ = announce_scrap.get_url()
            uniq_id_ = announce_scrap.get_uniq_id(url_)
            price_ = announce_scrap.get_price()

            if isinstance(url_, str):
                if add_the_announce(data, uniq_id_, price_):
                    df["url"] = url_
                    df["unique id"] = uniq_id_
                    df["date_scrapped"] = datetime.now().strftime("%Y/%m/%d - %Hh%M")
                    df["announce_publication_date"] = announce_scrap.get_publication_date()
                    df["vehicle brand"] = announce_scrap.get_brand_and_model()
                    df["vehicle type"] = announce_scrap.get_type()
                    df["color"] = announce_scrap.get_color()
                    df["price"] = price_
                    df["city"] = good_soup_selector(announce_scrap.get_city(), announce_scrap_clear.get_city())
                    df["postal code"] = good_soup_selector(announce_scrap.get_postal(), announce_scrap_clear.get_postal())
                    df["seller"] = good_soup_selector(announce_scrap.get_seller(), announce_scrap_clear.get_seller())
                    df["seller_name"] = good_soup_selector(announce_scrap.get_seller_name(), announce_scrap_clear.get_seller_name())
                    df["vehicle release date"] = announce_scrap.get_release()
                    df["mileage"] = announce_scrap.get_mileage()
                    df["motorisation"] = announce_scrap.get_engine_type()
                    df["fiscal power"] = announce_scrap.get_fiscal_power()
                    df["guarantee"] = announce_scrap.get_guarantee()
                    df["hand"] = announce_scrap.get_hand()
                    df["chassis"] = announce_scrap.get_chassis()
                    df["engine capacity [CC]"] = announce_scrap.get_capa()
                    df["comments"] = announce_scrap.get_comment()
#
                    # save images
                    announce_scrap.get_images(uniq_id_)
#
                    # concatenate to csv and write
                    try:
                        data = pd.concat([data, df], axis=0)
                        data.to_csv(path_or_buf = data_fulloccaz, index=False)
                    except:
                        df.to_csv(path_or_buf = data_fulloccaz, index=False)

                    # deplacer le fichier html traité dans le vault
                    subprocess.run(["mv", os.path.join(directory, html_file), "vault"])
                else:
                    subprocess.run(["mv", os.path.join(directory, html_file), "vault"])
            else:
                subprocess.run(["mv", os.path.join(directory, html_file),
                                os.path.join("vault", html_file.replace(".html", "_BAD.html"))])


        # End time
        end_time = datetime.now()
        td = end_time - start_time

        # log update
        log_import = pd.read_csv(csv_name)
        log_new = pd.DataFrame({'source': [source],
                                'step': ['to dataframe'],
                                'status': ['completed'],
                                'time': [datetime.now()],
                                'details': [f"{td.seconds/60} minutes elapsed"]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv(csv_name, index=False)


    except (ValueError, TypeError, NameError, KeyError, RuntimeWarning) as err:
        # log update
        log_import = pd.read_csv(csv_name)
        log_new = pd.DataFrame({'source': [source],
                                'step': ['to dataframe'],
                                'status': ['error'],
                                'time': [datetime.now()],
                                'details': [err]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv(csv_name, index=False)


if __name__ == "__main__":
    scraping_to_dataframe()

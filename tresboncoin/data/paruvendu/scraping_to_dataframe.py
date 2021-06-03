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
    csv_name = "../log_pv.csv"


    # directory of html annonces
    directory = 'annonces'


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


    # load csv if exists or starting from template
    data_paruvendu = "../paruvendu.csv"
    #
    if os.path.isfile(data_paruvendu) is False:
        df = paruvendu_announce_template.copy()
    else:
        data = pd.read_csv(data_paruvendu)
        df = paruvendu_announce_template.copy()


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
            announce_scrap = paruvendu_page_scraper(announce=soup_)

            df["url"] = announce_scrap.get_url()
            df["reference"] = announce_scrap.get_reference()
            df["unique id"] = announce_scrap.get_uniq_id(announce_scrap.get_url())
            df["date_scrapped"] = datetime.now().strftime("%Y/%m/%d - %Hh%M")
            df["announce_publication_date"] = announce_scrap.get_publication_date()
            df["vehicle brand"] = announce_scrap.get_brand()
            df["vehicle type"] = announce_scrap.get_type()
            df["moto scoot"] = announce_scrap.get_moto()
            df["color"] = announce_scrap.get_color()
            df["vehicle condition"] = announce_scrap.get_cond()
            df["price"] = announce_scrap.get_price()
            df["city"] = announce_scrap.get_city()
            df["postal code"] = announce_scrap.get_postalcode()
            df["vehicle release date"] = announce_scrap.get_releasedate()
            df["mileage"] = announce_scrap.get_mileage()
            df["Fiscal power [HP]"] = announce_scrap.get_power()
            df["engine capacity [CC]"] = announce_scrap.get_capa()
            df["comments"] = announce_scrap.get_comment()
            df["seller"] = announce_scrap.get_seller()[0]
            df["seller_name"] = announce_scrap.get_seller()[1]

            # save images
            announce_scrap.get_images(announce_scrap.get_reference())

            # concatenate to csv and write
            try:
                data = pd.concat([data, df], axis=0)
                data.to_csv(path_or_buf = data_paruvendu, index=False)
            except:
                df.to_csv(path_or_buf = data_paruvendu, index=False)

            # deplacer le fichier html traité dans le vault
            subprocess.run(["mv", os.path.join(directory, html_file), "vault"])


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
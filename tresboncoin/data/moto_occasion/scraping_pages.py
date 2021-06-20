from datetime import datetime
import requests
from bs4 import BeautifulSoup
import time
import random
import pandas as pd
import os


PATH_TO_CSV = os.path.dirname(os.path.abspath(__file__)).replace('/moto_occasion', '') + '/scraping_outputs/moto-occasion.csv'
PATH_TO_LOG = os.path.dirname(os.path.abspath(__file__)).replace('/moto_occasion', '') + "/log.csv"
PATH_TO_FOLDER = os.path.dirname(os.path.abspath(__file__))
PATH_TO_PAGES_FOLDER = os.path.dirname(os.path.abspath(__file__)) + ('/pages')
PATH_TO_ANNONCES_FOLDER = os.path.dirname(os.path.abspath(__file__)) + ('/annonces')
PATH_TO_INDEX = os.path.dirname(os.path.abspath(__file__)) + '/index.csv'


def scraping_pages():
    try:
        # Start time of the pages scrapping
        start_time = datetime.now()

        # site to scrap
        source = 'moto-occasion'

        # log update
        log_import = pd.read_csv(PATH_TO_LOG)
        log_new = pd.DataFrame({'source': [source],
                                'step': ['scrap pages'],
                                'status': ['started'],
                                'time': [datetime.now()],
                                'details': [""]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv(PATH_TO_LOG, index=False)

        # init scrap
        page_number = 1
        scrap = True
        while scrap == True:

            print("page number:", page_number)

            # url to scrap
            url = f'http://moto-occasion.motorevue.com/motos?page={page_number}'
            response = requests.get(url)
            file_name = source + "-" + str(page_number) + "-" + start_time.strftime("%Y-%m-%d_%Hh%M")

            with open(PATH_TO_PAGES_FOLDER + f"/{file_name}.html", "w") as file:
                file.write(response.text)
                file.close()

            # check if page is empty, if yes stop scrapping
            soup = BeautifulSoup(response.content, "html.parser")
            warning = soup.find("div", class_="media-body").text.replace('\r', '').replace('\t', '').replace('\n', '').strip()
            if warning == 'Aucune annonce trouvée.':
                scrap = False

            #time.sleep(random.randint(1, 2))
            page_number += 1

        # End time
        end_time = datetime.now()
        td = end_time - start_time

        # log update
        log_import = pd.read_csv(PATH_TO_LOG)
        log_new = pd.DataFrame({'source': [source],
                                'step': ['scrap pages'],
                                'status': ['completed'],
                                'time': [datetime.now()],
                                'details': [f"{td.seconds/60} minutes elapsed, {page_number} pages scrapped"]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv(PATH_TO_LOG, index=False)

    except (ValueError, TypeError, NameError, KeyError, RuntimeWarning) as err:
        # log update
        log_import = pd.read_csv(PATH_TO_LOG)
        log_new = pd.DataFrame({'source': [source],
                                'step': ['scrap pages'],
                                'status': ['error'],
                                'time': [datetime.now()],
                                'details': [err]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv(PATH_TO_LOG, index=False)


if __name__ == "__main__":
    scraping_pages()

import requests
from bs4 import BeautifulSoup
import os
import codecs
from datetime import datetime
import time
import random
import pandas as pd

PATH_TO_CSV = os.path.dirname(os.path.abspath(__file__)).replace('/motovente', '') + '/scraping_outputs/motovente.csv'
PATH_TO_LOG = os.path.dirname(os.path.abspath(__file__)).replace('/motovente', '') + "/log.csv"
PATH_TO_FOLDER = os.path.dirname(os.path.abspath(__file__))
PATH_TO_PAGES_FOLDER = os.path.dirname(os.path.abspath(__file__)) + ('/pages')
PATH_TO_ANNONCES_FOLDER = os.path.dirname(os.path.abspath(__file__)) + ('/annonces')
PATH_TO_INDEX = os.path.dirname(os.path.abspath(__file__)) + '/index.csv'


def scraping_annonces():
    #try:
    # website source name
    source = 'motovente'

    # Start time
    start_time = datetime.now()

    # import previously scrapped
    df_import = pd.read_csv(PATH_TO_CSV)

    # log update
    log_import = pd.read_csv(PATH_TO_LOG)
    log_new = pd.DataFrame({'source': [source],
                            'step': ['scrap annonces'],
                            'status': ['started'],
                            'time': [datetime.now()],
                            'details': [""]})
    log = log_import.append(log_new, ignore_index=True)
    log.to_csv(PATH_TO_LOG, index=False)

    count = 0

    # iterate over html files in pages directory
    for filename in [file for file in os.listdir(PATH_TO_PAGES_FOLDER) if file.endswith(".html")]:
        f = codecs.open(PATH_TO_PAGES_FOLDER + f"{'/'+filename}", 'r')
        soup = BeautifulSoup(f, "html.parser")

        # find the url for bike and download as html file
        for bike in soup.find('ul', class_="lstannonce").find_all('li'):

            # initialization for the dataframe
            url_bike_ls = []
            reference_ls = []

            link = bike.find("a").get('href')
            url_site = 'http://motovente.com'
            url_bike = url_site + link
            reference = url_bike.split("-")[-1].replace('.html', '')

            reference_ls.append(reference)
            url_bike_ls.append(url_bike)

            # find the price of the bike from the feed
            price = int(bike.find('div', class_='z5').text.split('€')[0])

            # test if the bike with the same price is already in the databse
            test_rows = df_import.loc[(df_import['reference'] == reference) & (df_import['price'] == price)].shape[0]

            if test_rows == 0:
                response = requests.get(url_bike)
                file_name = source + "-" + reference + "-" + start_time.strftime("%Y-%m-%d_%Hh%M")

                with open(PATH_TO_ANNONCES_FOLDER + f"/{file_name}.html", "w") as file:
                    file.write(response.text)
                    file.close()

                # create index dataframe
                df = pd.DataFrame(list(zip(reference_ls, url_bike_ls)), columns=['reference', 'url'])

                # import index history
                history = pd.read_csv(PATH_TO_INDEX)

                # concatenate new and history
                final_df = history.append(df, ignore_index=True)

                # export to csv
                final_df.to_csv(PATH_TO_INDEX, index=False)

                #time.sleep(random.randint(1, 2))
                count += 1
        print("motovente - annonce number:", count)

        # delete html file
        os.remove(PATH_TO_PAGES_FOLDER+"/"+filename)

    # End time
    end_time = datetime.now()
    td = end_time - start_time

    # log update
    log_import = pd.read_csv(PATH_TO_LOG)
    log_new = pd.DataFrame({'source': [source],
                            'step': ['scrap annonces'],
                            'status': ['completed'],
                            'time': [datetime.now()],
                            'details': [f"{td.seconds/60} minutes elapsed, {count} annonces scrapped"]})
    log = log_import.append(log_new, ignore_index=True)
    log.to_csv(PATH_TO_LOG, index=False)
"""
    except (ValueError, TypeError, NameError, KeyError, RuntimeWarning) as err:
        # log update
        log_import = pd.read_csv(PATH_TO_LOG)
        log_new = pd.DataFrame({'source': [source],
                                'step': ['scrap annonces'],
                                'status': ['error'],
                                'time': [datetime.now()],
                                'details': [err]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv(PATH_TO_LOG, index=False)

"""
if __name__ == "__main__":
    scraping_annonces()

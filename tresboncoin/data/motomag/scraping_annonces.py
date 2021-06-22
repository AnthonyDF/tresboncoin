import requests
from bs4 import BeautifulSoup
import os
import codecs
from datetime import datetime
import time
import random
import pandas as pd
import os


PATH_TO_CSV = os.path.dirname(os.path.abspath(__file__)).replace('/motomag', '') + '/scraping_outputs/motomag.csv'
PATH_TO_LOG = os.path.dirname(os.path.abspath(__file__)).replace('/motomag', '') + "/log.csv"
PATH_TO_FOLDER = os.path.dirname(os.path.abspath(__file__))
PATH_TO_IMG_FOLDER = os.path.dirname(os.path.abspath(__file__)) + '/img'
PATH_TO_PAGES_FOLDER = os.path.dirname(os.path.abspath(__file__)) + ('/pages')
PATH_TO_ANNONCES_FOLDER = os.path.dirname(os.path.abspath(__file__)) + ('/annonces')
PATH_TO_INDEX = os.path.dirname(os.path.abspath(__file__)) + '/index.csv'


def scraping_annonces():
    try:
        # website source name
        source = 'motomag'

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

        # import previously scrapped
        df_import = pd.read_csv(PATH_TO_CSV)

        count_annonce = 0

        # iterate over html files in pages directory
        for filename in [file for file in os.listdir(PATH_TO_PAGES_FOLDER) if file.endswith(".html")]:
            f = codecs.open(PATH_TO_PAGES_FOLDER + f"{'/'+filename}", 'r')
            soup = BeautifulSoup(f, "html.parser")
            bike_soup = soup.find_all(class_="col-md-6 mt10")

            for bike in bike_soup:
                url_bike_ls = []
                reference_ls = []

                bike_url = bike.find("a").get('href')
                reference = bike_url.split("=")[-1]
                uniq_id = source + '-' + bike_url.split("=")[-1]

                reference_ls.append(uniq_id)
                url_bike_ls.append(bike_url)

                price = bike.find('span').text.replace(' €', '')

                # test if the bike with the same price is already in the databse
                test_rows = df_import.loc[(df_import['uniq_id'].astype(str) == str(uniq_id)) & (df_import['price'].astype(str) == str(price))].shape[0]

                if test_rows == 0:
                    count_annonce += 1

                    response = requests.get(bike_url)
                    file_name = source + "-" + reference + "-" + start_time.strftime("%Y-%m-%d_%Hh%M")

                    with open(PATH_TO_ANNONCES_FOLDER + f"/{file_name}.html", "w") as file:
                        file.write(response.text)
                        file.close()

                    # create index dataframe
                    df = pd.DataFrame(list(zip(reference_ls, url_bike_ls)), columns=['uniq_id', 'url'])

                    # import index history
                    history = pd.read_csv(PATH_TO_INDEX)

                    # concatenate new and history
                    final_df = history.append(df, ignore_index=True)
                    # final_df = df

                    # drop duplicates
                    final_df.drop_duplicates(inplace=True)

                    # export to csv
                    final_df.to_csv(PATH_TO_INDEX, index=False)

                    print("motomag - annonce number:", count_annonce)

                    # time.sleep(random.randint(2, 3))

            # delete html file
            os.remove(PATH_TO_PAGES_FOLDER + "/" + filename)

        # End time
        end_time = datetime.now()
        td = end_time - start_time

        # log update
        log_import = pd.read_csv(PATH_TO_LOG)
        log_new = pd.DataFrame({'source': [source],
                                'step': ['scrap annonces'],
                                'status': ['completed'],
                                'time': [datetime.now()],
                                'details': [f"{td.seconds/60} minutes elapsed, {count_annonce} annonces scrapped"]})
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


if __name__ == '__main__':
    scraping_annonces()

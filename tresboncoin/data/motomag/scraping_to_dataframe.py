from datetime import datetime
import requests
from bs4 import BeautifulSoup
import time
import random
import pandas as pd
import os

PATH_TO_CSV = os.path.dirname(os.path.abspath(__file__)).replace('/motomag', '') + '/scraping_outputs/motomag.csv'
PATH_TO_LOG = os.path.dirname(os.path.abspath(__file__)).replace('/motomag', '') + "/log.csv"


def scraping_to_dataframe():
    try:
        # Start time
        start_time = datetime.now()

        # site to scrap
        source = 'motomag'

        # import previously scrapped
        df_import = pd.read_csv(PATH_TO_CSV)

        # log update
        log_import = pd.read_csv(PATH_TO_LOG)
        log_new = pd.DataFrame({'source': [source],
                                'step': ['scrap pages'],
                                'status': ['started'],
                                'time': [datetime.now()],
                                'details': [""]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv(PATH_TO_LOG, index=False)

        # Scrap the first page to find the max page number and save this page as html
        url = 'https://www.motomag.com/-cote-argus-moto-scooter-petites-annonces-.html?debut_listingAnnoncesMoto=0#pagination_listingAnnoncesMoto'
        response = requests.get(url)

        soup = BeautifulSoup(response.content, "html.parser")
        max_page = int(soup.find(rel="nofollow").text)

        data = {'url': [],
                'uniq_id': [],
                'brand': [],
                'model': [],
                'price': [],
                'bike_year': [],
                'mileage': []}

        for page_number in range(0, max_page+1):
            # url to scrap
            url = f'https://www.motomag.com/-cote-argus-moto-scooter-petites-annonces-.html?debut_listingAnnoncesMoto={page_number*30}#pagination_listingAnnoncesMoto'
            bike_response = requests.get(url)
            page_soup = BeautifulSoup(bike_response.content, "html.parser")
            bike_soup = page_soup.find_all(class_="col-md-6 mt10")

            for bike in bike_soup:
                try:
                    bike_url = bike.find("a").get('href')
                    uniq_id = source + '-' + bike_url.split("=")[-1]
                    brand = bike.find(itemprop="description").text.split(' - ')[0]
                    model = bike.find(itemprop="description").text.split(' - ')[1].split(' (')[0]
                    price = bike.find('span').text.replace(' €', '')
                    year = int(bike.find(class_="article-txt pa5").text.split("\n")[-2].split(' - ')[0])
                    mileage = int(bike.find(class_="article-txt pa5").text.split("\n")[-2].split(' - ')[1].replace(' km', ''))

                    data['url'].append(bike_url)
                    data['uniq_id'].append(uniq_id)
                    data['brand'].append(brand)
                    data['model'].append(model)
                    data['price'].append(price)
                    data['bike_year'].append(year)
                    data['mileage'].append(mileage)
                except:
                    pass

            print(page_number)
           # time.sleep(random.randint(1, 2))

        df = pd.DataFrame(data)
        df = df.append(df_import)
        df = df.drop_duplicates(subset=['url'])
        df.to_csv(PATH_TO_CSV, index=False)

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


if __name__ == '__main__':
    scraping_to_dataframe()

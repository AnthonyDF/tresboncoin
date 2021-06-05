import requests
from bs4 import BeautifulSoup
import os
import codecs
from datetime import datetime
import time
import random
import pandas as pd


def scraping_annonces():
    try:
        # website source name
        source = 'moto-occasion'

        # directory of html pages
        directory = 'pages'

        # Start time
        start_time = datetime.now()

        # import previously scrapped
        df_import = pd.read_csv('moto-occasion.csv')

        # log update
        log_import = pd.read_csv('../log.csv')
        log_new = pd.DataFrame({'source': [source],
                                'step': ['scrap annonces'],
                                'status': ['started'],
                                'time': [datetime.now()],
                                'details': [""]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv('../log.csv', index=False)

        count = 0

        # iterate over html files in pages directory
        for filename in [file for file in os.listdir(directory) if file.endswith(".html")]:
            f = codecs.open(f"{directory+'/'+filename}", 'r')
            soup = BeautifulSoup(f, "html.parser")

            # find the url for bike and download as html file
            for bike in soup.find_all("li", class_="list-items__item"):

                # initialization for the dataframe
                url_bike_ls = []
                reference_ls = []

                # generate the bike url and reference
                link = bike.find("a", class_="list-items__link").get('href')
                url_site = 'http://moto-occasion.motorevue.com'
                url_bike = url_site + link
                reference = url_bike.split("/")[-1].replace('.html', '')

                reference_ls.append(reference)
                url_bike_ls.append(url_bike)

                # find the price of the bike from the feed
                price_soup = bike.find('div', class_='media-body').find('p', class_='list-items__price').find('span', class_='currency-price-wrap')
                price = price_soup.text.replace("€", "").replace("TTC", "").replace("\t","").replace(" ", "")
                price = float("".join(price.split()).replace(",", ".").replace('PrixNC','0'))

                # test if the bike with the same price is already in the databse
                test_rows = df_import.loc[(df_import['reference'] == reference) & (df_import['price'] == price)].shape[0]

                if test_rows == 0:
                    response = requests.get(url_bike)
                    file_name = source + "-" + reference + "-" + start_time.strftime("%Y-%m-%d_%Hh%M")

                    with open(f"annonces/{file_name}.html", "w") as file:
                        file.write(response.text)
                        file.close()

                    # create index dataframe
                    df = pd.DataFrame(list(zip(reference_ls, url_bike_ls)), columns=['reference', 'url'])

                    # import index history
                    history = pd.read_csv('index.csv')

                    # concatenate new and history
                    final_df = history.append(df, ignore_index=True)

                    # export to csv
                    final_df.to_csv('index.csv', index=False)

                    time.sleep(random.randint(3, 9))
                    count += 1
                    print(count)

            # delete html file
            os.remove(directory+"/"+filename)

        # End time
        end_time = datetime.now()
        td = end_time - start_time

        # log update
        log_import = pd.read_csv('../log.csv')
        log_new = pd.DataFrame({'source': [source],
                                'step': ['scrap annonces'],
                                'status': ['completed'],
                                'time': [datetime.now()],
                                'details': [f"{td.seconds/60} minutes elapsed, {count} annonces scrapped"]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv('../log.csv', index=False)

    except (ValueError, TypeError, NameError, KeyError, RuntimeWarning) as err:
        # log update
        log_import = pd.read_csv('../log.csv')
        log_new = pd.DataFrame({'source': [source],
                                'step': ['scrap annonces'],
                                'status': ['error'],
                                'time': [datetime.now()],
                                'details': [err]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv('../log.csv', index=False)


if __name__ == "__main__":
    scraping_annonces()

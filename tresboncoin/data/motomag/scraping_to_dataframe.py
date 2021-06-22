from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import os
import shutil
import codecs
import re

PATH_TO_CSV = os.path.dirname(os.path.abspath(__file__)).replace('/motomag', '') + '/scraping_outputs/motomag.csv'
PATH_TO_LOG = os.path.dirname(os.path.abspath(__file__)).replace('/motomag', '') + "/log.csv"
PATH_TO_FOLDER = os.path.dirname(os.path.abspath(__file__))
PATH_TO_IMG_FOLDER = os.path.dirname(os.path.abspath(__file__)) + '/img'
PATH_TO_PAGES_FOLDER = os.path.dirname(os.path.abspath(__file__)) + ('/pages')
PATH_TO_ANNONCES_FOLDER = os.path.dirname(os.path.abspath(__file__)) + ('/annonces')
PATH_TO_INDEX = os.path.dirname(os.path.abspath(__file__)) + '/index.csv'


def scraping_to_dataframe():
    try:
        source = 'motomag'
        # Start time
        start_time = datetime.now()

        # log update
        log_import = pd.read_csv(PATH_TO_LOG)
        log_new = pd.DataFrame({'source': [source],
                                'step': ['to dataframe'],
                                'status': ['started'],
                                'time': [datetime.now()],
                                'details': [""]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv(PATH_TO_LOG, index=False)

        # import index
        index_df = pd.read_csv(PATH_TO_INDEX)

        count = 0

        for filename in [file for file in os.listdir(PATH_TO_ANNONCES_FOLDER) if file.endswith(".html")]:

            reference = int(filename.split("-")[1])
            uniq_id = source+"-"+str(reference)

            # initialize list for dataframe
            data = {'uniq_id': [],
                    'reference': [],
                    'brand': [],
                    'model': [],
                    'price': [],
                    'bike_year': [],
                    'bike_type': [],
                    'engine_size': [],
                    'mileage': []}

            f = codecs.open(f"{PATH_TO_ANNONCES_FOLDER +'/'+filename}", 'r')
            model_soup = BeautifulSoup(f, "html.parser")

            try:
                count += 1
                engine_size = int(model_soup.find('div', class_='f17').text.split('cm3')[0])
                mileage = int(model_soup.find('div', class_='f17').text.split('cm3')[1].split(' km')[0])
                bike_type = model_soup.select('h1.f14')[0].text
                brand = model_soup.select('h1.fPtNaB:nth-child(2)')[0].text.split(' - ')[0].lower()
                model = model_soup.select('h1.fPtNaB:nth-child(2)')[0].text.split(' - ')[1].lower()
                price = model_soup.find(itemprop='prix').text.replace(' €', '')
                year = model_soup.find('div', class_='f17').text.split('Année du modèle : ')[1]
                bike_year = re.match(r'\d{4}', year).group(0)

                data['uniq_id'].append(uniq_id)
                data['reference'].append(reference)
                data['brand'].append(brand)
                data['model'].append(model)
                data['price'].append(price)
                data['bike_year'].append(bike_year)
                data['bike_type'].append(bike_type)
                data['mileage'].append(mileage)
                data['engine_size'].append(engine_size)
                # time.sleep(random.randint(1, 2))

                df = pd.DataFrame(data)
                df['source'] = source
                df['scraped_date'] = datetime.now()

                # merge dataframes
                df = df.merge(index_df, on='uniq_id', how='left')

                # import history
                history = pd.read_csv(PATH_TO_CSV)

                # concatenate new and history
                final_df = history.append(df, ignore_index=True)

                # export to csv
                final_df.to_csv(PATH_TO_CSV, index=False)

            except:
                pass

            # move file to vault after process
            # source path
            source_folder = PATH_TO_ANNONCES_FOLDER + f"/{filename}"
            # destination path
            destination = PATH_TO_ANNONCES_FOLDER + f"/vault/{filename}"
            # Move the content of source_folder to destination
            shutil.move(source_folder, destination)

        # End time
        end_time = datetime.now()
        td = end_time - start_time

        # log update
        log_import = pd.read_csv(PATH_TO_LOG)
        log_new = pd.DataFrame({'source': [source],
                                'step': ['scrap pages'],
                                'status': ['completed'],
                                'time': [datetime.now()],
                                'details': [f"{td.seconds/60} minutes elapsed, {count} pages scrapped"]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv(PATH_TO_LOG, index=False)

        # remove duplicates
        df = pd.read_csv(PATH_TO_CSV)
        df.drop_duplicates(subset=['reference', 'price'], inplace=True)
        df.to_csv(PATH_TO_CSV, index=False)

    except (ValueError, TypeError, NameError, KeyError, RuntimeWarning) as err:
        # log update
        log_import = pd.read_csv(PATH_TO_LOG)
        log_new = pd.DataFrame({'source': [source],
                                'step': ['to dataframe'],
                                'status': ['error'],
                                'time': [datetime.now()],
                                'details': [err]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv(PATH_TO_LOG, index=False)


if __name__ == '__main__':
    scraping_to_dataframe()

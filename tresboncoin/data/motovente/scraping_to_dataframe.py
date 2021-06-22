import requests
from bs4 import BeautifulSoup
from PIL import Image
import os
import glob
import pandas as pd
import time
import random
import codecs
import shutil
from datetime import datetime
from tresboncoin.fuzzy_match import remove_punctuations
import string
import unidecode


PATH_TO_CSV = os.path.dirname(os.path.abspath(__file__)).replace('/motovente', '') + '/scraping_outputs/motovente.csv'
PATH_TO_LOG = os.path.dirname(os.path.abspath(__file__)).replace('/motovente', '') + "/log.csv"
PATH_TO_FOLDER = os.path.dirname(os.path.abspath(__file__))
PATH_TO_IMG_FOLDER = os.path.dirname(os.path.abspath(__file__)) + '/img'
PATH_TO_PAGES_FOLDER = os.path.dirname(os.path.abspath(__file__)) + ('/pages')
PATH_TO_ANNONCES_FOLDER = os.path.dirname(os.path.abspath(__file__)) + ('/annonces')
PATH_TO_INDEX = os.path.dirname(os.path.abspath(__file__)) + '/index.csv'


def scraping_to_dataframe():
    try:
        source = 'motovente'
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

            f = codecs.open(f"{PATH_TO_ANNONCES_FOLDER +'/'+filename}", 'r')
            model_soup = BeautifulSoup(f, "html.parser")

            #try:
            Raw_text = model_soup.find(class_='infovehicule').text
            keywords = [row.text.strip() for row in model_soup.find(class_='infovehicule').find_all('p')]

            for word in keywords:
                Raw_text = Raw_text.replace(word, '<>')

            values = Raw_text.split('<>')[1:]

            data = {}

            for key in keywords:
                for value in values:
                    data[key] = [value]
                    values.remove(value)
                    break

            data['reference'] = reference
            data['uniq_id'] = uniq_id
            price =int(model_soup.find('p', style="font-size:18px;color:red;font-weight:bold;text-align:center;margin-top:5px;margin-bottom:5px;").text.replace('\x80', ''))
            data['price'] = price

            '''
            # pictures
            try:
                if str(reference) not in [file.split('-')[2] for file in glob.glob(PATH_TO_IMG_FOLDER + '/*')]:
                    img_soup = bike_soup.find("div", id="announce_content_images_container").find_all("img")
                    k = 0
                    for image in img_soup:
                        image_url = image['src']
                        img_data = requests.get(image_url).content
                        with open(PATH_TO_IMG_FOLDER + f'/{uniq_id}-{k}.jpg', 'wb') as handler:
                            handler.write(img_data)

                        image = Image.open(PATH_TO_IMG_FOLDER + f'/{uniq_id}-{k}.jpg')
                        ratio = image.size[0] / image.size[1]
                        image = image.resize((300, int(300/ratio)))
                        image.save(PATH_TO_IMG_FOLDER + f'/{uniq_id}-{k}.jpg', optimize=True, quality = 50)
                        k += 1
                        #time.sleep(random.randint(1, 2))
            except:
                pass
            '''

            df = pd.DataFrame(data)
            df.columns = [remove_punctuations(unidecode.unidecode(column)).lower() for column in df.columns]

            df.rename(columns={'marque': 'brand',
                               'modele': 'model',
                               'cylindree': 'engine_size',
                               'categorie': 'bike_type',
                               'energie': 'energy',
                               'annee': 'bike_year',
                               'date 1ere mise en circulation': 'year_circulation',
                               'kilometrage': 'mileage',
                               'couleur': 'bike_color',
                               'garantie': 'warranty',
                               'premiere main': 'first_hand'}, inplace=True)

            df['source'] = source
            df['scraped_date'] = datetime.now()

            # merge dataframes
            df = df.merge(index_df, on='reference', how='left')
            df.engine_size = df.engine_size.str.replace(' cm3', '')
            df.mileage = df.mileage.str.replace(' km', '')

            # import history
            history = pd.read_csv(PATH_TO_CSV)

            # concatenate new and history
            final_df = history.append(df, ignore_index=True)

            # export to csv
            final_df.to_csv(PATH_TO_CSV, index=False)
            #except:
            #    pass

            # move file to vault after process
            # source path
            source_folder = PATH_TO_ANNONCES_FOLDER + f"/{filename}"
            # destination path
            destination = PATH_TO_ANNONCES_FOLDER + f"/vault/{filename}"
            # Move the content of source_folder to destination
            shutil.move(source_folder, destination)
            count += 1

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

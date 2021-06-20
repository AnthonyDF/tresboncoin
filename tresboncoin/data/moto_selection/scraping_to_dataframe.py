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

PATH_TO_CSV = os.path.dirname(os.path.abspath(__file__)).replace('/moto_selection', '') + '/scraping_outputs/moto-selection.csv'
PATH_TO_LOG = os.path.dirname(os.path.abspath(__file__)).replace('/moto_selection', '') + "/log.csv"
PATH_TO_FOLDER = os.path.dirname(os.path.abspath(__file__))
PATH_TO_IMG_FOLDER = os.path.dirname(os.path.abspath(__file__)) + '/img'
PATH_TO_PAGES_FOLDER = os.path.dirname(os.path.abspath(__file__)) + ('/pages')
PATH_TO_ANNONCES_FOLDER = os.path.dirname(os.path.abspath(__file__)) + ('/annonces')
PATH_TO_INDEX = os.path.dirname(os.path.abspath(__file__)) + '/index.csv'


def scraping_to_dataframe():
    try:
        source = 'moto-selection'
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

        count = 1

        for filename in [file for file in os.listdir(PATH_TO_ANNONCES_FOLDER) if file.endswith(".html")]:

            reference = int(filename.split("-")[2])
            uniq_id = source+"-"+str(reference)

            # initialize list for dataframe
            uniq_id_ls = []
            reference_ls = []
            bike_type_ls = []
            bike_size_ls = []
            bike_year_ls = []
            bike_year_circulation_ls = []
            bike_km_ls = []
            bike_color_ls = []
            bike_warranty_ls = []
            bike_description_ls = []
            price_ls = []
            bike_brand_ls = []
            bike_model_ls = []
            vendor_city_ls = []
            vendor_type_ls = []

            f = codecs.open(f"{PATH_TO_ANNONCES_FOLDER +'/'+filename}", 'r')
            bike_soup = BeautifulSoup(f, "html.parser")

            try:
                bike_type = bike_soup.find(id="item_data").find_all(class_="table_value")[0].text.upper()
                bike_size = int(bike_soup.find(id="item_data").find_all(class_="table_value")[1].text.replace("N.C.", "0").replace('cm3',''))
                bike_year = int(bike_soup.find(id="item_data").find_all(class_="table_value")[2].text.replace("N.C.", "0").replace('Année ',''))
                bike_year_circulation = int(bike_soup.find(id="item_data").find_all(class_="table_value")[3].text.replace("N.C.","0"))
                bike_km = int(bike_soup.find(id="item_data").find_all(class_="table_value")[4].text.replace("N.C.", "").replace(' km','').replace(' ',''))
                bike_color = bike_soup.find(id="item_data").find_all(class_="table_value")[5].text.replace("N.C.", "")
                bike_warranty = bike_soup.find(id="item_data").find_all(class_="table_value")[6].text.replace("N.C.", "")
                bike_description = bike_soup.select('#announce_content > p:nth-child(10)')[0].text
                price = int(bike_soup.select('#price > span:nth-child(1)')[0].text.replace(" ", ""))
                bike_brand = bike_soup.select('#announce_content > h2:nth-child(2) > span:nth-child(2)')[0].text.upper()
                bike_model = bike_soup.select('#announce_content > h2:nth-child(2) > span:nth-child(3)')[0].text.upper()
                vendor_city = bike_soup.select('#announce_content > h2:nth-child(2) > span:nth-child(4)')[0].text.split(')')[-1]
                vendor_type = bike_soup.find(id="seller_status").text.replace(".","")

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

                uniq_id_ls.append(uniq_id)
                reference_ls.append(reference)
                bike_type_ls.append(bike_type)
                bike_size_ls.append(bike_size)
                bike_year_ls.append(bike_year)
                bike_year_circulation_ls.append(bike_year_circulation)
                bike_km_ls.append(bike_km)
                bike_color_ls.append(bike_color)
                bike_warranty_ls.append(bike_warranty)
                bike_description_ls.append(bike_description)
                price_ls.append(price)
                bike_brand_ls.append(bike_brand)
                bike_model_ls.append(bike_model)
                vendor_city_ls.append(vendor_city)
                vendor_type_ls.append(vendor_type)

                df = pd.DataFrame(list(zip(
                    uniq_id_ls,
                    reference_ls,
                    bike_type_ls,
                    bike_size_ls,
                    bike_year_ls,
                    bike_year_circulation_ls,
                    bike_km_ls,
                    bike_color_ls,
                    bike_warranty_ls,
                    bike_description_ls,
                    price_ls,
                    bike_brand_ls,
                    bike_model_ls,
                    vendor_city_ls,
                    vendor_type_ls)),
                             columns=[
                                'uniq_id',
                                'reference',
                                'bike_type',
                                'bike_size',
                                'bike_year',
                                'bike_year_circulation',
                                'bike_km',
                                'bike_color',
                                'bike_warranty',
                                'bike_description',
                                'price',
                                'bike_brand',
                                'bike_model',
                                'vendor_city',
                                'vendor_type'])

                df['source'] = source
                df['scrap_date'] = datetime.now()

                # merge dataframes
                df = df.merge(index_df, on='reference', how='left')

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

            print("Annonce number", count)
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

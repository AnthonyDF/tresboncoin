import requests
from bs4 import BeautifulSoup
import os
import codecs
from datetime import datetime
import pandas as pd
import re


PATH_TO_CSV = os.path.dirname(os.path.abspath(__file__)).replace('/motomag', '') + '/scraping_outputs/motomag.csv'
PATH_TO_LOG = os.path.dirname(os.path.abspath(__file__)).replace('/motomag', '') + "/log.csv"
PATH_TO_FOLDER = os.path.dirname(os.path.abspath(__file__))
PATH_TO_IMG_FOLDER = os.path.dirname(os.path.abspath(__file__)) + '/img'
PATH_TO_PAGES_FOLDER = os.path.dirname(os.path.abspath(__file__)) + ('/pages')
PATH_TO_ANNONCES_FOLDER = os.path.dirname(os.path.abspath(__file__)) + ('/annonces')
PATH_TO_INDEX = os.path.dirname(os.path.abspath(__file__)) + '/index.csv'


def scraping_to_dataframe(res, code, ref):
    try:
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

        count = 0

        reference = int(ref)
        uniq_id = source + "-" + str(ref)

        # initialize list for dataframe
        data = {'uniq_id': [],
                'reference': [],
                'brand': [],
                'model': [],
                'price': [],
                'bike_year': [],
                'bike_type': [],
                'engine_size': [],
                'mileage': [],
                'source': [],
                'scraped_date': [],
                'url': [],
                'age': [],
                'code_annonce': [code]}

        model_soup = BeautifulSoup(res.content, "html.parser")

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
            data["age"].append(int(datetime.now().strftime("%Y")) - int(bike_year))
            data['url'].append("https://www.motomag.com/spip.php?page=pamoto&id_annonce=" + str(reference))
            data['source'].append(source)
            data['scraped_date'].append(datetime.now())
            df = pd.DataFrame(data)
            # loading existing csv
            data_hist = pd.read_csv(PATH_TO_CSV)
            # merge dataframes
            new_csv = pd.concat([data_hist, df], axis=0)
            # export to csv
            new_csv.to_csv(PATH_TO_CSV, index=False)
            print("Motomag line added. New Shape: " + str(new_csv.shape[0]))
        except:
            print("error encountered in file: " + filename)

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
        print("error occured")


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

                bike_url = bike.find("a").get('href')
                reference = bike_url.split("=")[-1]
                # uniq_id = source + '-' + bike_url.split("=")[-1]
                moto_carac = bike.select("div[class*='article-txt pa5']")
                code_annonce = moto_carac[0].text.replace("\n","").replace(" ", "")
                # price = bike.find('span').text.replace(' €', '')

                # test if the bike with the same price is already in the databse
                if code_annonce not in list(df_import["code_annonce"]):
                    count_annonce += 1

                    response = requests.get(bike_url)
                    scraping_to_dataframe(response, code_annonce, reference)

                else:
                    print("Announce already in dataset.")

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

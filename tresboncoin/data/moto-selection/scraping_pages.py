from datetime import datetime
import requests
from bs4 import BeautifulSoup
import time
import random
import pandas as pd


def scraping_pages():
    try:
        # Start time
        start_time = datetime.now()

        # site to scrap
        source = 'moto-selection'

        # log update
        log_import = pd.read_csv('../log.csv')
        log_new = pd.DataFrame({'source': [source],
                                'step': ['scrap pages'],
                                'status': ['started'],
                                'time': [datetime.now()],
                                'details': [""]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv('../log.csv', index=False)

        # Scrap the first page to find the max page number and save this page as html
        url = 'http://www.moto-selection.com/moto-occasion/page-1.html'
        response = requests.get(url)
        file_name = source + "-" + str(1) + "-" + start_time.strftime("%Y-%m-%d_%Hh%M")

        with open(f"pages/{file_name}.html", "w") as file:
            file.write(response.text)
            file.close()

        soup = BeautifulSoup(response.content, "html.parser")
        max_page = int(soup.select('#announces_list > h2:nth-child(2) > span:nth-child(2)')[0].text.split(' / ')[-1])

        for page_number in range(2, max_page+1):
            # url to scrap
            url = f'http://www.moto-selection.com/moto-occasion/page-{page_number}.html'
            response = requests.get(url)
            file_name = source + "-" + str(page_number) + "-" + start_time.strftime("%Y-%m-%d_%Hh%M")

            with open(f"pages/{file_name}.html", "w") as file:
                file.write(response.text)
                file.close()

            print('Page number: ', page_number)
            time.sleep(random.randint(2, 3))

        # End time
        end_time = datetime.now()
        td = end_time - start_time

        # log update
        log_import = pd.read_csv('../log.csv')
        log_new = pd.DataFrame({'source': [source],
                                'step': ['scrap pages'],
                                'status': ['completed'],
                                'time': [datetime.now()],
                                'details': [f"{td.seconds/60} minutes elapsed, {page_number} pages scrapped"]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv('../log.csv', index=False)

    except (ValueError, TypeError, NameError, KeyError, RuntimeWarning) as err:
        # log update
        log_import = pd.read_csv('../log.csv')
        log_new = pd.DataFrame({'source': [source],
                                'step': ['scrap pages'],
                                'status': ['error'],
                                'time': [datetime.now()],
                                'details': [err]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv('../log.csv', index=False)


if __name__ == '__main__':
    scraping_pages()

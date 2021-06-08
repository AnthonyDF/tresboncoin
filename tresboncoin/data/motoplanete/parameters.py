import numpy as np
import pandas as pd

headers = {
    'authority': 'www.motoplanete.com',
    'cache-control': 'max-age=0',
    'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
    'sec-ch-ua-mobile': '?0',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-user': '?1',
    'sec-fetch-dest': 'document',
    'referer': 'https://www.motoplanete.com/',
    'accept-language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7'
}

motoplanete_page_url = "https://www.motoplanete.com/occasion-moto.html?nom=&annee=&part_pro=&cylindree-min=50&cylindree-max=3000&prix-min=0&prix-max=20000&limit=100&sort=4&offset="

motoplanete_announce_template = pd.DataFrame({"url": [np.nan],
                                              "unique id": [np.nan],
                                              "date_scrapped": [np.nan],
                                              "announce_publication_date": [np.nan],
                                              "vehicle brand": [np.nan],
                                              "vehicle type": [np.nan],
                                              "color": [np.nan],
                                              "hand": [np.nan],
                                              "price": [np.nan],
                                              "city": [np.nan],
                                              "postal code": [np.nan],
                                              "seller": [np.nan],
                                              "seller_name": [np.nan],
                                              "vehicle release date": [np.nan],
                                              "mileage": [np.nan],
                                              "engine capacity [CC]": [np.nan],
                                              "comments": [np.nan]})

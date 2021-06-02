import numpy as np
import pandas as pd

headers = {
    'authority': 'www.paruvendu.fr',
    'cache-control': 'max-age=0',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="90", "Google Chrome";v="90"',
    'sec-ch-ua-mobile': '?0',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'sec-fetch-site': 'none',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-user': '?1',
    'sec-fetch-dest': 'document',
    'accept-language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7'
}

paruvendu_page_url = "https://www.paruvendu.fr/auto-moto/listefo/default/default?moto-typeRech=&r=VMOMO000&px1=ex:%2050000&r2=&codeINSEE=&lo=&pa=&ray=100&cy=&nrj=&km1=&a0=&fulltext=&p="

paruvendu_announce_template = pd.DataFrame({"url": [np.nan],
                                            "reference": [np.nan],
                                            "unique id": [np.nan],
                                            "date_scrapped": [np.nan],
                                            "announce_publication_date": [np.nan],
                                            "vehicle brand": [np.nan],
                                            "vehicle type": [np.nan],
                                            "moto scoot": [np.nan],
                                            "color": [np.nan],
                                            "vehicle condition": [np.nan],
                                            "price": [np.nan],
                                            "city": [np.nan],
                                            "postal code": [np.nan],
                                            "vehicle release date": [np.nan],
                                            "mileage": [np.nan],
                                            "Fiscal power [HP]": [np.nan],
                                            "engine capacity [CC]": [np.nan],
                                            "comments": [np.nan],
                                            "seller": [np.nan],
                                            "seller_name": [np.nan]})

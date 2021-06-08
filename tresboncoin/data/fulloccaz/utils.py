from datetime import datetime
import re
from scraper import *


def save_page_list(site_name, req, page=1):
    datetime_1 = datetime.now().strftime("%Y-%m-%d_%Hh%M")
    page_list_name = site_name + "-" + datetime_1 + "-" + str(page)
    with open("pages/" + page_list_name + ".html", "w") as file:
        file.write(req.text)
        file.close()
    return "pages/" + page_list_name + ".html"


def save_page_uniq(site_name, req, uniq_id):
    datetime_1 = datetime.now().strftime("%Y-%m-%d_%Hh%M")
    page_name = site_name + "-" + uniq_id + "-" + datetime_1
    with open("annonces/" + page_name + ".html", "w") as file:
        file.write(req.text)
        file.close()


def add_the_announce(df, uniq_id, price):
    if float(uniq_id) in list(df["unique id"]):
        index_ = df.index[df['unique id'] == float(uniq_id)].tolist()[0]

        # if an announce with same uniq id but different price is found, return true and add
        # or return False and skip
        return df.iloc[index_]["price"] != price

    # else, return true and add
    return True


def good_soup_selector(res_soup1, res_soup2):

    if res_soup1 is None:
        if res_soup2 is None:
            return None
        else:
            return res_soup2
    else:
        return res_soup1


def cleanhtml(raw_html):
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, ' ', raw_html)
    return cleantext

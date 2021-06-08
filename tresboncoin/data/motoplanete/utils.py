import datetime
import chromedriver_binary
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import re

def get_source_selenium(url_):
    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options, executable_path="/Users/victor/Downloads/chromedriver")
    driver.get(url_)
    page_source = driver.page_source
    driver.quit()
    return page_source

def save_page_list(site_name, req, page=1):
    datetime_1 = datetime.datetime.now().strftime("%Y-%m-%d_%Hh%M")
    page_list_name = site_name + "-" + datetime_1 + "-" + str(page)
    with open("pages/" + page_list_name + ".html", "w")  as file:
        file.write(req)
        file.close()
    return "pages/" + page_list_name + ".html"

def save_page_uniq(site_name, req, uniq_id):
    datetime_1 = datetime.datetime.now().strftime("%Y-%m-%d_%Hh%M")
    page_name = site_name + "-" + uniq_id + "-" + datetime_1
    with open("annonces/" + page_name + ".html", "w")  as file:
        file.write(req.text)
        file.close()

def add_the_announce(df, uniq_id, price):
    if int(uniq_id) in list(df["unique id"]):
        index_ = df.index[df['unique id'] == int(uniq_id)].tolist()[0]

        # if an announce with same uniq id but different price is found, return true and add
        # or return False and skip
        return df.iloc[index_]["price"]!=price

    # else, return true and add
    return True

def cleanhtml(raw_html):
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, ' ', raw_html)
    return cleantext

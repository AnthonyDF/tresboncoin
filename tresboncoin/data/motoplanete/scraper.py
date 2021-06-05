from bs4 import BeautifulSoup
import numpy as np
import requests
import os
import re
from utils import *
from PIL import Image
import PIL

class motoplanete_page_scraper():

    def __init__(self, page=None, announce=None):
        """
            page: 100 announces soup
            announce : single announce soup
        """
        self.page = page
        self.announce = announce


    ########################
    #### Page scraper
    ########################

    def get_prices_from_pages(self):

        # set empty price list
        price_list = []

        # extract price
        step1 = self.page.select('div[class*="table-result"]')
        if len(step1)>0:
            step2 = step1[0].select('div[class*="tr"]')
            if len(step2)>0:
                for l in step2[1:]:
                    if "pub_native_web" not in l.attrs["class"]:
                        try:
                            price_list.append(float("".join(re.findall('[0-9]', l.select('div')[3].text))))
                        except:
                            price_list.append(np.nan)
        #
        return price_list


    def get_references_from_url_list(self, url_list_):

        # set empty ref list
        ref_list = []

        for k in url_list_:
            try:
                ref_list.append(k.split("/")[4])
            except:
                ref_list.append(np.nan)
        #
        return ref_list


    def get_urls_from_pages(self):

        # set empty url list
        url_list = []

        # extract url
        step1 = self.page.select('div[class*="table-result"]')
        if len(step1)>0:
            step2 = step1[0].select('div[class*="tr"]')
            if len(step2)>0:
                for l in step2[1:]:
                    if "pub_native_web" not in l.attrs["class"]:
                        try:
                            url_list.append("https://www.motoplanete.com" + l.select('div[class*="name"]')[0].select('a')[0].attrs["href"])
                        except:
                            url_list.append(np.nan)
        #
        return url_list


    ########################
    #### Announce scraper
    ########################

    def get_url(self):

        # extract url
        step1 = self.announce.select('meta[property*="og:url"]')
        if len(step1)>0:
            return step1[0].attrs["content"].split("?")[0]
        else:
            return None


    def get_uniq_id(self, url_):
        return url_.split("occasion-moto")[1].split("/")[1]


    def get_publication_date(self):

        # extract
        step1 = self.announce.select('ul[class*="details"]')
        if len(step1)>0:
            for k in step1[0].select("li"):
                if k.select('i')[0].attrs["class"][0] == "icon-profile":
                    return k.select("b")[0].text
        else:
            return np.nan


    def get_brand_and_model(self):

        # extract
        step1 = self.announce.select('h1')
        if len(step1)>0:
            return step1[0].text
        else:
            return None


    def get_type(self):

        # extract
        step1 = self.announce.select('ul[class*="details"]')
        if len(step1)>0:
            for k in step1[0].select("li"):
                if k.select('i')[0].attrs["class"][0] == "icon-roadster-2":
                    return k.select("b")[0].text
        else:
            return np.nan


    def get_color(self):

        # extract
        step1 = self.announce.select('ul[class*="details"]')
        if len(step1)>0:
            for k in step1[0].select("li"):
                if k.select('i')[0].attrs["class"][0] == "icon-format_color_fill":
                    return k.select("b")[0].attrs["class"][-1]
        else:
            return np.nan


    def get_hand(self):

        # extract
        step1 = self.announce.select('ul[class*="details"]')
        if len(step1)>0:
            for k in step1[0].select("li"):
                if k.select('i')[0].attrs["class"][0] == "icon-user":
                    return k.text
        else:
            return np.nan


    def get_price(self):

        # extract
        step1 = self.announce.select('ul[class*="details"]')
        if len(step1)>0:
            for k in step1[0].select("li"):
                if k.select('i')[0].attrs["class"][0] == "icon-eur":
                    try:
                        return float("".join(re.findall("[0-9]", k.select("b")[0].text)))
                    except:
                        return np.nan
        else:
            return np.nan


    def get_city(self):

        # extract
        step1 = self.announce.select('div[class*="contact"]')
        if len(step1)>0:
            try:
                return step1[0].select("span")[-1].text
            except:
                return np.nan
        return None


    def get_postal(self):

        # extract
        step1 = self.announce.select('div[class*="contact"]')
        if len(step1)>0:
            try:
                return step1[0].select("span")[0].text
            except:
                return np.nan
        return None


    def get_release(self):

        # extract
        step1 = self.announce.select('ul[class*="details"]')
        if len(step1)>0:
            for k in step1[0].select("li"):
                if k.select('i')[0].attrs["class"][0] == "icon-profile":
                    try:
                        return k.select("b")[0].text
                    except:
                        return np.nan
        else:
            return np.nan


    def get_mileage(self):

        # extract
        step1 = self.announce.select('ul[class*="details"]')
        if len(step1)>0:
            for k in step1[0].select("li"):
                if k.select('i')[0].attrs["class"][0] == "icon-meter":
                    try:
                        return float("".join(re.findall("[0-9]", k.select("b")[0].text)))
                    except:
                        return np.nan
        else:
            return np.nan


    def get_capa(self):

        # extract
        step1 = self.announce.select('ul[class*="details"]')
        if len(step1)>0:
            for k in step1[0].select("li"):
                if k.select('i')[0].attrs["class"][0] == "icon-picto-pieces-moteur":
                    try:
                        return float("".join(re.findall("[0-9]", k.select("b")[0].text)))
                    except:
                        return np.nan
        else:
            return np.nan


    def get_comment(self):

        # extract
        step1 = self.announce.select('div[class*="description"]')
        if len(step1)>0:
            try:
                return cleanhtml(step1[0].text.replace("\n", " ").replace("\t", " ").replace("  ", "").strip())
            except:
                return ""
        return ""


    def get_seller(self):

        # count
        count_ = 0

        # extract
        step1 = self.announce.select('div[class*="contact"]')
        if len(step1)>0:
            step2 = step1[0].select('p')
            while step2[count_].text != "Concession":
                count_+=1
            try:
                return step1[0].select('p')[count_+1].text
            except:
                return np.nan
        return None


    def get_seller_name(self):

        # count
        count_ = 0

        # extract
        step1 = self.announce.select('div[class*="contact"]')
        if len(step1)>0:
            step2 = step1[0].select('p')
            while step2[count_].text != "Contact":
                count_+=1
            try:
                return step1[0].select('p')[count_+1].text
            except:
                return np.nan
        return None


    def get_images(self, uniq_id):

        # get images url list
        image_list = []

        # extract
        step1 = self.announce.select('img[class*="swiper-lazy"]')

        if len(step1)>0:
            for k in step1:
                if "data-src" in k.attrs:
                    image_list.append("https://www.motoplanete.com" + k.attrs['data-src'])

        # save images
        k=1
        for image_url in image_list[0:3]:
            image_name = f'images/{uniq_id}-{k}.jpg'
            if os.path.isfile(image_name) is False:
                img_data = requests.get(image_url).content
                with open(image_name, 'wb') as handler:
                    handler.write(img_data)
                try:
                    image = Image.open(image_name)
                    ratio = image.size[0] / image.size[1]
                    image = image.resize((300,int(300/ratio)))
                    image.save(f'images/{uniq_id}-{k}.jpg',optimize = True, quality = 50)
                except:
                    pass
            k+=1

        return





if __name__ == "__main__":

    pagelist = os.listdir("pages")

    if len(pagelist) != 0:
        with open(os.path.join("pages", pagelist[0]), 'r') as f:
            readable_html = f.read()
        soup_ = BeautifulSoup(readable_html, 'html.parser')
        scrap = motoplanete_page_scraper(page=soup_)

        url_list_ = scrap.get_urls_from_pages()
        for url_single, uniq_id, price_ in zip(url_list_, scrap.get_references_from_url_list(url_list_), scrap.get_prices_from_pages()):
            print(url_single, uniq_id, price_)

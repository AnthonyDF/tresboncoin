import numpy as np
import requests
import os
import re
from PIL import Image
from utils import *


class fulloccaz_page_scraper():

    def __init__(self, page=None, announce=None):
        """
            page: 12 announces soup
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
        step1 = self.page.select('div[id*="IDVignettes"]')
        if len(step1) > 0:
            step2 = step1[0].select('div[class*="CLPrix"]')
            if len(step2) > 0:
                for k in step2:
                    try:
                        price_list.append(float("".join(re.findall("[0-9]", k.text.replace("\n", "").replace("\t", "")))))
                    except:
                        price_list.append(np.nan)
        return price_list


    def get_urls_from_pages(self):

        # set empty url list
        url_list = []

        # extract url
        step1 = self.page.select('div[id*="IDVignettes"]')
        if len(step1) > 0:
            step2 = step1[0].select('a')
            if len(step2) > 0:
                for k in step2:
                    if "href" in k.attrs:
                        try:
                            url_list.append("https://www.fulloccaz.com" + k.attrs["href"])
                        except:
                            url_list.append(np.nan)
        return url_list

    def get_references_from_url_list(self, url_list_):

        # set empty ref list
        ref_list = []

        for k in url_list_:
            try:
                ref_list.append(k.split("/")[5].split("-")[0])
            except:
                ref_list.append(np.nan)
        #
        return ref_list


    ########################
    #### Announce scraper
    ########################

    def get_url(self):

        # extract url
        step1 = self.announce.select('a[class*="CLFacebook"]')
        if len(step1)>0:
            if "href" in step1[0].attrs:
                return step1[0].attrs["href"].split("=")[-1]
        return np.nan


    def get_uniq_id(self, url_):
        try:
            return url_.split("/")[-1].split("-")[0]
        except:
            return np.nan


    def get_publication_date(self):

        # extract
        step1 = self.announce.select('div[class*="CLInfosAnnonces"]')
        if len(step1)>0:
            step2 = step1[0].select('tr')
            if len(step2) > 0:
                for k in step2:
                    if k.select('td')[0].text.find("Mise à jour") >= 0:
                        try:
                            return k.select('td')[1].text
                        except:
                            return np.nan
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
        step1 = self.announce.select('div[class*="CLInfosAnnonces"]')
        if len(step1)>0:
            step2 = step1[0].select('tr')
            if len(step2) > 0:
                for k in step2:
                    if k.select('td')[0].text.find("Catégorie") >= 0:
                        try:
                            return k.select('td')[1].text
                        except:
                            return np.nan
        else:
            return np.nan


    def get_color(self):

        # extract
        step1 = self.announce.select('div[class*="CLInfosAnnonces"]')
        if len(step1)>0:
            step2 = step1[0].select('tr')
            if len(step2) > 0:
                for k in step2:
                    if k.select('td')[0].text.find("Couleur") >= 0:
                        try:
                            return k.select('td')[1].text
                        except:
                            return np.nan
        else:
            return np.nan


    def get_price(self):

        # extract
        step1 = self.announce.select('div[class*="CLInfosAnnonces"]')
        if len(step1)>0:
            step2 = step1[0].select('tr')
            if len(step2) > 0:
                for k in step2:
                    if k.select('td')[0].text.find("Prix") >= 0:
                        try:
                            return float("".join(re.findall("\d+", k.select('td')[1].text)))
                        except:
                            return np.nan
        else:
            return np.nan


    def get_city(self):

        # extract
        step1 = self.announce.select('div[class*="CLAnnonceConcession"]')
        if len(step1)>0:
            try:
                return step1[0].text.split(re.findall("[0-9]{5}", step1[0].text)[0])[-1].strip()
            except:
                return None
        return None


    def get_postal(self):

        # extract
        step1 = self.announce.select('div[class*="CLAnnonceConcession"]')
        if len(step1)>0:
            try:
                return re.findall("[0-9]{5}", step1[0].text)[0]
            except:
                return None
        return None


    def get_seller(self):

        # extract
        step1 = self.announce.select('div[class*="CLAnnonceConcession"]')
        if len(step1)>0:
            try:
                step2 = step1[0].select("strong")[0].text.split(":")
                return step2[-1].strip()
            except:
                pass
        return np.nan


    def get_seller_name(self):

        # extract
        step1 = self.announce.select('div[class*="CLAnnonceConcession"]')
        if len(step1)>0:
            try:
                step2 = step1[0].select('div[id*="IDadresse"]')
                step3 = step2[0].text.replace("\t", " ").split("\n")
                for k in step3:
                    if k.find("Votre contact") >= 0:
                        return k.split(":")[-1].strip()
            except:
                pass
        return np.nan


    def get_release(self):

        # extract
        step1 = self.announce.select('div[class*="CLInfosAnnonces"]')
        if len(step1)>0:
            step2 = step1[0].select('tr')
            if len(step2) > 0:
                for k in step2:
                    if k.select('td')[0].text.find("Date de mise") >= 0:
                        try:
                            return k.select('td')[1].text
                        except:
                            return np.nan
        else:
            return np.nan


    def get_mileage(self):

        # extract
        step1 = self.announce.select('div[class*="CLInfosAnnonces"]')
        if len(step1)>0:
            step2 = step1[0].select('tr')
            if len(step2) > 0:
                for k in step2:
                    if k.select('td')[0].text.find("Kilom") >= 0:
                        try:
                            return float(re.findall("\d+", k.select('td')[1].text)[0])
                        except:
                            return np.nan
        else:
            return np.nan


    def get_capa(self):

        # extract
        step1 = self.announce.select('div[class*="CLInfosAnnonces"]')
        if len(step1)>0:
            step2 = step1[0].select('tr')
            if len(step2) > 0:
                for k in step2:
                    if k.select('td')[0].text.find("Cylind") >= 0:
                        try:
                            return float(re.findall("\d+", k.select('td')[1].text)[0])
                        except:
                            return np.nan
        else:
            return np.nan


    def get_engine_type(self):

        # extract
        step1 = self.announce.select('div[class*="CLInfosAnnonces"]')
        if len(step1)>0:
            step2 = step1[0].select('tr')
            if len(step2) > 0:
                for k in step2:
                    if k.select('td')[0].text.find("Motori") >= 0:
                        try:
                            return k.select('td')[1].text
                        except:
                            return np.nan
        else:
            return np.nan


    def get_fiscal_power(self):

        # extract
        step1 = self.announce.select('div[class*="CLInfosAnnonces"]')
        if len(step1)>0:
            step2 = step1[0].select('tr')
            if len(step2) > 0:
                for k in step2:
                    if k.select('td')[0].text.find("Puissance") >= 0:
                        try:
                            return k.select('td')[1].text
                        except:
                            return np.nan
        else:
            return np.nan


    def get_guarantee(self):

        # extract
        step1 = self.announce.select('div[class*="CLInfosAnnonces"]')
        if len(step1)>0:
            step2 = step1[0].select('tr')
            if len(step2) > 0:
                for k in step2:
                    if k.select('td')[0].text.find("Garant") >= 0:
                        try:
                            return k.select('td')[1].text.replace("\n", "").replace("\t", "").strip()
                        except:
                            return np.nan
        else:
            return np.nan


    def get_hand(self):

        # extract
        step1 = self.announce.select('div[class*="CLInfosAnnonces"]')
        if len(step1)>0:
            step2 = step1[0].select('tr')
            if len(step2) > 0:
                for k in step2:
                    if k.select('td')[0].text.find("Première") >= 0:
                        try:
                            return "First hand"
                        except:
                            return np.nan
        else:
            return np.nan


    def get_chassis(self):

        # extract
        step1 = self.announce.select('div[class*="CLInfosAnnonces"]')
        if len(step1)>0:
            step2 = step1[0].select('tr')
            if len(step2) > 0:
                for k in step2:
                    if k.select('td')[0].text.find("Chassis") >= 0:
                        try:
                            return k.select('td')[1].text
                        except:
                            return np.nan
        else:
            return np.nan


    def get_comment(self):

        # extract
        step1 = self.announce.select('div[class*="CLAnnonceDescription"]')
        if len(step1)>0:
            try:
                return cleanhtml(step1[0].text.replace("\n", " ").replace("\t", " ").replace("  ", " ").strip())
            except:
                return ""
        return ""



    def get_images(self, uniq_id):

        # get images url list
        image_list = []

        # extract
        step1 = self.announce.select('div[class*="CLSlide-group"]')

        if len(step1)>0:
            step2 = step1[0].select("a")
            if len(step2) > 0:
                for k in step2:
                    if "href" in k.attrs:
                        image_list.append(k.attrs['href'])

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

    print("Nothing.")

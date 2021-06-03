from bs4 import BeautifulSoup
import numpy as np
import requests
import os
import re
from PIL import Image
import PIL

class paruvendu_page_scraper():

    def __init__(self, page=None, announce=None):
        """
            page: 15 announces soup
            announce : single announce soup
        """
        self.page = page
        self.announce = announce


    ########################
    #### Page scraper
    ########################

    def get_prices_from_page(self):

        # set empty price list
        price_list = []

        # extract price
        step1 = self.page.select('div[class*="lazyload_bloc"]')
        if len(step1)>0:
            for k in step1:
                step2 = k.find_all("div", class_="ergov3-priceannonce")
                if len(step2)>0:
                    step3 = re.findall('[0-9]', step2[0].text)
                    if len(step3)>0:
                        price = float("".join(step3))
                        price_list.append(price)
                    else:
                        price_list.append(np.nan)
                else:
                    return None
        else:
            return None

        return price_list


    def get_unique_IDs_from_page(self):

        # set empty uniq_ID list
        uniq_ID_list = []

        # extract uniq ID
        step1 = self.page.select('div[class*="lazyload_bloc"]')
        if len(step1)>0:
            for k in step1:
                if "data-id" in k.attrs:
                    uniq_ID_list.append(k.attrs["data-id"])
                else:
                    uniq_ID_list.append(np.nan)
        else:
            return None

        return uniq_ID_list


    def get_urls_from_pages(self):

        # set empty url list
        url_list = []

        # extract url
        step1 = self.page.select('div[class*="lazyload_bloc"]')
        if len(step1)>0:
            for k in step1:
                step2 = k.find_all("a")
                if len(step2)>2:
                    if "href" in step2[1].attrs:
                        url_list.append(step2[1].attrs["href"])
                    else:
                        url_list.append(np.nan)
                else:
                    return None
        else:
            return None

        return url_list


    def get_titles_from_pages(self):

        # set empty title list
        title_list = []

        # extract title
        step1 = self.page.select('div[class*="lazyload_bloc"]')
        if len(step1)>0:
            for k in step1:
                step2 = k.find_all("h3")
                if len(step2)>0:
                    title_list.append(step2[0].text.replace("\n", "").replace("\t", "").replace("Moto ", "").strip())
                else:
                    title_list.append(np.nan)
        else:
            return None
        #
        return title_list


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


    def get_reference(self):

        # extract ref
        step1 = self.announce.select('div[class*="vvdetails14_refdate"]')
        if len(step1)>0:
            return step1[0].text.replace("\n", "").replace("\t", "").split("ParuVendu")[-1].split("-")[0].strip()
        else:
            return None


    def get_uniq_id(self, url_):
        return url_.split("/")[-1].split("A1")[0]


    def get_publication_date(self):

        # extract
        step1 = self.announce.select('div[class*="vvdetails14_refdate"]')
        if len(step1)>0:
            try:
                return step1[0].text.replace("\n", "").replace("\t", "").split("ParuVendu")[-1].split("-")[1].strip().split(" ")[1]
            except:
                return np.nan
        else:
            return None


    def get_brand(self):

        # extract
        step1 = self.announce.select('div[id*="blcheader"]')
        if len(step1)>0:
            return step1[0].select("h1")[0].text.replace(u'\xa0', u' ').strip().split(" ")[-1]
        else:
            return None


    def get_type(self):

        # extract
        step1 = self.announce.select('div[class*="im12_txt_ann"]')
        if len(step1)>0:
            step2 = step1[0].select('li[class*="nologo"]')
            if len(step2)>0:
                step3 = step2[0].select("span")
                if len(step3)>0:
                    return step3[0].text.replace("\n", "")
        return None


    def get_moto(self):

        # extract
        step1 = self.announce.select('div[id*="blcheader"]')
        if len(step1)>0:
            return step1[0].select("h1")[0].text.replace(u'\xa0', u' ').strip().split(" ")[0]
        else:
            return None


    def get_color(self):

        # extract
        step1 = self.announce.select('div[class*="im12_txt_ann"]')
        if len(step1)>0:
            step2 = step1[0].select('li[class*="puiss"]')
            if len(step2)>0:
                for k in step2:
                    if k.text.find("Couleur")>0:
                        return k.select("span")[0].text.strip()
        return None


    def get_cond(self):

        # extract
        step1 = self.announce.select('div[class*="im12_txt_ann"]')
        if len(step1)>0:
            step2 = step1[0].select('li[class*="puiss"]')
            if len(step2)>0:
                for k in step2:
                    if k.text.find("Etat")>0:
                        return k.select("span")[0].text.strip()
        return None


    def get_power(self):

        # extract
        step1 = self.announce.select('div[class*="im12_txt_ann"]')
        if len(step1)>0:
            step2 = step1[0].select('li[class*="puiss"]')
            if len(step2)>0:
                for k in step2:
                    if k.text.find("fiscale")>0:
                        return int(k.select("span")[0].text.strip())
        return None


    def get_price(self):

        # extract
        step1 = self.announce.select('div[id*="autoprix"]')
        if len(step1)>0:
            try:
                return float("".join(re.findall("[0-9]", step1[0].text)))
            except:
                return np.nan
        return None


    def get_city(self):

        # extract
        step1 = self.announce.select('div[id*="blcheader"]')
        if len(step1)>0:
            return step1[0].select("h2")[0].text.split(" ")[-1].replace("\n", "")
        return None


    def get_postalcode(self):

        # extract
        step1 = self.announce.select('div[id*="blcheader"]')
        if len(step1)>0:
            return step1[0].select("h2")[0].text.split(" ")[0].replace("\n", "")
        return None


    def get_releasedate(self):

        # extract
        step1 = self.announce.select('div[class*="im12_txt_ann"]')
        if len(step1)>0:
            step2 = step1[0].select('li[class*="ann"]')
            if len(step2)>0:
                step3 = step2[0].select("span")
                if len(step3)>0:
                    return "".join(re.findall("[0-9]", step3[0].text))
        return None


    def get_mileage(self):

        # extract
        step1 = self.announce.select('div[class*="im12_txt_ann"]')
        if len(step1)>0:
            step2 = step1[0].select('li[class*="kil"]')
            if len(step2)>0:
                step3 = step2[0].select("span")
                if len(step3)>0:
                    return "".join(re.findall("[0-9]", step3[0].text))
        return None


    def get_capa(self):

        # extract
        step1 = self.announce.select('div[class*="im12_txt_ann"]')
        if len(step1)>0:
            step2 = step1[0].select('li[class*="cyl"]')
            if len(step2)>0:
                step3 = step2[0].select("span")
                if len(step3)>0:
                    return "".join(re.findall("[0-9]", step3[0].text))
        return None


    def get_comment(self):

        # extract
        step1 = self.announce.select('div[class*="im12_txt_ann"]')
        if len(step1)>0:
            step2 = step1[0].select('div[class*="txt_annonceauto"]')
            if len(step2)>0:
                raw_text = step2[0].text.split("Prix")[0].strip()
                #raw_text = unicode(raw_text, errors='replace')
                return raw_text.replace(u'\x80', u' ').strip().replace("\n", " ").replace("\t", " ")
        return None


    def get_seller(self):

        # extract
        step1 = self.announce.select('p[class*="txtpresentation-vendeur"]')
        if len(step1)>0:
            if step1[0].text.find("particulier")>0:
                return ["Particulier", step1[0].text.split(":")[-1].strip().split("\n")[0].strip()]
            else:
                return ["Professionnel", step1[0].text.strip().split("\n")[0]]
        return None


    def get_images(self, uniq_id):

        # get images url list
        image_list = []

        # extract
        step1 = self.announce.find('div',id="listePhotos")

        if step1 != None:
            if len(step1)>0:
                step2 = step1.select("img")
                if len(step2)>0:
                    if "src" in step2[0].attrs:
                        for k in range(len(step2)-1):
                            image_list.append(step2[0].attrs["src"].replace("_1.jpeg", "_" + str(k+1) + ".jpeg"))

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
        scrap = paruvendu_page_scaper(page=soup_)

        for url, uniq_id, price in zip(scrap.get_urls_from_pages(), scrap.get_unique_IDs_from_page(), scrap.get_prices_from_page()):
            print(url, uniq_id, price)

import scrapy
import logging
from scrapy.exceptions import CloseSpider
from datetime import date


class AnnoncesSpider(scrapy.Spider):
    current_page = 1
    name = 'annonces'
    allowed_domains = ['www.motoconcess.com']

    def start_requests(self):
        yield scrapy.Request(url='https://www.motoconcess.com/?page=1', callback=self.parse, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/94.0.4606.81 Safari/537.36 '
        })

    def parse(self, response):
        if response.xpath('normalize-space(//div[@class="display-4 text-center mt-5"]/text())').get() == 'Désolé,':
            print('STOP')
            raise CloseSpider(f'Maximum number of page reached: {self.current_page}')

        for bike in response.xpath("//li/a"):
            bike_link = bike.xpath(".//@href[1]").get()
            yield response.follow(url=bike_link, callback=self.parse_bike, headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/94.0.4606.81 Safari/537.36 '
            })

        # pagination:
        self.current_page = self.current_page + 1
        next_page = f'https://www.motoconcess.com/?page={self.current_page}'
        yield scrapy.Request(url=next_page, callback=self.parse, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/94.0.4606.81 Safari/537.36 '
        })

    def parse_bike(self, response):
        logging.info(response.url)
        today = date.today()

        try:
            price = response.xpath("//th[contains(text(),'Prix')]//following-sibling::td/text()").get()
            price = int(price.replace(' €', '').replace(' ', '').strip())
        except AttributeError:
            pass

        try:
            brand = response.xpath("//th[contains(text(),'Marque')]//following-sibling::td/text()").get()
            brand = brand.lower()
        except AttributeError:
            pass

        try:
            model = response.xpath("//h1/text()").get().lower()
            model = model.replace(brand, '').strip()
        except AttributeError:
            pass

        try:
            category = response.xpath("//th[contains(text(),'Catégorie')]//following-sibling::td/text()").get()
            category = category.lower()
        except AttributeError:
            pass

        try:
            engine_size = response.xpath("//th[contains(text(),'Cylindrée')]//following-sibling::td/text()").get()
            engine_size = int(engine_size.replace(' cm³', '').replace(' ', '').strip())
        except AttributeError:
            pass

        try:
            engine_type = response.xpath("//th[contains(text(),'Motorisation')]//following-sibling::td/text()").get()
            engine_type = engine_type.lower()
        except AttributeError:
            pass

        try:
            circulation_date = response.xpath(
                "//th[contains(text(),'Date de mise en circulation')]//following-sibling::td/text()").get()
            circulation_year = response.xpath(
                "//th[contains(text(),'Année du modèle')]//following-sibling::td/text()").get()
            circulation_year = int(circulation_year)
        except AttributeError:
            pass

        try:
            warranty = response.xpath("//th[contains(text(),'Garantie')]//following-sibling::td/text()").get()
            if warranty == "Jusqu'au ":
                raise AttributeError
        except AttributeError:
            pass

        try:
            mileage = response.xpath("//th[contains(text(),'Kilomètres')]//following-sibling::td/text()").get()
            mileage = int(mileage.replace(' km', '').replace(' ', '').strip())
        except AttributeError:
            pass

        try:
            first_hand = response.xpath("//th[contains(text(),'Première main')]//following-sibling::td/text()").get()
            if first_hand == 'Non':
                first_hand = False
            elif first_hand == 'Oui':
                first_hand = True
        except AttributeError:
            pass

        try:
            annonce_date = response.xpath(
                "//th[contains(text(),'Mise à jour annonce')]//following-sibling::td/text()").get()
        except AttributeError:
            pass

        try:
            comment = response.xpath("normalize-space(//div[contains(@class,'descriptif')]/p/text())").getall()
            comment = ''.join([word for word in comment])
        except AttributeError:
            pass

        try:
            id_annonce = response.xpath('//div[@class="form-width-photo"]/form/input[@name="Annonce"]/@value').get()
            id_annonce = int(id_annonce)
        except AttributeError:
            pass

        yield {
            'price': price,
            'brand': brand,
            'model': model,
            'category': category,
            'engine_size': engine_size,
            'engine_type': engine_type,
            'circulation_date': circulation_date,
            'circulation_year': circulation_year,
            'warranty': warranty,
            'mileage': mileage,
            'first_hand': first_hand,
            'annonce_date': annonce_date,
            'vendor_type': 'pro',
            'source': 'montoconcess',
            'url': response.url,
            'id': id_annonce,
            'comment': comment,
            'scraped_date': today.strftime("%d/%m/%Y")
        }
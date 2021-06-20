from data.moto_occasion.scraping import scraping as scraping_moto_occasion
from data.moto_selection.scraping import scraping as scraping_moto_selection
from data.motomag.scraping import scraping as scraping_motomag
#from data.paruvendu.scraping import scraping as scraping_paruvendu


def scraper():
    scraping_moto_occasion()
    scraping_moto_selection()
    scraping_motomag()
    #scraping_paruvendu()


if __name__ == '__main__':
    scraper()

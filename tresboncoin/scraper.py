from data.moto_occasion.scraping import scraping as scraping_moto_occasion
from data.moto_selection.scraping import scraping as scraping_moto_selection
from data.motomag.scraping import scraping as scraping_motomag
from data.motovente.scraping import scraping as scraping_motovente
from data.motomag.scraping import scraping as scraping_motomag
#from data.paruvendu.scraping import scraping as scraping_paruvendu
from termcolor import colored


def scraper():
    print(colored("moto-occasion scraping started", "blue"))
    scraping_moto_occasion()
    print(colored("moto-occasion scraping completed", "green"))

    print(colored("moto-selection scraping started", "blue"))
    scraping_moto_selection()
    print(colored("moto-selection scraping completed", "green"))

    print(colored("motovente scraping started", "blue"))
    scraping_motovente()
    print(colored("motovente scraping completed", "green"))

    print(colored("motomag scraping started", "blue"))
    scraping_motomag()
    print(colored("motomag scraping completed", "green"))

    # scraping_paruvendu()


if __name__ == '__main__':
    scraper()

from tresboncoin.data.motomag.scraping_pages import scraping_pages
from tresboncoin.data.motomag.scraping_annonces import scraping_annonces


def scraping():
    '''
    function to:
    1 - scraping pages
    2 - scrapping and saving new annonces
    '''

    scraping_pages()
    scraping_annonces()


if __name__ == "__main__":
    scraping()

from data.paruvendu.scraping_pages import scraping_pages
from data.paruvendu.scraping_annonces import scraping_annonces
from data.paruvendu.scraping_to_dataframe import scraping_to_dataframe


def scraping():
    '''
    function to:
    1 - scraping pages
    2 - scrapping annonces
    3 - tranform to dataframe
    '''

    scraping_pages()
    scraping_annonces()
    scraping_to_dataframe()


if __name__ == "__main__":
    scraping()

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import logging
import pymongo

from itemadapter import ItemAdapter


class MongodbPipeline(object):
    collection_name = 'motoconcess'

    def __init__(self, mongo_uri, mongo_db):
        """
        __init__ method to construct our pipeline object from MongoDB settings, these variables are taken from the
        settings.py file. You should edit this file with the right values for MONGO_DB, which is the name of the
        database you want connect to, and MONGO_URI, which is the Uri for establishing a connection to mongo,
        it differs depending on if you're using a local or cloud based mongo, I used mongoDB Atlas.
        """
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        """
        called to create our pipeline instance from the crawler.
        It accesses the variables in our settings file and returns a new instance of the pipeline.
        """
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'items')
        )

    def open_spider(self, spider):
        """
        open_spider and close_spider are called when our spider opens and closes. They are called only once,
        and here we use it to connect and disconnect from our mongoDB database using pymongo.MongoClient(). I also
        clean out any existing documents from the movie_sessions collection with deleteMany({}) , so it updates all
        sessions every time the spider runs.
        """
        self.client = pymongo.MongoClient(self.mongo_uri, ssl=True)
        self.db = self.client[self.mongo_db]
        self.db[self.collection_name].delete_many({})
        self.db['movies'].delete_many({})

    def close_spider(self, spider):
        """
        open_spider and close_spider are called when our spider opens and closes. They are called only once,
        and here we use it to connect and disconnect from our mongoDB database using pymongo.MongoClient(). I also
        clean out any existing documents from the movie_sessions collection with deleteMany({}) , so it updates all
        sessions every time the spider runs.
        """
        self.client.close()

    def process_item(self, item, spider):
        """
        process_item is what will be called for every item that enters this pipeline. So here is where we use pymongo
        to save our item on movie_sessions collection as a python dictionary, using .insertOne(dict(item)). Notice I
        had to add an if statement to check if the item was already in my collection (somehow it was saving
        duplicates). This method must return the item to send it to other pipelines or raise a DropItem exception to
        exclude it from other processes.
        """
        count = self.db[self.collection_name].count({
            "price": item["price"],
            "brand": item["brand"],
            "model": item["model"],
            "mileage": item["mileage"],
            "circulation_year": item["circulation_year"]
        })
        if count == 0:
            self.db[self.collection_name].insert_one(dict(item))
        return item

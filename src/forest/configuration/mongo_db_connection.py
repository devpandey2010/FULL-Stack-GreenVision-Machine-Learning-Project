import sys
import os
from src.forest.exception import CustomException
from src.forest.logger import logging
from src.forest.constant.database import DATABASE_NAME
import pymongo
import certifi 
from dotenv import load_dotenv

load_dotenv()

ca=certifi.where()
class MongoDBClient:
    client=None
    def __init__(self,database_name:str=DATABASE_NAME)->None:
        try:
            if MongoDBClient.client is None:
                mongo_db_url=os.getenv("MONGO_DB_URL")
                if mongo_db_url is None:
                    raise Exception("Enviornment key:MONGO_DB_URL is not set")
                MongoDBClient.client=pymongo.MongoClient(mongo_db_url,tlsCAFile=ca)
            self.client=MongoDBClient.client
            self.database=self.client[database_name]
            self.database_name=database_name
        except Exception as e:
            raise CustomException(e,sys) from e
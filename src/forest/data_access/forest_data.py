import os
import sys
import pymongo
import numpy as np
import pandas as pd
from typing import Optional
from src.forest.logger import logging
from src.forest.utils import main_utils
from src.forest.exception import CustomException
from src.forest.configuration.mongo_db_connection import MongoDBClient
from src.forest.constant.database import DATABASE_NAME


class ForestData:
    def __init__(self):
        try:
             # Initialize MongoDB client using custom class
            self.mongo_client=MongoDBClient(database_name=DATABASE_NAME)
        except Exception as e:
            raise CustomException(e,sys)
        
    
    def export_collection_as_dataframe(self,collection_name:str,database_name:Optional[str]=None)->pd.DataFrame:
        try:
            # If database name is provided use it, otherwise use the default one
            if database_name is None:
                collection=self.mongo_client.database[collection_name]

            else:
                collection=self.mongo_client.client[database_name][collection_name]
                  # Convert MongoDB collection to DataFrame
            df=pd.DataFrame(list(collection.find()))
            # Drop MongoDB internal ID column
            if "_id" in df.columns.to_list():
                df=df.drop(columns="_id",axis=1)
                # Replace string "na" with actual NaN
            df.replace({"na":np.nan},inplace=True)
            return df
        except Exception as e:
            raise CustomException(e,sys)
            


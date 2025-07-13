import os 
import sys
import shutil
from zipfile import ZipFile
from sklearn.model_selection import train_test_split
import numpy as np
from pandas import DataFrame
from src.forest.entity.config_entity import DataIngestionConfig
from src.forest.exception import CustomException
from src.forest.logger import logging
from src.forest.utils.main_utils import MainUtils
from src.forest.configuration.aws_connection import S3Client
from src.forest.configuration.mongo_db_connection import MongoDBClient
from src.forest.constant.training_pipeline import SCHEMA_FILE_PATH
from src.forest.constant.database import COLLECTION_NAME,DATABASE_NAME
from src.forest.entity.artifact_entity import DataIngestionArtifact
from src.forest.data_access.forest_data import ForestData

class DataIngestion:

    def __init__(self,data_ingestion_config:DataIngestionConfig=DataIngestionConfig()):
        try:
            self.data_ingestion_config=data_ingestion_config
        except Exception as e:
            raise CustomException(e,sys)
        
    def export_data_into_feature_store(self)->DataFrame:

        '''This function will export the data from MongoDB collect the data as DataFrame and then store the data as Forest.csv
        This we can see in the Project Flow'''

        try:
            logging.info("Exporting Data from MongoDB")
            sensor_data=ForestData()
            dataframe=sensor_data.export_collection_as_dataframe(collection_name=self.data_ingestion_config.collection_name)

            logging.info(f"shape of dataframe:{dataframe.shape}")
            feature_store_file_path=self.data_ingestion_config.feature_store_file_path
            dir_path=os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path,exist_ok=True)
            logging.info(f"saving exported data into feature store file path:{feature_store_file_path}")
            dataframe.to_csv(feature_store_file_path,index=False,header=True)
            return dataframe

        except Exception as e:
            raise CustomException(e,sys)
        
    def split_data_as_train_test(self,dataframe:DataFrame)->None:
        '''In the project Flow we can  see that the Forest.csv is divided into train.csv and test.csv
        This function will split the data into train and test then save as train.csv and test.csv'''

        logging.info("Entered the train and test split block of DataIngeston")

        try:
            train_set,test_set=train_test_split(dataframe,test_size=self.data_ingestion_config.train_test_split_ratio,random_state=42)
            logging.info("Performed train test split on dataframe")

            logging.info("Exited split_data_as_train_test method of Data_Ingestion class")
            dir_path=os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path,exist_ok=True)

            logging.info("Exporting train and test file path")
            train_set.to_csv(self.data_ingestion_config.training_file_path,index=False,header=True)
            test_set.to_csv(self.data_ingestion_config.testing_file_path,index=False,header=True)

            logging.info("Exported train and test file path")

        except Exception as e:
            raise CustomException(e,sys) from e
        
    def initiate_data_ingestion(self)->DataIngestionArtifact:
        ''' 
        Method Name :   initiate_data_ingestion
        Description :   This method initiates the data ingestion components of training pipeline 
        
        Output      :   train set and test set are returned as the artifacts of data ingestion components
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        '''
        logging.info("Entered initiate_data_ingestion method of Data_Ingestion class")

        try:
            dataframe = self.export_data_into_feature_store()
            _schema_config = MainUtils.read_yaml_file(file_path=SCHEMA_FILE_PATH)

            dataframe = dataframe.drop(_schema_config["drop_columns"], axis=1,errors='ignore')

            logging.info("Got the data from mongodb")

            self.split_data_as_train_test(dataframe)

            logging.info("Performed train test split on the dataset")

            logging.info(
                "Exited initiate_data_ingestion method of Data_Ingestion class"
            )

            data_ingestion_artifact = DataIngestionArtifact(trained_file_path=self.data_ingestion_config.training_file_path,
                                                            test_file_path=self.data_ingestion_config.testing_file_path)
            
            logging.info(f"Data ingestion artifact: {data_ingestion_artifact}")
            return data_ingestion_artifact
        except Exception as e:
            raise CustomException(e, sys) from e

        







        





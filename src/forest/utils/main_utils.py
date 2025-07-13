import os.path
import sys
import numpy as np
import dill #similar to pickle
import pymongo
import yaml
import pickle
import boto3

from src.forest.exception import CustomException
from src.forest.logger import logging

class MainUtils:
    def __init__(self)->None:
        pass

    def read_yaml_file(file_path:str)->dict:
        try:
            with open(file_path,"rb") as yaml_file:
                return yaml.safe_load(yaml_file)
            
        except Exception as e:
            raise CustomException(e,sys) from e
        
    def write_yaml_file(file_path:str,content:object,replace:bool=False)->None:
        try:
            if replace:
                if os.path.exists(file_path):
                    os.remove(file_path)
            os.makedirs(os.path.dirname(file_path),exist_ok=True)
            with open(file_path,'w') as file:
                yaml.dump(content,file)

        except Exception as e:
            raise CustomException(e,sys)

    @staticmethod  
    def load_object(file_path:str)->object:
        logging.info("Entered the load object method of main Utils class")

        try:
            with open(file_path,"rb") as file_obj:
                obj=dill.load(file_obj)

            logging.info("Exited the load_object of Main Utils Class")
            return obj
        
        except Exception as e:
            raise CustomException(e,sys) from e
        
    @staticmethod
    def save_object(file_path:str,obj:object)->None:
        logging.info("Entered the save_object of maon utils Class")

        try:
            os.makedirs(os.path.dirname(file_path),exist_ok=True)
            with open(file_path,'wb') as file_obj:
                dill.dump(obj,file_obj)
            
            logging.info("Exited the save_object method of main utils class")

        except Exception as e:
            raise CustomException(e,sys) from e
        
    @staticmethod
    def upload_data(from_filename,to_filename,bucket_name):
        # 'from_filename': local file ka naam jo upload karna hai
        # 'bucket_name': S3 bucket ka naam jisme upload karna hai
        # 'to_filename': S3 mein file ka naam jisse upload hogi
        try:
            s3_resource=boto3.resource('s3')
            s3_resource.meta.client.upload_file(from_filename,bucket_name,to_filename)

        except Exception as e:
            raise CustomException(e,sys) from e

    @staticmethod
    def download_model(bucket_name,bucket_file_name,dest_file_name):
        try:
            s3_client=boto3.client("s3")
            s3_client.download_file(bucket_name,bucket_file_name,dest_file_name)
            return dest_file_name
        except Exception as e:
            raise CustomException(e,sys) from e

        
    @staticmethod
    def save_numpy_array_data(file_path:str,array:np.array):
        try:
            dir_path=os.path.dirname(file_path)
            os.makedirs(dir_path,exist_ok=True)
            with open(file_path,"wb") as file_obj:
                np.save(file_obj,array)
        
        except Exception as e:
            raise CustomException(e,sys) from e
        
    @staticmethod
    def load_numpy_array_data(file_path:str)->np.array:
        try:
            with open(file_path,"rb") as file_obj:
                return np.load(file_obj,allow_pickle=True)
            
        except Exception as e:
            raise CustomException(e,sys) from e
        
    def create_directories(path_to_directories:list,verbose=True):
        for path in path_to_directories:
            os.makedirs(path,exist_ok=True)
            if verbose:
                logging.info(f"Created Directory at:{path}")
        
    
        
    
        
    



import sys
from src.forest.cloud_storage.aws_storage import SimpleStorageService
from src.forest.exception import CustomException
from src.forest.entity.estimator import SensorModel
from pandas import DataFrame
from dotenv import load_dotenv

load_dotenv()

class SensorEstimator:
    '''
    This class is used to save and retrieve sensor model in s3 bucket and to do predictions

    '''

    def __init__(self,bucket_name,model_path,):
        """
        param bucket_name: Name of your model bucket
        param model_path: Location of your model in bucket
        """
        self.bucket_name=bucket_name
        self.s3=SimpleStorageService()
        self.model_path=model_path
        self.loaded_model:SensorModel=None


    def is_model_present(self,model_path):

        try:
            return self.s3.s3_key_path_available(bucket_name=self.bucket_name,s3_key=model_path)
        except CustomException as e:
            print(e)
            return False
        
    
    def load_model(self)->SensorModel:
        '''
        Load the model from model_path
        return 
        '''
        return self.s3.load_model(self.model_path,bucket_name=self.bucket_name)
    
    def save_model(self,from_file,remove:bool=False)->None:
        '''
        Save the model to model_path
        from file:Your local system model path
        remove:By default it is false that means you will have you model locally available in your system folder

        '''
        try:
            self.s3.upload_file(from_file,to_filename=self.model_path,
                                bucket_name=self.bucket_name,
                                remove=remove)
            
        except Exception as e:
            raise CustomException(e,sys)
        
    
    def predict(self,dataframe:DataFrame):
        '''
        param:Dataframe
        return
        '''
        try:
            if self.loaded_model is None:
                self.loaded_model=self.load_model()

            return self.loaded_model.predict(dataframe=dataframe)
        except Exception as e:
            raise CustomException(e,sys)
    
    
import sys
from pandas import DataFrame
from sklearn.pipeline import Pipeline
from src.forest.exception import CustomException
from src.forest.logger import logging
from dataclasses import dataclass

class TargetValueMapping:
    def __init__(self):
        self.neg:int=0
        self.pos:int=1

    def _asdict(self):
        return self.__dict__
    def reverse_mapping(self):
        mapping_response=self._asdict()
        return dict(zip(mapping_response.values(),mapping_response.keys()))
    
class SensorModel:
    def __init__(self,preprocessing_object:Pipeline,trained_model_object:object):
        self.preprocessing_object=preprocessing_object
        self.trained_model_object=trained_model_object

    def predict(self,dataframe:DataFrame)->DataFrame:
        logging.info("Enterd predict method of SensorModel class")
        try:
            logging.info("Using the trained model to get predictions")
            transformed_feature=self.preprocessing_object.transform(dataframe)
            logging.info("Used the trained model to get predictions")
            return self.trained_model_object.predict(transformed_feature)
        except Exception as e:
            raise CustomException(e,sys) from e
        
    def __repr__(self):
        return f"{type(self.trained_model_object).__name__}()"
    def __str__(self):
        return f"{type(self.trained_model_object).__name__}()"
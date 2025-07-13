import os,sys
import numpy as np
import pandas as pd
from pandas import DataFrame
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from src.forest.constant import *
from src.forest.exception import CustomException
from src.forest.logger import logging
from src.forest.utils.main_utils import MainUtils
from sklearn.compose import ColumnTransformer
from src.forest.constant.training_pipeline import TARGET_COLUMN,SCHEMA_FILE_PATH
from src.forest.entity.config_entity import DataTransformationConfig
from src.forest.entity.artifact_entity import DataIngestionArtifact,DataTransformationArtifact

class DataTransformation:

    def __init__(self,data_ingestion_artifact:DataIngestionArtifact,data_transformation_config:DataTransformationConfig):
        self.data_ingestion_artifact=data_ingestion_artifact
        self.data_transformation_config=data_transformation_config

    @staticmethod
    def read_data(file_path)->DataFrame:

        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise CustomException(e,sys)
        
    def get_data_transformer_object(self)->object:
        logging.info("Got numerical,categorical,transformation columns from schema cofig")
        try:

            _schema_config=MainUtils.read_yaml_file(file_path=SCHEMA_FILE_PATH)
        
            num_features=_schema_config["numerical_columns"]
            categorical_features=_schema_config["categorical_columns"]

            numerical_pipeline=Pipeline(steps=[('imputer',SimpleImputer(strategy='median')),
                                          ('scaler',StandardScaler())])
        
            preprocessor=ColumnTransformer([
            ("Numeric_Pipeline",numerical_pipeline,num_features)

            ])
            logging.info("Created Preprocessor object from columnTransformer")
            logging.info("Exited")

            return preprocessor
        except Exception as e:
            raise CustomException(e,sys) from e
        
    def initiate_data_transformation(self)->DataTransformationArtifact:

        '''
         Method Name :   initiate_data_transformation
        Description :   This method initiates the data transformation component for the pipeline 
        
        Output      :   data transformer object is created and returned 
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        '''

        logging.info("Entered initiate_data_transformation method of DataTranformation class")

        try:
            logging.info("Starting data transformation")
            preprocessor=self.get_data_transformer_object()
            logging.info("Got the preprocessor object")

            train_df=DataTransformation.read_data(file_path=self.data_ingestion_artifact.trained_file_path)
            test_df=DataTransformation.read_data(file_path=self.data_ingestion_artifact.test_file_path)

            input_feature_train_df=train_df.drop(columns=[TARGET_COLUMN],axis=1)
            target_feature_train_df=train_df[TARGET_COLUMN]

            logging.info("Got train features and test features of training dataset")

            input_feature_test_df=test_df.drop(columns=[TARGET_COLUMN],axis=1)
            target_feature_test_df=test_df[TARGET_COLUMN]

            logging.info("Got train features and test features of testing dataset")
            logging.info(
                "Applying preprocessing object on training dataframe and testing dataframe"
            )

            input_feature_train_arr=preprocessor.fit_transform(input_feature_train_df)

            logging.info("Used the preprocessor object to fit transform the train features")

            input_feature_test_arr=preprocessor.transform(input_feature_test_df)

            logging.info("Used the preprocessor object to tranform the test features")

            train_arr=np.c_[input_feature_train_arr,np.array(target_feature_train_df)]
            test_arr=np.c_[input_feature_test_arr,np.array(target_feature_test_df)]

            MainUtils.save_object(self.data_transformation_config.transformed_object_file_path,preprocessor)
            MainUtils.save_numpy_array_data(self.data_transformation_config.transformed_train_file_path, array=train_arr)
            MainUtils.save_numpy_array_data(self.data_transformation_config.transformed_test_file_path, array=test_arr)

            logging.info("Saved the preprocessor object")
            logging.info("Exited initiate_data_transformation method of DataTransformation class")

            data_transformation_artifact = DataTransformationArtifact(
                transformed_object_file_path=self.data_transformation_config.transformed_object_file_path,
                transformed_train_file_path=self.data_transformation_config.transformed_train_file_path,
                transformed_test_file_path=self.data_transformation_config.transformed_test_file_path
            )

            return data_transformation_artifact
        except Exception as e:
            raise CustomException(e,sys) from e
        


        




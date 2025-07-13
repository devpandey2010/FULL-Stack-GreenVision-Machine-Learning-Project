import sys
import pandas as pd
from pandas import DataFrame
from src.forest.exception import CustomException
from src.forest.logger import logging
from src.forest.utils.main_utils import MainUtils
from src.forest.constant.training_pipeline import SCHEMA_FILE_PATH
from src.forest.entity.artifact_entity import DataValidationArtifact,DataIngestionArtifact
from src.forest.entity.config_entity import DataValidationConfig

class DataValidation:
    def __init__(self,data_ingestion_artifact:DataIngestionArtifact,data_validation_config:DataValidationConfig):
        self.data_ingestion_artifact=data_ingestion_artifact
        self.data_validation_config=data_validation_config
        self._schema_config=MainUtils.read_yaml_file(file_path=SCHEMA_FILE_PATH)


    def validate_number_of_columns(self,dataframe:DataFrame)-> bool:
        try:
            status=len(dataframe.columns)==len(self._schema_config["columns"])
            logging.info(f"Is required column present:{status}")
            return status
        except Exception as e:
            raise CustomException(e,sys)
        
    
    @staticmethod
    def read_data(file_path)->DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise CustomException(e,sys)
        
    def is_numerical_column_exists(self,df:DataFrame)->bool:
        try:
            dataframe_columns=df.columns
            status=True
            missing_numerical_columns=[]
            for column in self._schema_config["numerical_columns"]:
                if column not in dataframe_columns:
                    status=False
                    missing_numerical_columns.append(column)
            logging.info("Missing numerical column:{missing_numerical_columns}")
            return status
        except Exception as e:
            raise CustomException(e,sys) from e
        
    def initiate_data_validation(self) -> bool:
        """
        Method Name :   initiate_data_validation
        Description :   This method initiates the data validation component for the pipeline
        
        Output      :   Returns bool value based on validation results
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        logging.info("Entered initiate_data_validation method of Data_Validation class")
        try:

            validation_error_msg = ""
            logging.info("Starting data validation")
            train_df, test_df = (DataValidation.read_data(file_path=self.data_ingestion_artifact.trained_file_path),
                                 DataValidation.read_data(file_path=self.data_ingestion_artifact.test_file_path))

            status = self.validate_number_of_columns(dataframe=train_df)
            if not status:
                validation_error_msg += f"Columns are missing in training dataframe."
            
            status = self.validate_number_of_columns(dataframe=test_df)

            logging.info(f"All required columns present in testing dataframe: {status}")
            if not status:
                validation_error_msg += f"Columns are missing in test dataframe."

            status = self.is_numerical_column_exists(df=train_df)

            if not status:
                validation_error_msg += f"Numerical columns are missing in training dataframe."

            status = self.is_numerical_column_exists(df=test_df)

            if not status:
                validation_error_msg += f"Numerical columns are missing in test dataframe."
            
            validation_status = len(validation_error_msg) == 0
            if validation_status:...
                # drift_status = self.detect_dataset_drift(train_df, test_df)
                # if drift_status:
                #     logging.info(f"Drift detected.")
            else:
                logging.info(f"Validation_error: {validation_error_msg}")
                
                data_validation_artifact = DataValidationArtifact(
                validation_status=validation_status,
                valid_train_file_path=self.data_ingestion_artifact.trained_file_path,
                valid_test_file_path=self.data_ingestion_artifact.test_file_path,
                invalid_train_file_path=self.data_validation_config.invalid_train_file_path,
                invalid_test_file_path=self.data_validation_config.invalid_test_file_path,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )
            
                logging.info(f"Data validation artifact: {data_validation_artifact}")
                return data_validation_artifact

        except Exception as e:
            raise CustomException(e, sys) from e



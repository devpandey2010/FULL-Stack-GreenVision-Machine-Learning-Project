import os
import sys
import pandas as pd
from src.forest.exception import CustomException
from src.forest.logger import logging
from src.forest.utils.main_utils import MainUtils
from src.forest.entity.config_entity import ModelEvaluationConfig
from src.forest.entity.artifact_entity import ModelTrainerArtifact,DataIngestionArtifact,ModelEvaluationArtifact
from sklearn.metrics import f1_score,accuracy_score
from src.forest.constant.training_pipeline import TARGET_COLUMN
from src.forest.entity.s3_estimator import SensorEstimator
from typing import Optional
from dataclasses import dataclass

@dataclass
class EvaluateModelResponse:

    trained_model_f1_score:float
    best_model_f1_score:float
    is_model_accepted:bool
    difference:float


class ModelEvaluation:

    def __init__(self,model_eval_config:ModelEvaluationConfig,
                 data_ingestion_artifact:DataIngestionArtifact,
                 model_trainer_artifact:ModelTrainerArtifact):
        try:
            self.model_eval_config=model_eval_config
            self.data_ingestion_artifact=data_ingestion_artifact
            self.model_trainer_artifact=model_trainer_artifact

        except Exception as e:
            raise CustomException(e,sys) from e
        
    
    def get_best_model(self)->Optional[SensorEstimator]:
        try:
            bucket_name=self.model_eval_config.bucket_name
            model_path=self.model_eval_config.s3_model_key_path
            sensor_estimator=SensorEstimator(bucket_name=bucket_name,
                                             model_path=model_path)
            
            if sensor_estimator.is_model_present(model_path=model_path):
                return sensor_estimator
            return None
        except Exception as e:
            raise CustomException(e,sys)
        
    def evaluate_model(self)->EvaluateModelResponse:
        try:
            test_df=pd.read_csv(self.data_ingestion_artifact.test_file_path)
            x,y=test_df.drop(TARGET_COLUMN,axis=1),test_df[TARGET_COLUMN]
            trained_model=MainUtils.load_object(file_path=self.model_trainer_artifact.trained_model_file_path)
            y_pred_trained_model=trained_model.predict(x)

            trained_model_f1_score=f1_score(y,y_pred_trained_model,average='micro')
            best_model_f1_score=None
            best_model=self.get_best_model()
            if best_model is not None:
                y_pred_best_model=best_model.predict(x)
                best_model_f1_score=f1_score(y,y_pred_best_model,average='micro')

            # calucate how much percentage training model accuracy is increased/decreased
            tmp_best_model_score = 0 if best_model_f1_score is None else best_model_f1_score
            result = EvaluateModelResponse(trained_model_f1_score=trained_model_f1_score,
                                           best_model_f1_score=best_model_f1_score,
                                           is_model_accepted=trained_model_f1_score > tmp_best_model_score,
                                           difference=trained_model_f1_score - tmp_best_model_score
                                           )
            logging.info(f"Result: {result}")
            return result
        except Exception as e:
            raise CustomException(e,sys)
        
    
    def initiate_model_evaluation(self)->ModelEvaluationArtifact:

        try:
            evaluate_model_response = self.evaluate_model()
            model_evaluation_artifact = ModelEvaluationArtifact(
                is_model_accepted=evaluate_model_response.is_model_accepted,
                best_model_path=self.model_trainer_artifact.trained_model_file_path,
                trained_model_path=self.model_trainer_artifact.trained_model_file_path,
                changed_accuracy=evaluate_model_response.difference)

            logging.info(f"Model evaluation artifact: {model_evaluation_artifact}")
            return model_evaluation_artifact
        except Exception as e:
            raise CustomException(e,sys)

            





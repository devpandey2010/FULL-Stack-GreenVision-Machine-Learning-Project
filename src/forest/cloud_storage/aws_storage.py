from src.forest.logger import logging
import boto3
from src.forest.configuration.aws_connection import S3Client
from io import StringIO
from typing import Union,List
import os,sys
from mypy_boto3_s3.service_resource import Bucket
from src.forest.exception import CustomException
from botocore.exceptions import ClientError
from pandas import DataFrame,read_csv
import pickle
from logging import exception

class SimpleStorageService:

    def __init__(self):
        s3_client=S3Client()
        self.s3_resource=s3_client.s3_resource
        self.s3_client=s3_client.s3_client

    def s3_key_path_available(self,bucket_name,s3_key)->bool:
        try:
            bucket=self.get_bucket(bucket_name)
            file_objects=[file_object for file_object in bucket.objects.filter(Prefix=s3_key)]
            if len(file_objects)>0:
                return True
            else:
                return False
        except Exception as e:
            raise CustomException(e,sys)
        
    @staticmethod
    def read_object(object_name:object,decode:bool=True,make_readable:bool=False)->Union[StringIO,str]:
        """
        Method Name :   read_object
        Description :   This method reads the object_name object with kwargs

        Output      :   The column name is renamed
        On Failure  :   Write an exception log and then raise an exception

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        logging.info("Entered the read_object method of S3Operations class")

        try:
            #check if object_name is None
            if object_name is None:
                raise CustomException("Object is None.Cannot read from None object.",sys)
            
            #check if object_name is a list(multiple objects found)
            if isinstance(object_name,list):
                #use the first object in the list if it's not empty
                if len(object_name)>0:
                    object_name=object_name[0]
                else:
                    raise CustomException("Empty list of objects provided",sys)
            
            #check if object has get method
            if not hasattr(object_name,'get'):
                raise CustomException(f"Object of type {type(object_name)} does not have 'get' method",sys)
            
            #Get the object content
            response=object_name.get()
            if not response or 'Body' not in response:
                raise CustomException("Invalid response from S# object get() method",sys)
            
            #Read the content
            body=response["Body"]
            content=body.read()

            # Process the content based on parameters
            if decode:
                content = content.decode()

            if make_readable:
                content = StringIO(content)

            logging.info("Exited the read_object method of S3Operations class")
            return content

        except Exception as e:
            logging.error(f"Error in read_object: {str(e)}")
            raise CustomException(e, sys) from e
        
    
    def get_bucket(self,bucket_name:str)->Bucket:

        logging.info("Enteres the get bucket of S3operations class")
        try:
            bucket=self.s3_resource.Bucket(bucket_name)
            logging.info("Exited the get_bucket method of S3Opretaions class")
            return bucket
        except Exception as e:
            raise CustomException(e,sys) from e
        
    
    def get_file_object(self,filename:str,bucket_name:str)->Union[List[object],object]:
        '''if you want to download the file from the S3
        or if you want to read the file_content into memory'''
        logging.info(f"Entered the get_file_object method of S3Operations class for file:{filename} in bucket:{bucket_name}")
        try:
            bucket=self.get_bucket(bucket_name)

            #check if bucket exists
            if bucket is None:
                raise CustomException(f"Bucket{bucket_name} not found",sys)
            
            #Get objects with given prefix
            file_objects=list(bucket.objects.filter(Prefix=filename))

            #check if any objects were found
            if len(file_objects)==0:
                logging.warning(f"No objects found with prefix{filename}in bucket{bucket_name}")
                raise CustomException(f"No objects found withy prefix {filename} in bucket {bucket_name}",sys)
            
            #Return single object if only one found,otherwise return the list
            file_objs=file_objects[0] if len(file_objects)==1 else file_objects
            logging.info(f"Found {len(file_objects)}objects with prefix {filename}")
            logging.info("Exited the get_file_object method of S3Operations class")

            return file_objs
        except Exception as e:
            logging.error(f"Error in get_file_object:{str(e)}")
            raise CustomException(e,sys) from e
        
    
    def load_model(self,model_name:str,bucket_name:str,model_dir:str=None)->object:

        '''It will return a deseralized model object which can be used for preiction and model_dir is
        optional if we have save the model in S3 in a directory then it will give that directory'''

        logging.info("Entered the load_model method of S3Operations class")
        try:
            # Create S3 file path
            model_file = model_name if model_dir is None else f"{model_dir}/{model_name}"

            #model file from the bucket name
            file_object=self.get_file_object(model_file,bucket_name) 

            # Read the file object into bytes
            model_obj=self.read_object(file_object,decode=False)

             # Deserialize into model
            model=pickle.loads(model_obj)

            logging.info("Exited the load_model method of S3Operations class")
            return model
        except Exception as e:
            raise CustomException(e,sys) from e
        
    
    def create_folder(self,folder_name:str,bucket_name:str)->None:
        '''Purpose:It creates the folder in s3 bucket'''
        logging.info("Entered the create_folder method of S3Opeartions class")
        try:
            self.s3_resource.Object(bucket_name,folder_name).load()

        except ClientError as e:
            if e.response["Error"]["Code"]=="404":
                folder_obj=folder_name+"/"
                self.s3_client.put_object(Bucket=bucket_name,Key=folder_obj)
            else:
                pass
            logging.info("Exited the create_folder method of S3Opeartions class")

    
    def upload_file(self,from_filename:str,to_filename:str,bucket_name:str,remove:bool=True):

        """This method uploads a file from your local system to an AWS S3 bucket.
           If remove=True, it deletes the local file after a successful upload"""

        logging.info("Entered the upload_file method of S3Operations class")
        try:
            logging.info(f"Uploading{from_filename} file to {to_filename} file in {bucket_name} bucket")

            self.s3_resource.meta.client.upload_file(from_filename,bucket_name,to_filename)
            logging.info(f"Uploaded {from_filename} file to {to_filename} file in {bucket_name} bucket")

            if remove is True:
                os.remove(from_filename)
                
                logging.info(f"Remove is set to {remove},deletd the file")
            else:
                logging.info(f"Remove is set to {remove},not deletd the file")
            
            logging.info("Exited the upload_file method of S3Opeartions class")

        except Exception as e:
            raise CustomException(e,sys) from e
        

    def upload_df_as_csv(self,data_frame:DataFrame,local_filename:str,bucket_filename:str,bucket_name:str,)->None:
        
        """This Function will convert the pandas DataFrame to csv File and the upload the csv file to S3bucket"""
        logging.info("Entered the upload_df_as_csv method of S3Opeartions class")

        try:
            data_frame.to_csv(local_filename,index=None,header=True)

            self.upload_file(local_filename,bucket_filename,bucket_name)
            logging.info("Exited the upload_df_as_csv method of S3Opeartions class")

        except Exception as e:
            raise CustomException(e,sys) from e
        
        
    def get_df_from_object(self,object_:object)->DataFrame:

        '''This method read a CSV file from S3 object and convert it to the dataframe and return the dataframe'''
        
        logging.info("Entered the get_df_from_object method of S3Opeartions class")

        try:
            content=self.read_object(object_,make_readable=True)
            df=read_csv(content,na_values="na")
            logging.info("Exited the get_df_from_object method of S3Operations class")
            return df
        except Exception as e:
            raise CustomException(e,sys) from e
        
    
    def read_csv(self,filename:str,bucket_name:str)->DataFrame:

        logging.info("Entered the read_csv method of S3Opeartions class")
        try:
            csv_obj=self.get_file_object(filename,bucket_name)
            df=self.get_df_from_object(csv_obj)
            logging.info("Exited the read_csv method of S3Opeartions class")
            return df
        except Exception as e:
            raise CustomException(e,sys) from e







        

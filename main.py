from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.components.data_validation import DataValidation
from networksecurity.components.data_transformation import DataTransformation
from networksecurity.components.model_trainer import ModelTrainer
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.entity.config_entity import (
    DataIngestionConfig,
    DataValidationConfig,
    DataTransformationConfig,
    ModelTrainerConfig,
    TrainingPipelineConfig,
)
from networksecurity.entity.artifact_entity import DataIngestionArtifact

import sys

if __name__ == "__main__":
    print("########### Main started ###########")
    try:
        logging.info("Data Ingestion Started")
        training_pipeline_config = TrainingPipelineConfig()
        dataingestionconfig = DataIngestionConfig(training_pipeline_config)
        data_ingestion = DataIngestion(dataingestionconfig)
        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
        print(data_ingestion_artifact)
        logging.info("Data Ingestion Completed")

        logging.info("Data Validation Started")
        data_validation_config = DataValidationConfig(training_pipeline_config)
        data_validation = DataValidation(
            data_ingestion_artifact, data_validation_config
        )
        data_validation_artifact = data_validation.initiate_data_validation()
        print(data_validation_artifact)
        logging.info("Data Validation Completed")

        logging.info("data Transformation Started")
        data_transformation_config = DataTransformationConfig(training_pipeline_config)
        data_transformation = DataTransformation(
            data_validation_artifact, data_transformation_config
        )
        data_transformation_artifact = (
            data_transformation.initiate_data_transformation()
        )
        print(data_transformation_artifact)
        logging.info("data Transformation Completed")

        logging.info("Model Training Started")
        model_trainer_config = ModelTrainerConfig(training_pipeline_config)
        model_trainer = ModelTrainer(
            model_trainer_config=model_trainer_config,
            data_transformation_artifact=data_transformation_artifact,
        )
        model_trainer_artifact = model_trainer.initiate_model_trainer()
        print(model_trainer_artifact)
        logging.info("Model Training Completed")

    except Exception as e:
        raise NetworkSecurityException(e, sys)
    print("######### Main Completed ###########")

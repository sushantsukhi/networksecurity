from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.entity.config_entity import (
    DataIngestionConfig,
    TrainingPipelineConfig,
)
from networksecurity.logging.logger import logging
from networksecurity.exception.exception import NetworkSecurityException

import sys

if __name__ == "__main__":
    try:
        logging.info("Entered the data_ingestion: initiat_data_ingestion method")
        data_ingestion_config = DataIngestionConfig(
            training_pipeline_config=TrainingPipelineConfig()
        )
        data_ingestion = DataIngestion(data_ingestion_config)
        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
        print(data_ingestion_artifact)
        logging.info("Exited the data_ingestion: initiat_data_ingestion method")
    except Exception as e:
        raise NetworkSecurityException(e, sys)

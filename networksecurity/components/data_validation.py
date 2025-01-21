from networksecurity.entity.artifact_entity import (
    DataIngestionArtifact,
    DataValidationArtifact,
)
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from scipy.stats import ks_2samp
import pandas as pd
import os, sys

from networksecurity.utils.main_utils import utils
from networksecurity.constants import training_pipeline


class DataValidation:
    def __init__(
        self,
        data_ingestion_artifact: DataIngestionArtifact,
        data_validation_config: DataValidationConfig,
    ):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config = utils.read_yaml_file(
                training_pipeline.SCHEMA_FILE_PATH
            )
            # print(self._schema_config)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    @staticmethod
    def read_data(file_path) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def validate_number_of_columns(self, dataFrame: pd.DataFrame) -> bool:
        try:
            number_of_columns = len(self._schema_config)
            logging.info(f"Required number of columns:{number_of_columns}")
            logging.info(f"Data frame has columns:{len(dataFrame.columns)}")
            if len(dataFrame.columns) == number_of_columns:
                return True
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def detect_dataset_drift(self, base_df, current_df, threshold=0.05) -> bool:
        try:
            status = True
            report = {}
            for column in base_df.columns:
                d1 = base_df[column]
                d2 = current_df[column]
                is_same_dist = ks_2samp(d1, d2)

                if threshold <= is_same_dist.pvalue:
                    is_found = False
                else:
                    is_found = True
                    status = False

                report.update(
                    {
                        column: {
                            "p_value": float(is_same_dist.pvalue),
                            "drift_status": is_found,
                        }
                    }
                )
            self.drift_report_file_path = (
                self.data_validation_config.drift_report_file_path
            )
            # Create Directory
            dir_path = os.path.dirname(self.drift_report_file_path)
            os.makedirs(dir_path, exist_ok=True)
            utils.write_yaml_file(file_path=self.drift_report_file_path, content=report)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            # Read the data from train and test
            train_dataFrame = DataValidation.read_data(train_file_path)
            test_dataFrame = DataValidation.read_data(test_file_path)

            # Validate number of columns
            status = self.validate_number_of_columns(dataFrame=train_dataFrame)
            if not status:
                error_message = f"Trained dataframe does not contains all columns. \n"
            status = self.validate_number_of_columns(dataFrame=test_dataFrame)
            if not status:
                error_message = f"Test dataframe does not contains all columns. \n"

            ## Let's check data drift
            status = self.detect_dataset_drift(
                base_df=train_dataFrame, current_df=test_dataFrame
            )
            dir_path = os.path.dirname(
                self.data_validation_config.valid_train_file_path
            )
            os.makedirs(dir_path, exist_ok=True)

            train_dataFrame.to_csv(
                self.data_validation_config.valid_train_file_path,
                index=False,
                header=True,
            )
            test_dataFrame.to_csv(
                self.data_validation_config.valid_test_file_path,
                index=False,
                header=True,
            )

            data_validation_artifact = DataValidationArtifact(
                validation_status=status,
                valid_train_file_path=self.data_validation_config.valid_train_file_path,
                valid_test_file_path=self.data_validation_config.valid_test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,
            )

            return data_validation_artifact

        except Exception as e:
            raise NetworkSecurityException(e, sys)

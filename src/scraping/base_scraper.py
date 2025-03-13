import logging
import os
from typing import Optional
from abc import ABC, abstractmethod

import pandas as pd


class BaseScraper(ABC):
    """Base Data Scraping class that is inherited by all scrappers"""

    def __init__(self, output_path: str):
        self.output_path = output_path
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger()

    @abstractmethod
    def scrape_data(self):
        """Abstract method to scrape data"""
        pass

    def write_output(self, df: pd.DataFrame) -> None:
        """
        Appends the scraped data to the existing dataset.

        Args:
            df (pd.DataFrame): represents the dataframe
        """
        if not os.path.isfile(self.output_path):
            df.to_csv(self.output_path, index=False)
            self.logger.info(f"File created {self.output_path}")
        else:
            df.to_csv(self.output_path, mode="a", header=False, index=False)
            self.logger.info(f"Data appended to {self.output_path}")

    def build_df(self, columns: Optional[list[str]], data: list) -> pd.DataFrame:
        """
        Builds a dataframe w the provided columns and attributes

        Args:
            columns (Optional[list[str]]): list of columns needed to create the DataFrame
            data (list): data rows needed to create the DataFrame

        Returns:
            pd.DataFrame: the final DataFrame
        """
        self.logger.info("Building dataframe...")
        return pd.DataFrame(data=data, columns=columns)

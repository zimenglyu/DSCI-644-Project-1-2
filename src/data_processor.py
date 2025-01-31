from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    hour,
    sum,
    avg,
    to_timestamp,
    date_trunc,
    round as spark_round,
    explode,
    abs as spark_abs,
    quarter,
    year,
    concat,
    lit,
    count,
    month,
    date_format,
)

from typing import Dict
import glob
import shutil
import os


class DataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

        # Note: all functions are helper functions, you do not have to use them in the main code

    def aggregate_hourly_transactions(self, transactions: DataFrame) -> DataFrame:
        """
        Aggregate transactions by hour, calculating volume and weighted average price
        Note: this is helper function, you do not have to
        """
        return transactions.groupBy(
            date_trunc("hour", "timestamp").alias("datetime"), "ticker"
        ).agg(
            sum("shares").alias("volume"),
            (sum(col("shares") * col("price")) / sum("shares")).alias(
                "avg_stock_price"
            ),
        )

        # Function breakdown:
        # 1. Groups transactions by hour and ticker using date_trunc
        # 2. For each group, calculates:
        #    - Total volume: sum of all shares traded
        #    - Weighted average price: (sum of shares * price) / total shares
        # 3. Returns DataFrame with columns: datetime, ticker, volume, avg_stock_price

    def save_to_csv(self, df: DataFrame, output_path: str, filename: str) -> None:
        """
        Save DataFrame to a single CSV file.

        :param df: DataFrame to save
        :param output_path: Base directory path
        :param filename: Name of the CSV file
        """
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)

        # Create full path for the output file
        full_path = os.path.join(output_path, filename)
        print(f"Saving to: {full_path}")  # Debugging output

        # Create a temporary directory in the correct output path
        temp_dir = os.path.join(output_path, "_temp")
        print(f"Temporary directory: {temp_dir}")  # Debugging output

        # Save to temporary directory
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

        # Find the generated part file
        csv_file = glob.glob(f"{temp_dir}/part-*.csv")[0]

        # Move and rename it to the desired output path
        shutil.move(csv_file, full_path)

        # Clean up - remove the temporary directory
        shutil.rmtree(temp_dir)

    def aggregate_hourly(self, df: DataFrame) -> DataFrame:
        """Standardize the hourly data format"""
        return df.select(
            date_format(col("datetime"), "yyyy-MM-dd HH:mm:ss").alias("datetime"),
            col("ticker"),
            col("company_name"),
            col("avg_stock_price").alias("avg_price"),
            col("volume"),
            col("market_index"),
        ).orderBy("datetime", "ticker")

        # Function breakdown:
        # 1. Standardizes datetime format to "yyyy-MM-dd HH:mm:ss"
        # 2. Selects and renames required columns:
        #    - datetime: formatted timestamp
        #    - ticker: stock symbol
        #    - company_name: name of the company
        #    - avg_stock_price → avg_price: average price for the hour
        #    - volume: total trading volume
        #    - market_index: relevant market index
        # 3. Orders results by datetime and ticker

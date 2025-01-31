from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from typing import Dict, Tuple
from data_processor import DataProcessor
from dotenv import load_dotenv
import os
import yaml


def create_spark_session(mysql_connector_path: str, mongodb_uri: str) -> SparkSession:
    """
    Create and return a SparkSession with necessary configurations.

    :param mysql_connector_path: Path to MySQL JDBC connector JAR
    :param mongodb_uri: URI for MongoDB connection
    :return: Configured SparkSession
    """
    return (
        SparkSession.builder.appName("StockDataWarehouse")
        .config("spark.jars", mysql_connector_path)
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .config("spark.mongodb.input.uri", mongodb_uri)
        .getOrCreate()
    )


def main(config: Dict[str, str]) -> Tuple[DataFrame, SparkSession]:
    """
    Main function to process stock data for data warehouse

    :param config: Configuration dictionary containing database and path information
    :return: Tuple of the processed DataFrame and SparkSession
    """
    spark = create_spark_session(config["mysql_connector_path"], config["mongodb_uri"])
    data_processor = DataProcessor(spark)

    # TODO: Implement the following steps:
    # 1. Load data from sources (MySQL and MongoDB)
    # 2. Process hourly transactions
    # 3. Create aggregations (hourly/daily/monthly/quarterly)
    # 4. Save results to CSV files
    print("Code not implemented")

    return None, spark


if __name__ == "__main__":
    # Load configuration
    load_dotenv()

    # Load and process config
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
        # Replace environment variables
        for key, value in config.items():
            if (
                isinstance(value, str)
                and value.startswith("${")
                and value.endswith("}")
            ):
                env_var = value[2:-1]
                env_value = os.getenv(env_var)
                if env_value is None:
                    print(f"Warning: Environment variable {env_var} not found")
                config[key] = env_value or value

    processed_df, spark = main(config)
    spark.stop()

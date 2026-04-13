from __future__ import annotations

import os
import sys
import logging
from typing import Optional,Dict
from pyspark.sql import SparkSession
from config.settings import SparkSettings, get_settings
logger = logging.getLogger(__name__)

_spark: Optional[SparkSession] = None

def build_spark_session(
        settings:Optional[SparkSettings] = None,
        extract_config: dict[str, str] = None,
) -> SparkSession:
    """Get the Spark session."""
    # Ensure Spark can find the correct Python executable on Windows
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    cfg = get_settings().spark

    base_config: dict[str, str] = {
        "spark.jars.packages": (
            "io.delta:delta-spark_2.12:3.1.0"
        ),
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": (
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        ),
        # Performance
        "spark.sql.shuffle.partitions": str(cfg.sql_shuffle_partitions),
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        # Memory
        "spark.executor.memory": cfg.executor_memory,
        "spark.driver.memory": cfg.driver_memory,
        # Reliability
        "spark.task.maxFailures": "3",
        # Parquet optimisations
        "spark.sql.parquet.mergeSchema": "false",
        "spark.sql.parquet.filterPushdown": "true",
        "spark.hadoop.parquet.enable.summary-metadata": "false",
    }

    if extract_config:
        base_config.update(extract_config)

    builder = SparkSession.builder.appName(cfg.app_name).master(cfg.master)
    for key, value in base_config.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(cfg.spark_log_level)

    logger.info(
        "Spark session created",
        extra={"app_name": cfg.app_name,"master": cfg.master}
    )

    return spark

def get_spark(
        extract_configs: dict[str, str] = None,
)-> SparkSession:
    """Get the Spark session."""
    global _spark
    if _spark is None:
        _spark = build_spark_session(extract_config=extract_configs)
    return _spark

def close_spark():
    """Close the Spark session."""
    global _spark
    if _spark is not None:
        logger.info("Closing Spark session")
        _spark.stop()
        _spark = None
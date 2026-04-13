import pytest
from pyspark.sql import SparkSession

def test_spark_session_initialization(spark_session):
    """
    Check if the spark_session fixture from conftest.py
    can actually start Spark and return a session.
    """
    assert isinstance(spark_session, SparkSession)
    assert spark_session.sparkContext.appName is not None

    # Simple operation to ensure the JVM and Spark processes
    # are communicating correctly
    df = spark_session.createDataFrame([(1, "test")], ["id", "val"])
    assert df.count() == 1
    print("\nSpark session initialized successfully!")


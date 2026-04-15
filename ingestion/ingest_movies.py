import os
import pathlib

from utils.api import fetch_all_movies
from config.spark_config import get_spark
from utils.logger import get_logger
from utils.parquet import write_parquet
from ingestion.schema import MOVIE_SCHEMA

logger = get_logger(__name__)

MOVIE_IDS = [
    299534, 19995, 140607, 299536, 597, 135397, 420818,
    24428, 168259, 99861, 284054, 12445, 181808, 330457,
    351286, 109445, 321612, 260513,
]


def run_ingestion():
    from dotenv import load_dotenv
    load_dotenv()

    logger.info(f"Starting ingestion for {len(MOVIE_IDS)} movies")

    movies_data = fetch_all_movies(MOVIE_IDS)
    if not movies_data:
        logger.warning("No movies returned from API — aborting ingestion")
        return

    spark = get_spark()

    try:
        # Explicit schema prevents Spark from inferring string fields as
        # IntegerType when they happen to be null across the entire batch.
        # createDataFrame(list, schema) uses the driver-side Row conversion path
        # and avoids spawning Python worker processes (which fail to connect back
        # on this Windows machine due to socket/firewall policy).
        df = spark.createDataFrame(movies_data, schema=MOVIE_SCHEMA)

        if "id" in df.columns:
            df = df.dropDuplicates(["id"])

        count = df.count()
        logger.info(f"DataFrame created: {count} rows | {len(df.columns)} columns")
        df.printSchema()

        output_path = pathlib.Path(os.getcwd()) / "data" / "raw" / "movies.parquet"
        logger.info(f"Writing {count} records to {output_path}")
        write_parquet(df, output_path)

        logger.info("Ingestion completed successfully")

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise

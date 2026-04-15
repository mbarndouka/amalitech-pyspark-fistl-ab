from pyspark.sql import DataFrame

from utils.api import fetch_all_movies
from config.spark_config import get_spark
from config.settings import get_settings
from utils.logger import get_logger
from utils.parquet import write_parquet
from ingestion.schema import MOVIE_SCHEMA

logger = get_logger(__name__)


def run_ingestion(persist: bool = True) -> DataFrame:
    """Fetch movies from the TMDB API and return a Spark DataFrame.

    Args:
        persist: Write the raw DataFrame to disk as a parquet checkpoint.
                 Defaults to True — API responses are rate-limited and slow to
                 re-fetch, so caching them is almost always worthwhile.

    Returns:
        Raw Spark DataFrame (one row per movie, including cast_raw / crew_raw).
    """
    settings = get_settings()
    movie_ids = settings.pipeline.movie_ids

    logger.info(f"Starting ingestion for {len(movie_ids)} movies")

    movies_data = fetch_all_movies(movie_ids)
    if not movies_data:
        logger.warning("No movies returned from API — aborting ingestion")
        return None

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

        logger.info(f"DataFrame created: {len(df.columns)} columns")
        df.printSchema()

        if persist:
            output_path = settings.storage.raw_data_path / "movies.parquet"
            logger.info(f"Writing raw records to {output_path}")
            write_parquet(df, output_path)

        logger.info("Ingestion completed successfully")
        return df

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise

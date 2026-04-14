import os
from dotenv import load_dotenv

from utils.api import fetch_all_movies
from config.spark_config import get_spark, close_spark
from utils.logger import get_logger

logger = get_logger(__name__)

def run_ingestion():
    load_dotenv()

    movie_ids = [0, 299534, 19995, 140607, 299536, 597, 135397, 420818, 24428, 168259, 99861, 284054, 12445, 181808, 330457, 351286, 109445, 321612, 260513]

    logger.info(f"starting_ingestion {len(movie_ids)} movies")

    movies_data = fetch_all_movies(movie_ids)
    if not movies_data:
        logger.warning("No movies found")
        return

    spark = get_spark()

    try:
        # Avoid java.lang.UnsatisfiedLinkError from Hadoop's NativeIO on Windows

        df = spark.createDataFrame(movies_data)
        df.printSchema()

        output_path = os.path.join(os.getcwd(), "data","raw", "movies")

        logger.info(f"writing  {df.count()} saving to {output_path}")

        df.write\
                .format("parquet")\
                .mode("overwrite")\
                .save(output_path)

        logger.info("ingestion completed successfully")

    except Exception as e:
        logger.error(f"ingestion failed with error {e}")
        raise
    finally:
        close_spark()
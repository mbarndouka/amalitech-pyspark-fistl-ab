import os
from dotenv import load_dotenv

from utils.api import fetch_all_movies
from config.spark_config import get_spark
from utils.logger import get_logger
import pathlib

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

        # Idempotency: Drop duplicate records based on the movie 'id' if any exist in the API response
        if 'id' in df.columns:
            df = df.dropDuplicates(['id'])

        df.cache() # Cache to avoid re-computation and potentially multiple write attempts
        df.printSchema()

        output_dir = pathlib.Path(os.getcwd()) / "data" / "raw"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = str(output_dir / "movies.parquet")

        count = df.count()
        logger.info(f"writing {count} records to {output_path}")

        # Collect to pandas and write via pyarrow to avoid Hadoop winutils
        # permission errors on Windows (ExitCode -1073741515 / STATUS_DLL_NOT_FOUND)
        df.toPandas().to_parquet(output_path, index=False, engine="pyarrow")
        # df.write.mode("overwrite").parquet(output_path)

        logger.info("ingestion completed successfully")

    except Exception as e:
        logger.error(f"ingestion failed with error {e}")
        raise
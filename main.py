from dotenv import load_dotenv
load_dotenv()

from ingestion.ingest_movies import run_ingestion
from processing.clean_movies import run_cleaning
from analysis.kpi_movies import run_kpi
from config.spark_config import close_spark


if __name__ == '__main__':
    try:
        # run_ingestion()
        # run_cleaning()
        run_kpi()
    finally:
        close_spark()


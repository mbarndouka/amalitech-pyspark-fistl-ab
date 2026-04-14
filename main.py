from utils.api import fetch_all_movies
from dotenv import load_dotenv
load_dotenv()
import os
from ingestion.ingest_movies import run_ingestion


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Run the PySpark ingestion pipeline
    run_ingestion()


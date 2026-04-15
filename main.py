from dotenv import load_dotenv
load_dotenv()

from utils.logger import configure_logger
configure_logger()

from ingestion.ingest_movies import run_ingestion
from processing.clean_movies import run_cleaning
from analysis.kpi_movies import run_kpi
from analysis.advanced_queries import run_advanced_queries
from visualization.plots import run_visualization
from config.spark_config import close_spark


if __name__ == '__main__':
    try:
        # ── Full in-memory pipeline (no intermediate disk I/O) ─────────────────
        # Each stage returns a DataFrame that is passed directly to the next.
        # Set persist=True on run_ingestion / run_cleaning to write checkpoints
        # to disk (useful when iterating on downstream stages without re-fetching
        # from the API or re-running the cleaning transforms).

        raw_df   = run_ingestion(persist=True)   # always cache raw API data
        clean_df = run_cleaning(raw_df)           # no disk write — flows in memory
        run_kpi(clean_df)
        run_advanced_queries(clean_df)
        run_visualization(clean_df)

    finally:
        close_spark()

import os
import pathlib
from functools import reduce
from operator import add

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from config.spark_config import get_spark
from utils.logger import get_logger
from utils.parquet import write_parquet

logger = get_logger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

COLUMNS_TO_DROP = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']

JSON_LIKE_COLUMNS = [
    'belongs_to_collection',
    'genres',
    'production_countries',
    'production_companies',
    'spoken_languages',
]

TEXT_PLACEHOLDERS = ['No Data', 'N/A', 'NA', 'None', 'none', 'n/a', '']

FINAL_COLUMNS = [
    'id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection',
    'original_language', 'budget_musd', 'revenue_musd', 'production_companies',
    'production_countries', 'vote_count', 'vote_average', 'popularity', 'runtime',
    'overview', 'spoken_languages', 'poster_path', 'cast', 'cast_size', 'director',
    'crew_size',
]

# ── Step 2 helpers ─────────────────────────────────────────────────────────────

def _extract_json_columns(df):
    """Extract and flatten nested struct/array columns into pipe-separated strings.

    Explicit .cast('string') is required on every name field because Spark infers
    all-null columns as IntegerType, which causes array_join to raise a type error.
    """
    # belongs_to_collection is a single struct → pull the name field
    df = df.withColumn(
        "collection_name",
        F.col("belongs_to_collection.name").cast("string"),
    ).drop("belongs_to_collection")

    df = df.withColumn(
        "genres",
        F.array_join(
            F.transform(F.col("genres"), lambda x: x["name"].cast("string")),
            "|",
        ),
    )
    df = df.withColumn(
        "spoken_languages",
        # english_name carries the display value; name is locale-native script
        F.array_join(
            F.transform(F.col("spoken_languages"), lambda x: x["english_name"].cast("string")),
            "|",
        ),
    )
    df = df.withColumn(
        "production_countries",
        F.array_join(
            F.transform(F.col("production_countries"), lambda x: x["name"].cast("string")),
            "|",
        ),
    )
    df = df.withColumn(
        "production_companies",
        F.array_join(
            F.transform(F.col("production_companies"), lambda x: x["name"].cast("string")),
            "|",
        ),
    )
    return df


def _extract_cast_crew(df):
    """Extract cast names, cast size, director, and crew size from raw arrays."""
    df = (
        df
        .withColumn(
            "cast_size",
            F.when(F.col("cast_raw").isNotNull(), F.size(F.col("cast_raw"))),
        )
        .withColumn(
            "cast",
            F.array_join(
                F.transform(F.col("cast_raw"), lambda x: x["name"].cast("string")),
                "|",
            ),
        )
        .withColumn(
            "crew_size",
            F.when(F.col("crew_raw").isNotNull(), F.size(F.col("crew_raw"))),
        )
        .withColumn(
            "director",
            F.array_join(
                F.transform(
                    F.filter(
                        F.col("crew_raw"),
                        lambda x: x["job"].cast("string") == "Director",
                    ),
                    lambda x: x["name"].cast("string"),
                ),
                "|",
            ),
        )
        .drop("cast_raw", "crew_raw")
    )
    return df


def _inspect_columns(df):
    """Run value_counts on extracted columns to surface anomalies."""
    for col_name in ["collection_name", "genres", "spoken_languages",
                     "production_countries", "production_companies"]:
        if col_name not in df.columns:
            logger.warning(f"Column '{col_name}' not found — skipping inspection")
            continue
        logger.info(f"value_counts → {col_name}")
        df.groupBy(col_name).count().orderBy(F.desc("count")).show(20, truncate=False)


# ── Step 5 ─────────────────────────────────────────────────────────────────────

def _convert_dtypes(df):
    """Cast columns to their intended types; unparseable values become null.

    - budget / revenue → DoubleType  (enables null-safe arithmetic for musd conversion)
    - popularity       → DoubleType  (already double in raw data, cast for explicitness)
    - release_date     → DateType    (to_date returns null on parse failure, no exception)
    """
    df = df.withColumn("budget", F.col("budget").cast(DoubleType()))
    df = df.withColumn("revenue", F.col("revenue").cast(DoubleType()))
    df = df.withColumn("popularity", F.col("popularity").cast(DoubleType()))
    df = df.withColumn("release_date", F.to_date(F.col("release_date")))
    return df


# ── Step 6 ─────────────────────────────────────────────────────────────────────

def _replace_unrealistic(df):
    """Replace known-bad values with null and derive budget_musd / revenue_musd.

    - Budget / Revenue / Runtime == 0  →  null  (zero is not a valid measurement)
    - budget / revenue converted to million USD, originals dropped
    - vote_average set to null when vote_count == 0  (no votes → rating is meaningless)
    - overview / tagline placeholder strings replaced with null
    """
    # Zero financial / runtime values are missing data, not real zeros
    df = df.withColumn("budget",  F.when(F.col("budget")  == 0, None).otherwise(F.col("budget")))
    df = df.withColumn("revenue", F.when(F.col("revenue") == 0, None).otherwise(F.col("revenue")))
    df = df.withColumn("runtime", F.when(F.col("runtime") == 0, None).otherwise(F.col("runtime")))

    # Derive million-USD columns then drop raw dollar columns
    df = df.withColumn("budget_musd",  F.round(F.col("budget")  / 1_000_000, 2))
    df = df.withColumn("revenue_musd", F.round(F.col("revenue") / 1_000_000, 2))
    df = df.drop("budget", "revenue")

    # A rating with zero votes carries no information
    df = df.withColumn(
        "vote_average",
        F.when(F.col("vote_count") == 0, None).otherwise(F.col("vote_average")),
    )

    # Strip known placeholder text from free-text fields
    df = df.withColumn(
        "overview",
        F.when(F.trim(F.col("overview")).isin(TEXT_PLACEHOLDERS), None)
         .otherwise(F.col("overview")),
    )
    df = df.withColumn(
        "tagline",
        F.when(F.trim(F.col("tagline")).isin(TEXT_PLACEHOLDERS), None)
         .otherwise(F.col("tagline")),
    )
    return df


# ── Step 7 ─────────────────────────────────────────────────────────────────────

def _remove_duplicates(df):
    """Drop duplicate movie ids and rows with an unknown id or title."""
    before = df.count()
    df = df.dropDuplicates(["id"])
    df = df.filter(F.col("id").isNotNull() & F.col("title").isNotNull())
    after = df.count()
    logger.info(f"Duplicate / unknown-id removal: {before} → {after} rows")
    return df


# ── Step 8 ─────────────────────────────────────────────────────────────────────

def _filter_min_non_null(df, min_non_null: int = 10):
    """Keep only rows where at least `min_non_null` columns have non-null values."""
    non_null_count = reduce(
        add,
        [F.col(c).isNotNull().cast("int") for c in df.columns],
    )
    before = df.count()
    df = df.filter(non_null_count >= min_non_null)
    after = df.count()
    logger.info(f"Min-non-null filter (>= {min_non_null}): {before} → {after} rows")
    return df


# ── Step 9 ─────────────────────────────────────────────────────────────────────

def _filter_released(df):
    """Keep only movies with status == 'Released', then drop the status column."""
    before = df.count()
    df = df.filter(F.col("status") == "Released").drop("status")
    after = df.count()
    logger.info(f"Released-status filter: {before} → {after} rows")
    return df


# ── Steps 10–11 ────────────────────────────────────────────────────────────────

def _finalize(df):
    """Rename collection_name → belongs_to_collection, reorder to FINAL_COLUMNS.

    PySpark DataFrames have no row index, so 'reset index' is a no-op here.
    Columns not present in the DataFrame (e.g. due to upstream data gaps) are
    warned about and skipped so the pipeline never hard-errors on missing columns.
    """
    df = df.withColumnRenamed("collection_name", "belongs_to_collection")

    available = [c for c in FINAL_COLUMNS if c in df.columns]
    missing   = [c for c in FINAL_COLUMNS if c not in df.columns]
    if missing:
        logger.warning(f"Expected columns missing from DataFrame: {missing}")

    df = df.select(available)
    logger.info(f"Final column order ({len(available)} cols): {available}")
    return df


# ── Pipeline entry point ───────────────────────────────────────────────────────

def run_cleaning():
    spark = get_spark()
    logger.info("Starting data cleaning step")

    try:
        input_path = os.path.join(os.getcwd(), "data", "raw", "movies.parquet")
        logger.info(f"Reading raw data from: {input_path}")

        df = spark.read.parquet(input_path)
        logger.info(f"Row count: {df.count()} | Columns: {len(df.columns)}")
        logger.info("Original schema:")
        df.printSchema()

        # Step 1 — drop irrelevant columns
        logger.info(f"Dropping irrelevant columns: {COLUMNS_TO_DROP}")
        df = df.drop(*COLUMNS_TO_DROP)

        # Step 2 — extract JSON-like struct / array columns
        logger.info(f"Extracting JSON-like columns: {JSON_LIKE_COLUMNS}")
        df = _extract_json_columns(df)
        logger.info("Extracting cast and crew columns")
        df = _extract_cast_crew(df)

        # Inspect extracted columns to surface data-quality anomalies
        _inspect_columns(df)

        logger.info("Schema after extraction:")
        df.printSchema()

        # Step 5 — convert column datatypes
        logger.info("Converting column datatypes")
        df = _convert_dtypes(df)

        # Step 6 — replace unrealistic / placeholder values
        logger.info("Replacing unrealistic values")
        df = _replace_unrealistic(df)

        # Step 7 — remove duplicates and rows with unknown id / title
        df = _remove_duplicates(df)

        # Step 8 — require at least 10 non-null columns per row
        df = _filter_min_non_null(df, min_non_null=10)

        # Step 9 — keep Released movies only, drop status
        df = _filter_released(df)

        # Steps 10–11 — reorder columns and reset index
        df = _finalize(df)

        output_path = pathlib.Path(os.getcwd()) / "data" / "processed" / "movies_cleaned.parquet"
        logger.info(f"Saving cleaned data to: {output_path}")
        write_parquet(df, output_path)
        logger.info("Cleaning completed successfully")

        return df

    except Exception as e:
        logger.error(f"Cleaning failed: {e}")
        raise

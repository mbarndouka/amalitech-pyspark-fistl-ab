import os
import pathlib
from pyspark.sql import functions as F
from config.spark_config import get_spark
from utils.logger import get_logger

logger = get_logger(__name__)

COLUMNS_TO_DROP = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']

JSON_LIKE_COLUMNS = [
    'belongs_to_collection',
    'genres',
    'production_countries',
    'production_companies',
    'spoken_languages',
]


def _extract_json_columns(df):
    """Extract and flatten nested struct/array columns into pipe-separated strings.

    All extracted name fields are explicitly cast to string before joining because
    Spark infers all-null columns as IntegerType, which causes array_join to fail.
    """

    # belongs_to_collection is a single struct → extract the name field
    df = df.withColumn(
        "collection_name",
        F.col("belongs_to_collection.name").cast("string"),
    ).drop("belongs_to_collection")

    # genres → extract name from each struct element, join with "|"
    df = df.withColumn(
        "genres",
        F.array_join(
            F.transform(F.col("genres"), lambda x: x["name"].cast("string")),
            "|",
        ),
    )

    # spoken_languages → use english_name (name field is locale-native script)
    df = df.withColumn(
        "spoken_languages",
        F.array_join(
            F.transform(F.col("spoken_languages"), lambda x: x["english_name"].cast("string")),
            "|",
        ),
    )

    # production_countries → extract name, join with "|"
    df = df.withColumn(
        "production_countries",
        F.array_join(
            F.transform(F.col("production_countries"), lambda x: x["name"].cast("string")),
            "|",
        ),
    )

    # production_companies → extract name, join with "|"
    df = df.withColumn(
        "production_companies",
        F.array_join(
            F.transform(F.col("production_companies"), lambda x: x["name"].cast("string")),
            "|",
        ),
    )

    return df


def _inspect_columns(df):
    """Run value_counts on extracted columns to surface anomalies."""
    inspect_cols = [
        "collection_name",
        "genres",
        "spoken_languages",
        "production_countries",
        "production_companies",
    ]
    for col_name in inspect_cols:
        if col_name not in df.columns:
            logger.warning(f"Column '{col_name}' not found — skipping inspection")
            continue
        logger.info(f"value_counts → {col_name}")
        df.groupBy(F.col(col_name)).count().orderBy(F.desc("count")).show(20, truncate=False)


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

        # Step 2 — evaluate JSON-like columns (log before extraction)
        logger.info(f"JSON-like columns to extract: {JSON_LIKE_COLUMNS}")

        # Step 3 — extract and flatten nested struct/array columns
        logger.info("Extracting nested columns into pipe-separated strings")
        df = _extract_json_columns(df)
        logger.info("Schema after extraction:")
        df.printSchema()

        # Step 4 — inspect extracted columns with value_counts to identify anomalies
        _inspect_columns(df)

        # Persist cleaned result
        output_dir = pathlib.Path(os.getcwd()) / "data" / "processed"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = str(output_dir / "movies_cleaned.parquet")

        logger.info(f"Saving cleaned data to: {output_path}")
        df.write.mode("overwrite").parquet(output_path)
        logger.info("Cleaning completed successfully")

        return df

    except Exception as e:
        logger.error(f"Cleaning failed: {e}")
        raise

import os
from pyspark.sql import functions as F
from config.spark_config import get_spark
from utils.logger import get_logger

logger = get_logger(__name__)


# ── Internal helpers ───────────────────────────────────────────────────────────

def _load_df(spark):
    """Read the cleaned parquet and add derived columns used across all queries."""
    input_path = os.path.join(os.getcwd(), "data", "processed", "movies_cleaned.parquet")
    df = spark.read.parquet(input_path)

    # profit and roi are derived here (not stored in parquet) so every query
    # section can use them without re-importing the UDFs from kpi_movies.
    df = df.withColumn(
        "profit_musd",
        F.when(
            F.col("revenue_musd").isNotNull() & F.col("budget_musd").isNotNull(),
            F.round(F.col("revenue_musd") - F.col("budget_musd"), 2),
        ),
    )
    df = df.withColumn(
        "roi",
        F.when(
            F.col("budget_musd").isNotNull() & (F.col("budget_musd") > 0),
            F.round(F.col("revenue_musd") / F.col("budget_musd"), 4),
        ),
    )
    return df


def _show(title: str, df, n: int = 20):
    separator = "=" * 60
    logger.info(f"\n{separator}\n  {title}\n{separator}")
    df.show(n, truncate=False)


# ── Section 2 — Advanced Search Queries ───────────────────────────────────────
# Cast and genres are stored as pipe-separated strings, so containment checks
# use Column.contains() which maps to SQL LIKE '%value%'.
# NOTE: cast / director / genres are currently empty in the dataset due to an
# upstream API gap (name fields return null). Results will appear once fixed.

def search_scifi_action_bruce_willis(df):
    """Search 1: Best-rated Science Fiction + Action movies starring Bruce Willis.

    Filters on the pipe-separated genres and cast columns, then sorts by
    vote_average descending so the highest-rated titles appear first.
    """
    result = (
        df
        .filter(
            F.col("genres").contains("Science Fiction") &
            F.col("genres").contains("Action") &
            F.col("cast").contains("Bruce Willis"),
        )
        .select("title", "release_date", "genres", "vote_average", "vote_count", "cast")
        .orderBy(F.desc("vote_average"))
    )
    _show("Search 1: Best-rated Sci-Fi Action movies starring Bruce Willis", result)
    return result


def search_uma_thurman_tarantino(df):
    """Search 2: Movies starring Uma Thurman directed by Quentin Tarantino.

    Sorted by runtime ascending (shortest to longest).
    """
    result = (
        df
        .filter(
            F.col("cast").contains("Uma Thurman") &
            F.col("director").contains("Quentin Tarantino"),
        )
        .select("title", "release_date", "runtime", "vote_average", "director", "cast")
        .orderBy(F.asc("runtime"))
    )
    _show("Search 2: Uma Thurman × Quentin Tarantino (sorted by runtime)", result)
    return result


# ── Section 3 — Franchise vs Standalone Performance ───────────────────────────

def franchise_vs_standalone(df):
    """Compare franchise vs standalone movie performance across 5 KPIs.

    A movie is a franchise entry when belongs_to_collection is non-null and
    non-empty. Median ROI uses percentile_approx(0.5) — Spark's scalable
    approximate median that works across any partition count.
    """
    df = df.withColumn(
        "type",
        F.when(
            F.col("belongs_to_collection").isNotNull() &
            (F.trim(F.col("belongs_to_collection")) != ""),
            "Franchise",
        ).otherwise("Standalone"),
    )

    result = (
        df
        .groupBy("type")
        .agg(
            F.count("*").alias("movie_count"),
            F.round(F.mean("revenue_musd"),                      2).alias("mean_revenue_musd"),
            F.round(F.percentile_approx("roi", 0.5),             4).alias("median_roi"),
            F.round(F.mean("budget_musd"),                       2).alias("mean_budget_musd"),
            F.round(F.mean("popularity"),                        4).alias("mean_popularity"),
            F.round(F.mean("vote_average"),                      3).alias("mean_rating"),
        )
        .orderBy("type")
    )

    _show("Franchise vs Standalone — Performance Comparison", result)
    return result


# ── Section 4 — Most Successful Movie Franchises ──────────────────────────────

def most_successful_franchises(df):
    """Rank franchises by total revenue, showing budget, revenue, and rating KPIs.

    Only rows where belongs_to_collection is populated are included.
    """
    franchise_df = df.filter(
        F.col("belongs_to_collection").isNotNull() &
        (F.trim(F.col("belongs_to_collection")) != ""),
    )

    result = (
        franchise_df
        .groupBy("belongs_to_collection")
        .agg(
            F.count("*").alias("movie_count"),
            F.round(F.sum("budget_musd"),    2).alias("total_budget_musd"),
            F.round(F.mean("budget_musd"),   2).alias("mean_budget_musd"),
            F.round(F.sum("revenue_musd"),   2).alias("total_revenue_musd"),
            F.round(F.mean("revenue_musd"),  2).alias("mean_revenue_musd"),
            F.round(F.mean("vote_average"),  3).alias("mean_rating"),
        )
        .orderBy(F.desc("total_revenue_musd"))
    )

    _show("Most Successful Movie Franchises (by total revenue)", result)
    return result


# ── Section 5 — Most Successful Directors ─────────────────────────────────────

def most_successful_directors(df):
    """Rank directors by total revenue, including movie count and mean rating.

    The director column is pipe-separated to support co-directed films.
    Each director name is split out and exploded into its own row so groupBy
    works at the individual-director level. Empty tokens (from all-null API
    data) are filtered out before aggregation.
    """
    director_df = (
        df
        .withColumn(
            "director_name",
            F.explode(F.split(F.col("director"), "\\|")),
        )
        .filter(F.trim(F.col("director_name")) != "")
    )

    result = (
        director_df
        .groupBy("director_name")
        .agg(
            F.count("*").alias("total_movies"),
            F.round(F.sum("revenue_musd"),   2).alias("total_revenue_musd"),
            F.round(F.mean("vote_average"),  3).alias("mean_rating"),
        )
        .orderBy(F.desc("total_revenue_musd"))
    )

    _show("Most Successful Directors (by total revenue)", result)
    return result


# ── Pipeline entry point ───────────────────────────────────────────────────────

def run_advanced_queries():
    spark = get_spark()
    logger.info("Starting advanced queries and franchise analysis")

    try:
        df = _load_df(spark)
        logger.info(f"Loaded {df.count()} rows | {len(df.columns)} columns")

        # Section 2 — Search queries
        search_scifi_action_bruce_willis(df)
        search_uma_thurman_tarantino(df)

        # Section 3 — Franchise vs Standalone
        franchise_vs_standalone(df)

        # Section 4 — Most Successful Franchises
        most_successful_franchises(df)

        # Section 5 — Most Successful Directors
        most_successful_directors(df)

        logger.info("Advanced queries completed")

    except Exception as e:
        logger.error(f"Advanced queries failed: {e}")
        raise

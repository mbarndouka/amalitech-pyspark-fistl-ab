import os
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType

from config.spark_config import get_spark
from utils.logger import get_logger

logger = get_logger(__name__)

TOP_N = 5  # rows returned per KPI table

# ── UDFs ───────────────────────────────────────────────────────────────────────
# UDFs handle None-safety explicitly so no downstream null-guard is needed.
# Note: column expressions (F.col - F.col) would be faster for production
# workloads; UDFs are used here to centralise the derived-metric logic.

@F.udf(returnType=DoubleType())
def profit_udf(revenue_musd: float, budget_musd: float):
    """Revenue minus budget (million USD). Returns None if either input is null."""
    if revenue_musd is None or budget_musd is None:
        return None
    return round(revenue_musd - budget_musd, 2)


@F.udf(returnType=DoubleType())
def roi_udf(revenue_musd: float, budget_musd: float):
    """Revenue divided by budget. Returns None when budget is null or zero."""
    if revenue_musd is None or budget_musd is None or budget_musd == 0.0:
        return None
    return round(revenue_musd / budget_musd, 4)


# ── Ranking helper ─────────────────────────────────────────────────────────────

def rank_movies(
    df,
    metric_col: str,
    label: str,
    ascending: bool = False,
    filter_expr=None,
    n: int = TOP_N,
):
    """Rank movies by *metric_col* and return the top/bottom *n* results.

    Args:
        df:          Cleaned movies DataFrame.
        metric_col:  Name of the column to sort by (must already exist in df).
        label:       Display name shown in the output column header.
        ascending:   False → highest first (best performers).
                     True  → lowest first (worst performers).
        filter_expr: Optional Spark Column expression applied before ranking.
        n:           Number of rows to return.

    Returns:
        DataFrame with columns [rank, title, release_date, <label>].
    """
    if filter_expr is not None:
        df = df.filter(filter_expr)

    order_fn = F.asc if ascending else F.desc
    window = Window.orderBy(order_fn(F.col(metric_col)))

    return (
        df
        .filter(F.col(metric_col).isNotNull())
        .withColumn("rank", F.rank().over(window))
        .filter(F.col("rank") <= n)
        .select(
            "rank",
            "title",
            "release_date",
            F.col(metric_col).alias(label),
        )
        .orderBy("rank").limit(5)
    )


# ── Display helper ─────────────────────────────────────────────────────────────

def _show(title: str, ranked_df):
    """Log a section header then display the ranked DataFrame."""
    separator = "=" * 60
    logger.info(f"\n{separator}\n  {title}\n{separator}")
    ranked_df.show(truncate=False)


# ── KPI runner ─────────────────────────────────────────────────────────────────

def run_kpi():
    spark = get_spark()
    logger.info("Starting KPI analysis")

    try:
        input_path = os.path.join(os.getcwd(), "data", "processed", "movies_cleaned.parquet")
        logger.info(f"Reading cleaned data from: {input_path}")

        df = spark.read.parquet(input_path)
        logger.info(f"Loaded {df.count()} rows | {len(df.columns)} columns")

        # ── Derived metrics via UDFs ───────────────────────────────────────────
        df = df.withColumn("profit_musd", profit_udf(F.col("revenue_musd"), F.col("budget_musd")))
        df = df.withColumn("roi",         roi_udf(F.col("revenue_musd"),    F.col("budget_musd")))

        # ── 1. Highest Revenue ─────────────────────────────────────────────────
        _show(
            f"Top {TOP_N} — Highest Revenue (M USD)",
            rank_movies(df, "revenue_musd", "revenue_musd"),
        )

        # ── 2. Highest Budget ──────────────────────────────────────────────────
        _show(
            f"Top {TOP_N} — Highest Budget (M USD)",
            rank_movies(df, "budget_musd", "budget_musd"),
        )

        # ── 3. Highest Profit ──────────────────────────────────────────────────
        _show(
            f"Top {TOP_N} — Highest Profit (M USD)",
            rank_movies(df, "profit_musd", "profit_musd"),
        )

        # ── 4. Lowest Profit ───────────────────────────────────────────────────
        _show(
            f"Bottom {TOP_N} — Lowest Profit (M USD)",
            rank_movies(df, "profit_musd", "profit_musd", ascending=True),
        )

        # ── 5. Highest ROI  (budget >= 10 M USD) ──────────────────────────────
        _show(
            f"Top {TOP_N} — Highest ROI  (budget ≥ 10 M USD)",
            rank_movies(
                df, "roi", "roi",
                filter_expr=F.col("budget_musd") >= 10,
            ),
        )

        # ── 6. Lowest ROI  (budget >= 10 M USD) ───────────────────────────────
        _show(
            f"Bottom {TOP_N} — Lowest ROI  (budget ≥ 10 M USD)",
            rank_movies(
                df, "roi", "roi",
                ascending=True,
                filter_expr=F.col("budget_musd") >= 10,
            ),
        )

        # ── 7. Most Voted ──────────────────────────────────────────────────────
        _show(
            f"Top {TOP_N} — Most Voted",
            rank_movies(df, "vote_count", "vote_count"),
        )

        # ── 8. Highest Rated  (vote_count >= 10) ──────────────────────────────
        _show(
            f"Top {TOP_N} — Highest Rated  (vote_count ≥ 10)",
            rank_movies(
                df, "vote_average", "vote_average",
                filter_expr=F.col("vote_count") >= 10,
            ),
        )

        # ── 9. Lowest Rated  (vote_count >= 10) ───────────────────────────────
        _show(
            f"Bottom {TOP_N} — Lowest Rated  (vote_count ≥ 10)",
            rank_movies(
                df, "vote_average", "vote_average",
                ascending=True,
                filter_expr=F.col("vote_count") >= 10,
            ),
        )

        # ── 10. Most Popular ───────────────────────────────────────────────────
        _show(
            f"Top {TOP_N} — Most Popular",
            rank_movies(df, "popularity", "popularity"),
        )

        logger.info("KPI analysis completed")

    except Exception as e:
        logger.error(f"KPI analysis failed: {e}")
        raise

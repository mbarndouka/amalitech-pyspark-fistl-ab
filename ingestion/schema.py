"""
Explicit PySpark schema for the TMDB /movie/{id}?append_to_response=credits response.

Why this exists
---------------
spark.createDataFrame(list_of_dicts) infers types by sampling all rows.
When a string field happens to be None across the entire fetched batch,
Spark has no type evidence and defaults to IntegerType — silently dropping
any real string values that arrive in future batches.

Providing the schema explicitly guarantees StringType for every known string
field regardless of null prevalence, and prevents the downstream cleaning
pipeline from treating those columns as integers.

Field shapes confirmed against the live API (movie 597 / Titanic).
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

MOVIE_SCHEMA = StructType([
    StructField("adult",              BooleanType(), True),
    StructField("backdrop_path",      StringType(),  True),
    StructField("belongs_to_collection", StructType([
        StructField("id",           LongType(),   True),
        StructField("name",         StringType(), True),
        StructField("poster_path",  StringType(), True),
        StructField("backdrop_path",StringType(), True),
    ]), True),
    StructField("budget",            LongType(),   True),
    StructField("genres", ArrayType(StructType([
        StructField("id",   LongType(),   True),
        StructField("name", StringType(), True),
    ])), True),
    StructField("homepage",          StringType(), True),
    StructField("id",                LongType(),   True),
    StructField("imdb_id",           StringType(), True),
    StructField("origin_country",    ArrayType(StringType()), True),
    StructField("original_language", StringType(), True),
    StructField("original_title",    StringType(), True),
    StructField("overview",          StringType(), True),
    StructField("popularity",        DoubleType(), True),
    StructField("poster_path",       StringType(), True),
    StructField("production_companies", ArrayType(StructType([
        StructField("id",             LongType(),   True),
        StructField("logo_path",      StringType(), True),
        StructField("name",           StringType(), True),
        StructField("origin_country", StringType(), True),
    ])), True),
    StructField("production_countries", ArrayType(StructType([
        StructField("iso_3166_1", StringType(), True),
        StructField("name",       StringType(), True),
    ])), True),
    StructField("release_date",  StringType(), True),
    StructField("revenue",       LongType(),   True),
    StructField("runtime",       LongType(),   True),
    StructField("spoken_languages", ArrayType(StructType([
        StructField("english_name", StringType(), True),
        StructField("iso_639_1",    StringType(), True),
        StructField("name",         StringType(), True),
    ])), True),
    StructField("status",    StringType(), True),
    StructField("tagline",   StringType(), True),
    StructField("title",     StringType(), True),
    StructField("video",     BooleanType(), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count",   LongType(),   True),
    # Credits injected by the API client from append_to_response=credits
    StructField("cast_raw", ArrayType(StructType([
        StructField("adult",                BooleanType(),  True),
        StructField("gender",               IntegerType(),  True),
        StructField("id",                   LongType(),     True),
        StructField("known_for_department", StringType(),   True),
        StructField("name",                 StringType(),   True),
        StructField("original_name",        StringType(),   True),
        StructField("popularity",           DoubleType(),   True),
        StructField("profile_path",         StringType(),   True),
        StructField("cast_id",              IntegerType(),  True),
        StructField("character",            StringType(),   True),
        StructField("credit_id",            StringType(),   True),
        StructField("order",                IntegerType(),  True),
    ])), True),
    StructField("crew_raw", ArrayType(StructType([
        StructField("adult",                BooleanType(),  True),
        StructField("gender",               IntegerType(),  True),
        StructField("id",                   LongType(),     True),
        StructField("known_for_department", StringType(),   True),
        StructField("name",                 StringType(),   True),
        StructField("original_name",        StringType(),   True),
        StructField("popularity",           DoubleType(),   True),
        StructField("profile_path",         StringType(),   True),
        StructField("credit_id",            StringType(),   True),
        StructField("department",           StringType(),   True),
        StructField("job",                  StringType(),   True),
    ])), True),
])

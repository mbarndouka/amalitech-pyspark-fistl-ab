import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

import tomli
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def load_toml_config(file_path: str = "config.toml") -> Dict[str, Any]:
    """Load configuration from a TOML file."""
    path = Path(file_path)
    if not path.exists():
        return {}
    with open(path, "rb") as f:
        return tomli.load(f)


class TmdbSettings(BaseSettings):
    """TMDB api client settings."""
    api_key: str = Field(default="", env="TMDB_API_KEY")
    base_url: str = Field(
        default="https://api.themoviedb.org/3/movie",
        description="Base url for TMDB API",
    )
    request_timeout: int = Field(
        default=10,
        description="Request timeout in seconds for TMDB API requests",
    )
    max_retries: int = Field(
        default=3,
        description="Maximum number of retries for TMDB API requests",
        ge=0, le=10,
    )
    rate_limit: int = Field(
        default=40,
        description="Rate limit in requests per period for TMDB API",
    )
    rate_limit_period: int = Field(
        default=10,
        description="Rate limit period in seconds for TMDB API requests",
    )
    max_concurrent: int = Field(
        default=15,
        description="Maximum concurrent TMDB API requests",
    )

    model_config = SettingsConfigDict(env_prefix="TMDB_", env_file=".env", extra="ignore")

    @field_validator("api_key")
    @classmethod
    def api_key_must_be_set(cls, v: str) -> str:
        """Fail fast at startup if the TMDB API key is missing."""
        if not v:
            raise ValueError(
                "TMDB_API_KEY is not set. Export it or add it to .env before running."
            )
        return v


class StorageSettings(BaseSettings):
    """Storage settings."""
    raw_data_path: Path = Field(default=Path("data/raw"), description="Path to the raw data directory")
    processed_data_path: Path = Field(default=Path("data/processed"), description="Path to the processed data directory")
    cache_path: Path = Field(default=Path("data/cache"), description="Path to the cache directory")

    model_config = SettingsConfigDict(env_prefix="STORAGE_", env_file=".env", extra="ignore")

    def ensure_paths_exist(self):
        """Ensure that the specified paths exist, if not create them."""
        for path in [self.raw_data_path, self.processed_data_path, self.cache_path]:
            if not path.exists():
                path.mkdir(parents=True, exist_ok=True)


class SparkSettings(BaseSettings):
    """PySpark session settings."""
    app_name: str = Field(default="TMDB Data Processing", description="Name of the Spark application")
    master: str = Field(default="local[*]", description="Master URL for Spark")
    executor_memory: str = Field(default="2g", description="Memory allocation for Spark executors")
    driver_memory: str = Field(default="2g", description="Memory allocation for Spark driver")
    sql_shuffle_partitions: int = Field(default=10, description="Number of partitions for Spark SQL shuffle")
    spark_log_level: str = Field(default="WARN", description="Log level for Spark")

    model_config = SettingsConfigDict(env_prefix="SPARK_", env_file=".env", extra="ignore")

    @field_validator("spark_log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        """Validate the Spark log level."""
        valid_levels = ["OFF", "FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"]
        if value.upper() not in valid_levels:
            raise ValueError(f"Invalid Spark log level: {value}. Must be one of {valid_levels}")
        return value.upper()


class PipelineSettings(BaseSettings):
    """Pipeline execution settings."""
    env: str = Field(default="dev", description="Environment for the pipeline")
    batch_size: int = Field(default=10, description="Batch size for processing data")
    num_workers: int = Field(default=4, description="Number of worker threads for data processing")
    logging_level: str = Field(default="INFO", description="Logging level for the pipeline")
    logging_format: str = Field(default="json", description="Logging format")
    movie_ids: list[int] = Field(
        default=[
            299534, 19995, 140607, 299536, 597, 135397, 420818,
            24428, 168259, 99861, 284054, 12445, 181808, 330457,
            351286, 109445, 321612, 260513,
        ],
        description="List of TMDB movie IDs to ingest",
    )

    model_config = SettingsConfigDict(env_prefix="PIPELINE_", env_file=".env", extra="ignore")

    @field_validator("env")
    @classmethod
    def validate_env(cls, value: str) -> str:
        """Validate the environment."""
        valid_envs = ["dev", "prod", "development"]
        if value.lower() not in valid_envs:
            raise ValueError(f"Invalid environment: {value}. Must be one of {valid_envs}")
        return "dev" if value.lower() == "development" else value.lower()


class VizSettings(BaseSettings):
    """Visualization palette and layout settings (read from config.toml [visualization])."""
    palette:          list[str] = Field(default=["#E74C3C", "#3498DB", "#2ECC71", "#F39C12", "#9B59B6"])
    franchise_color:  str       = Field(default="#3498DB")
    standalone_color: str       = Field(default="#E74C3C")
    background_color: str       = Field(default="#1A1A2E")
    surface_color:    str       = Field(default="#16213E")
    grid_color:       str       = Field(default="#2D2D44")
    text_color:       str       = Field(default="#EAEAEA")
    muted_color:      str       = Field(default="#8899AA")
    fig_dpi:          int       = Field(default=150)
    fig_width:        int       = Field(default=14)
    fig_height:       int       = Field(default=8)
    output_dir:       str       = Field(default="data/plots")

    model_config = SettingsConfigDict(env_prefix="VIZ_", env_file=".env", extra="ignore")


class Settings(BaseSettings):
    """Application settings."""
    tmdb:          TmdbSettings
    storage:       StorageSettings
    spark:         SparkSettings
    pipeline:      PipelineSettings
    visualization: VizSettings

    model_config = SettingsConfigDict(env_prefix="APP_", env_file=".env", extra="ignore")

    @classmethod
    def from_toml(cls, file_path: str = "config.toml") -> "Settings":
        """Create settings from a TOML file merged with environment variables."""
        toml_data = load_toml_config(file_path)

        return cls(
            tmdb=TmdbSettings(**toml_data.get("tmdb", {})),
            storage=StorageSettings(**toml_data.get("storage", {})),
            spark=SparkSettings(**toml_data.get("spark", {})),
            pipeline=PipelineSettings(**toml_data.get("pipeline", {})),
            visualization=VizSettings(**toml_data.get("visualization", {})),
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Get the application settings."""
    try:
        return Settings.from_toml()
    except (FileNotFoundError, IOError):
        return Settings(
            tmdb=TmdbSettings(),
            storage=StorageSettings(),
            spark=SparkSettings(),
            pipeline=PipelineSettings(),
            visualization=VizSettings(),
        )

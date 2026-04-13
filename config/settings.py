from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class tmdbSettings(BaseSettings):
    """TMDB api client settings."""
    api_key: str = Field(..., env="TMDB_API_KEY")
    baseUrl: str = Field(
        default="https://api.themoviedb.org/3",
        description="Base url for TMDB API",
    )

    request_timeout: int = Field(
        default=10,
        description="Request timeout in seconds for TMDB API requests",
    )
    max_retries: int = Field(
        default=3,
        description="Maximum number of retries for TMDB API requests",
        ge=0, le=10
    )
    rate_limit: int = Field(
        default=40,
        description="Rate limit in seconds for TMDB API requests",
    )
    rate_limit_period: int = Field(default=10, description="Rate limit periode in seconds for TMDB API requests")
    model_config = SettingsConfigDict(env_prefix="TMDB_",env_file=".env", extra="ignore")

class storageSettings(BaseSettings):
    """Storage settings."""
    raw_data_path: Path = Field(default=Path("data/raw"), description="Path to the raw data directory")
    processed_data_path: Path = Field(default=Path("data/processed"), description="Path to the processed data directory")
    cache_path: Path = Field(default=Path("data/cache"), description="Path to the cache directory")

    model_config = SettingsConfigDict(env_prefix="STORAGE_",env_file=".env", extra="ignore")

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

    model_config = SettingsConfigDict(env_prefix="SPARK_",env_file=".env", extra="ignore")

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

    model_config = SettingsConfigDict(env_prefix="PIPELINE_",env_file=".env", extra="ignore")

    @field_validator("env")
    @classmethod
    def validate_env(cls, value: str) -> str:
        """Validate the environment."""
        valid_envs = ["dev", "prod", "development"]
        if value.lower() not in valid_envs:
            raise ValueError(f"Invalid environment: {value}. Must be one of {valid_envs}")
        return "dev" if value.lower() == "development" else value.lower()

class Settings(BaseSettings):
    """Application settings."""
    tmdb: tmdbSettings = tmdbSettings()
    storage: storageSettings = storageSettings()
    spark: SparkSettings = SparkSettings()
    pipeline: PipelineSettings = PipelineSettings()

    model_config = SettingsConfigDict(env_prefix="APP_",env_file=".env", extra="ignore")

@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Get the application settings."""
    return Settings()
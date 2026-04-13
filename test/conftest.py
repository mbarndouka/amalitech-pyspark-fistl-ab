import sys
from pathlib import Path

# Add project root to sys.path to allow imports like 'config.spark_config'
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from config.spark_config import build_spark_session, close_spark

@pytest.fixture(scope="session")
def spark_session():
    session = build_spark_session()
    yield session
    close_spark()
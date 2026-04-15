"""
Shared utility for writing a PySpark DataFrame to a single parquet file on Windows.

Why this exists
---------------
Spark's native parquet writer uses Hadoop's output committer, which requires
winutils.exe on Windows. Without it every write silently produces an empty
directory. The standard workaround — toPandas().to_parquet() — is blocked
on this machine by an Application Control policy on pandas' compiled DLL.

This helper collects the DataFrame to the driver, converts each Row to a
plain Python dict (recursive=True unwraps nested Row objects into dicts),
then writes with PyArrow using the Spark-derived arrow schema so that every
column keeps its correct type even when all values in a batch are null.
"""

import pathlib
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql.pandas.types import to_arrow_schema

from utils.logger import get_logger

logger = get_logger(__name__)


def write_parquet(df, output_path) -> None:
    """Collect *df* and write it to *output_path* as a single parquet file.

    Args:
        df:           PySpark DataFrame to write.
        output_path:  Destination path (str or pathlib.Path).
                      Any existing file or directory at this path is removed first.
    """
    output_path = pathlib.Path(output_path)

    # Remove prior output — a previous Spark native-write attempt may have left
    # an empty directory that would cause pq.write_table to fail.
    if output_path.exists():
        if output_path.is_dir():
            shutil.rmtree(output_path)
        else:
            output_path.unlink()

    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Collecting DataFrame for parquet write → {output_path}")
    rows = df.collect()

    # Row.asDict(recursive=True) converts nested Row / list-of-Row structures
    # into plain Python dicts so PyArrow can serialise them correctly.
    records = [row.asDict(recursive=True) for row in rows]

    # to_arrow_schema converts the Spark StructType to a PyArrow Schema.
    # Passing this explicitly prevents PyArrow from re-inferring column types
    # from the data (which would mistype all-null string columns as int64).
    arrow_schema = to_arrow_schema(df.schema)
    arrow_table  = pa.Table.from_pylist(records, schema=arrow_schema)

    pq.write_table(arrow_table, str(output_path))
    logger.info(f"Parquet written: {len(records)} rows → {output_path}")

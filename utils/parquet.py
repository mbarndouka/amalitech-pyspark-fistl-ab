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

Atomicity
---------
The write goes to a sibling .parquet.tmp file first. Only after a successful
write is the .tmp renamed over the final path. A crash mid-write leaves the
original file intact; the stale .tmp is cleaned up on the next run.
"""

import pathlib
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql.pandas.types import to_arrow_schema

from utils.logger import get_logger

logger = get_logger(__name__)


def write_parquet(df, output_path) -> None:
    """Collect *df* and write it atomically to *output_path* as a single parquet file.

    Args:
        df:           PySpark DataFrame to write.
        output_path:  Destination path (str or pathlib.Path).
                      The write is atomic: the destination is only replaced
                      after the new file has been fully written to a .tmp sibling.
    """
    output_path = pathlib.Path(output_path)
    tmp_path = output_path.with_suffix(".parquet.tmp")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Remove any stale .tmp left by a previous crashed run
    if tmp_path.exists():
        tmp_path.unlink()

    logger.info(f"Collecting DataFrame for parquet write → {output_path}")
    rows = df.collect()

    # Row.asDict(recursive=True) converts nested Row / list-of-Row structures
    # into plain Python dicts so PyArrow can serialise them correctly.
    records = [row.asDict(recursive=True) for row in rows]

    # to_arrow_schema converts the Spark StructType to a PyArrow Schema.
    # Passing this explicitly prevents PyArrow from re-inferring column types
    # from the data (which would mistype all-null string columns as int64).
    arrow_schema = to_arrow_schema(df.schema)
    arrow_table = pa.Table.from_pylist(records, schema=arrow_schema)

    try:
        pq.write_table(arrow_table, str(tmp_path))
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise

    # Atomic swap: remove old destination, then rename tmp → final
    if output_path.exists():
        if output_path.is_dir():
            shutil.rmtree(output_path)
        else:
            output_path.unlink()

    tmp_path.replace(output_path)
    logger.info(f"Parquet written: {len(records)} rows → {output_path}")


def spark_df_to_dict(df) -> dict:
    """Convert a PySpark DataFrame to a plain Python dict of lists (column → values).

    Uses the same collect + PyArrow path as write_parquet so that type handling
    (all-null string columns, nested structs, date types) is consistent.
    Intended for passing DataFrames directly to non-Spark consumers such as
    the visualization layer without writing an intermediate parquet file.
    """
    rows = df.collect()
    records = [row.asDict(recursive=True) for row in rows]
    arrow_schema = to_arrow_schema(df.schema)
    arrow_table = pa.Table.from_pylist(records, schema=arrow_schema)
    return arrow_table.to_pydict()

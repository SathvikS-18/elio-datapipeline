import pytest
from pyspark.sql import SparkSession
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pipeline.utils.transforms import (
    standardise_strings, add_surrogate_key, add_audit_columns,
    deduplicate, null_safe_trim, validate_not_null
)

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("test").getOrCreate()

def test_standardise_strings(spark):
    df = spark.createDataFrame([(" Hello ",), ("WORLD",)], ["name"])
    result = standardise_strings(df, ["name"])
    values = [row.name for row in result.collect()]
    assert values == ["hello", "world"]

def test_add_surrogate_key(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    result = add_surrogate_key(df, ["id", "name"], "sk")
    assert "sk" in result.columns
    sks = [row.sk for row in result.collect()]
    assert len(set(sks)) == 2  # unique keys
    assert all(len(sk) == 32 for sk in sks)  # md5 is 32 hex chars

def test_add_audit_columns(spark):
    df = spark.createDataFrame([(1,)], ["id"])
    result = add_audit_columns(df)
    assert "_loaded_at" in result.columns
    assert "_source" in result.columns

def test_deduplicate(spark):
    df = spark.createDataFrame(
        [(1, "2024-01-01"), (1, "2024-01-02"), (2, "2024-01-01")],
        ["id", "ts"]
    )
    result = deduplicate(df, ["id"], "ts", ascending=False)
    assert result.count() == 2
    # Should keep latest ts for id=1
    row = result.filter("id = 1").first()
    assert row.ts == "2024-01-02"

def test_null_safe_trim(spark):
    df = spark.createDataFrame([(" hello ",), ("",), (None,)], ["name"])
    result = null_safe_trim(df, ["name"])
    values = [row.name for row in result.collect()]
    assert values == ["hello", None, None]

def test_validate_not_null(spark):
    df = spark.createDataFrame([(1,), (None,), (3,)], ["id"])
    valid, invalid = validate_not_null(df, ["id"])
    assert valid.count() == 2
    assert invalid.count() == 1

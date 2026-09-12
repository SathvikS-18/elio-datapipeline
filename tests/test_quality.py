import pytest
from pyspark.sql import SparkSession
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pipeline.utils.quality import (
    check_null_rate, check_unique, check_row_count, check_referential_integrity
)

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("test_quality").getOrCreate()

def test_check_null_rate_passes(spark):
    df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    passed, rate = check_null_rate(df, "id", threshold=0.05)
    assert passed is True
    assert rate == 0.0

def test_check_null_rate_fails(spark):
    df = spark.createDataFrame([(1,), (None,), (None,)], ["id"])
    passed, rate = check_null_rate(df, "id", threshold=0.05)
    assert passed is False
    assert abs(rate - 0.6667) < 0.01

def test_check_unique_passes(spark):
    df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    passed, dupes = check_unique(df, ["id"])
    assert passed is True
    assert dupes == 0

def test_check_unique_fails(spark):
    df = spark.createDataFrame([(1,), (1,), (2,)], ["id"])
    passed, dupes = check_unique(df, ["id"])
    assert passed is False
    assert dupes == 1

def test_check_row_count_passes(spark):
    df = spark.createDataFrame([(1,), (2,)], ["id"])
    passed, count = check_row_count(df, min_rows=1)
    assert passed is True
    assert count == 2

def test_check_row_count_fails(spark):
    df = spark.createDataFrame([], "id: int")
    passed, count = check_row_count(df, min_rows=1)
    assert passed is False
    assert count == 0

def test_check_referential_integrity_passes(spark):
    fact = spark.createDataFrame([(1,), (2,)], ["fk_id"])
    dim = spark.createDataFrame([(1,), (2,), (3,)], ["fk_id"])
    passed, orphans = check_referential_integrity(fact, dim, "fk_id")
    assert passed is True
    assert orphans == 0

def test_check_referential_integrity_fails(spark):
    fact = spark.createDataFrame([(1,), (2,), (99,)], ["fk_id"])
    dim = spark.createDataFrame([(1,), (2,)], ["fk_id"])
    passed, orphans = check_referential_integrity(fact, dim, "fk_id")
    assert passed is False
    assert orphans == 1

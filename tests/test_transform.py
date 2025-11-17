import pytest
from pyspark.sql import SparkSession
from spark_job.app import transform

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("unit-test")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_transform_correct_mapping(spark):
    # Input sample matching the assignment
    data = [
        (1, "notification", "true",       1546333200),
        (3, "refresh",      "denied",     1546334200),
        (2, "background",   "notDetermined", 1546333611),
        (3, "refresh",      "4",          1546333443),
        (1, "notification", "false",      1546335647),
        (1, "background",   "true",       1546333546),
    ]

    df = spark.createDataFrame(
        data,
        ["id", "name", "value", "timestamp"]
    )

    result = transform(df)

    collected = {row["id"]: row["settings"] for row in result.collect()}

    assert collected[1] == {"notification": "false", "background": "true"}
    assert collected[2] == {"background": "notDetermined"}
    assert collected[3] == {"refresh": "denied"}


def test_transform_picks_highest_timestamp(spark):
    data = [
        (10, "abc", "old",  1),
        (10, "abc", "new",  2),
    ]

    df = spark.createDataFrame(data, ["id", "name", "value", "timestamp"])
    result = transform(df).collect()[0]

    assert result.settings == {"abc": "new"}

# tests/conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """테스트 전체 세션에서 공유할 Spark 세션 생성"""
    spark_session = (SparkSession.builder
                     .master("local[*]")
                     .appName("pytest-pyspark-local")
                     .getOrCreate())
    yield spark_session
    spark_session.stop()
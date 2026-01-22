import pytest
from pyspark.sql import SparkSession
import os

@pytest.fixture(scope="module")
def spark():
    """테스트용 Spark 세션 생성"""
    session = (SparkSession.builder
               .master("local[*]")
               .appName("ParquetZstdTest")
               # Zstd 관련 설정을 명시적으로 확인하기 위해 추가
              #  .config("spark.sql.parquet.compression.codec", "zstd")
               .getOrCreate())
    yield session
    session.stop()

def test_spark_read_zstd_parquet(spark):
    file_path = "output/sample_zstd.parquet/part-00002-c1a3a266-38eb-49e0-a78d-2665ea12f336-c000.zstd.parquet"
    
    # 1. 파일 존재 확인
    print(f"\n[Step 1] Path existence check: {file_path}")
    assert os.path.exists(file_path), "파일이 프로젝트 폴더에 없습니다."

    # 2. Spark로 Parquet 읽기
    print(f"[Step 2] Reading parquet via Spark...")
    df = spark.read.parquet(file_path)

    # 3. 스키마 및 데이터 확인
    print("[Step 3] Data Schema:")
    df.printSchema()
    
    data_count = df.count()
    print(f"[Step 4] Row Count: {data_count}")
    
    print("[Step 5] Data Show:")
    df.show(5)

    # 검증: 데이터가 존재해야 함
    assert data_count > 0
  
def test_spark_read_snappy_parquet(spark):
    file_path = "sample_snappy.parquet/part-00000-001fc403-939e-4af2-9c25-c632e5fb2eca-c000.snappy.parquet"
    
    # 1. 파일 존재 확인
    print(f"\n[Step 1] Path existence check: {file_path}")
    assert os.path.exists(file_path), "파일이 프로젝트 폴더에 없습니다."

    # 2. Spark로 Parquet 읽기
    print(f"[Step 2] Reading parquet via Spark...")
    df = spark.read.parquet(file_path)

    # 3. 스키마 및 데이터 확인
    print("[Step 3] Data Schema:")
    df.printSchema()
    
    data_count = df.count()
    print(f"[Step 4] Row Count: {data_count}")
    
    print("[Step 5] Data Show:")
    df.show(5)

    # 검증: 데이터가 존재해야 함
    # assert data_count > 0
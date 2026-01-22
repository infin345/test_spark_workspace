from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os

def run_exam(file_path:str):
    # 1. Spark 세션 생성
    spark = SparkSession.builder \
        .appName("ZstdParquetExample") \
        .master("local[*]") \
        .getOrCreate()
    # file_path = "no_spark_arrow_zstd.parquet"
    
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

def run_zstd_example():
    # 1. Spark 세션 생성
    spark = SparkSession.builder \
        .appName("ZstdParquetExample") \
        .master("local[*]") \
        .getOrCreate()

    # 2. 샘플 데이터 생성
    data = [
        ("Seo Hyo-jun", 35, "Backend Developer"),
        ("Gemini", 25, "AI Assistant"),
        ("Spark", 10, "Data Engine")
    ]
    
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("job", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema)

    # 3. Zstd 압축을 사용하여 Parquet 파일로 저장
    # .option("compression", "zstd")가 핵심입니다.
    output_path = "output/sample_zstd.parquet"
    print(f"--- '{output_path}'에 Zstd 압축으로 저장 중 ---")
    
    df.write.mode("overwrite") \
        .option("compression", "zstd") \
        .parquet(output_path)

    # 4. 저장된 파일 다시 읽기
    print(f"--- '{output_path}' 읽기 시도 ---")
    read_df = spark.read.parquet(output_path)
    
    # 5. 결과 및 스키마 출력
    read_df.show()
    read_df.printSchema()

    spark.stop()


def create_snappy_parquet():
    # 1. Spark 세션 생성
    spark = SparkSession.builder \
        .appName("SnappyVerification") \
        .master("local[*]") \
        .getOrCreate()

    # 2. 샘플 데이터 준비
    data_dict = {
        'name': ['Seo Hyo-jun', 'Gemini', 'Spark'],
        'age': [35, 25, 10],
        'job': ['Backend Developer', 'AI Assistant', 'Data Engine']
    }
    df_pd = pd.DataFrame(data_dict)
    
    # 3. Spark DataFrame으로 변환 및 Snappy로 저장
    file_snappy = "sample_snappy.parquet"
    df_spark = spark.createDataFrame(df_pd)
    
    print(f"\n[Step 1] Saving: {file_snappy} (Compression: Snappy)")
    df_spark.write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(file_snappy)
    print("✅ Save Completed!")

    # 4. 저장된 Snappy 파일 다시 읽기 및 count() 수행
    try:
        print(f"\n[Step 2] Testing 'count()' on {file_snappy}...")
        read_df = spark.read.parquet(file_snappy)
        
        # 동료분이 에러 났던 바로 그 지점입니다.
        cnt = read_df.count() 
        
        print(f"✅ Success! Row count: {cnt}")
        read_df.show()
    except Exception as e:
        print(f"❌ Error during count(): {e}")

    spark.stop()
    
if __name__ == "__main__":
    # run_zstd_example()
    run_exam("no_spark_arrow_zstd.parquet_")
    # run_exam("no_spark_arrow_zstd.parquet")
    create_snappy_parquet()
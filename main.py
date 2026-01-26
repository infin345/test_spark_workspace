import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def get_spark_session(app_name="SparkExample") -> SparkSession:
    """Spark 세션을 생성하거나 기존 세션을 반환합니다."""
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()

def run_exam(spark: SparkSession, file_path: str):
    # 1. 파일 존재 확인
    print(f"\n[Step 1] Path existence check: {file_path}")
    if not os.path.exists(file_path):
        print(f"⚠️  파일이 존재하지 않습니다: {file_path}")
        return

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
    assert data_count > 0, "데이터 건수가 0입니다."

def run_zstd_example(spark: SparkSession):
    # 1. 샘플 데이터 생성
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

    # 2. Zstd 압축을 사용하여 Parquet 파일로 저장
    output_path = "output/sample_zstd.parquet"
    print(f"\n--- '{output_path}'에 Zstd 압축으로 저장 중 ---")
    
    df.write.mode("overwrite") \
        .option("compression", "zstd") \
        .parquet(output_path)

    # 3. 저장된 파일 다시 읽기
    print(f"--- '{output_path}' 읽기 시도 ---")
    read_df = spark.read.parquet(output_path)
    
    # 4. 결과 및 스키마 출력
    read_df.show()
    read_df.printSchema()

def create_snappy_parquet(spark: SparkSession):
    # 1. 샘플 데이터 준비
    data_dict = {
        'name': ['Seo Hyo-jun', 'Gemini', 'Spark'],
        'age': [35, 25, 10],
        'job': ['Backend Developer', 'AI Assistant', 'Data Engine']
    }
    df_pd = pd.DataFrame(data_dict)
    
    # 2. Spark DataFrame으로 변환 및 Snappy로 저장
    file_snappy = "sample_snappy.parquet"
    df_spark = spark.createDataFrame(df_pd)
    
    print(f"\n[Step 1] Saving: {file_snappy} (Compression: Snappy)")
    df_spark.write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(file_snappy)
    print("✅ Save Completed!")

    # 3. 저장된 Snappy 파일 다시 읽기 및 count() 수행
    try:
        print(f"\n[Step 2] Testing 'count()' on {file_snappy}...")
        read_df = spark.read.parquet(file_snappy)
        
        # 동료분이 에러 났던 바로 그 지점입니다.
        cnt = read_df.count() 
        
        print(f"✅ Success! Row count: {cnt}")
        read_df.show()
    except Exception as e:
        print(f"❌ Error during count(): {e}")

def run_excel_example(spark: SparkSession):
    # 1. 엑셀 파일 생성 (테스트용)
    excel_path = "sample_excel.xlsx"
    print(f"\n[Step 1] Creating dummy Excel file: {excel_path}")
    
    data = {
        "Product": ["Laptop", "Mouse", "Monitor"],
        "Price": [1200, 25, 300],
        "Stock": [50, 200, 30]
    }
    pdf = pd.DataFrame(data)
    pdf.to_excel(excel_path, index=False)
    
    # 2. Pandas로 엑셀 읽기
    # Spark에는 엑셀 리더가 내장되어 있지 않아, 보통 Pandas로 읽고 변경합니다.
    print(f"[Step 2] Reading Excel via Pandas & Converting to Spark...")
    pdf_loaded = pd.read_excel(excel_path)
    
    # 3. Spark DataFrame으로 변환
    df_spark = spark.createDataFrame(pdf_loaded)
    
    # 4. 결과 출력
    print("[Step 3] Spark DataFrame from Excel:")
    df_spark.printSchema()
    df_spark.show()

if __name__ == "__main__":
    # Spark 세션은 프로그램 실행 시 한 번만 생성
    spark = get_spark_session("RefactoredSparkApp")
    
    try:
        # run_zstd_example(spark)
        
        # 파일명을 올바르게 수정 ("_" 제거)
        # target_file = "no_spark_arrow_zstd.parquet"
        # run_exam(spark, target_file)
        
        # create_snappy_parquet(spark)
        
        run_excel_example(spark)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # 프로그램 종료 시 세션 정리
        spark.stop()
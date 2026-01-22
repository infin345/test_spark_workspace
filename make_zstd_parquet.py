import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# 1. 샘플 데이터 생성 (Pandas DataFrame)
data = {
    'name': ['Seo Hyo-jun', 'Gemini', 'Spark'],
    'age': [35, 25, 10],
    'job': ['Backend Developer', 'AI Assistant', 'Data Engine']
}
df = pd.DataFrame(data)

# 2. 저장 경로 설정
file_name = "no_spark_zstd.parquet"

# 방법 A: Pandas의 to_parquet 사용 (가장 간단함)
df.to_parquet(
    file_name, 
    engine='pyarrow', 
    compression='zstd',  # 압축 코덱 지정
    index=False
)

print(f"--- '{file_name}' 저장 완료 (Pandas 이용) ---")

# 방법 B: PyArrow로 직접 저장 (메타데이터를 더 세밀하게 조정할 때)
table = pa.Table.from_pandas(df)
pq.write_table(table, "no_spark_arrow_zstd.parquet", compression='zstd')

print(f"--- 'no_spark_arrow_zstd.parquet' 저장 완료 (PyArrow 이용) ---")
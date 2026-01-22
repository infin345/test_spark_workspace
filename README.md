docker compose run --rm spark-test pytest -s tests/test_parquet.py

parquet-tools inspect part-00007-001fc403-939e-4af2-9c25-c632e5fb2eca-c000.snappy.parquet

docker compose run --rm spark-test python main.py
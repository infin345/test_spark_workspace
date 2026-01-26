docker compose run --rm spark-test pytest -s tests/test_parquet.py

parquet-tools inspect sample_snappy.parquet/part-00007-001fc403-939e-4af2-9c25-c632e5fb2eca-c000.snappy.parquet
parquet-tools csv  sample_snappy.parquet/part-00007-001fc403-939e-4af2-9c25-c632e5fb2eca-c000.snappy.parquet
parquet-tools show  sample_snappy.parquet/part-00007-001fc403-939e-4af2-9c25-c632e5fb2eca-c000.snappy.parquet

docker compose run --rm spark-test python main.pyg


Excel Support and Docker Setup Walkthrough
This document summarizes the changes made to enable Excel file processing in the Spark application and fix the Docker environment.

Changes
1. Code Changes
main.py
:
Refactored code to unify SparkSession management.
Added 
run_excel_example
 function to demonstrate reading Excel files using pandas and converting them to Spark DataFrames.
requirements.txt
:
Added openpyxl dependency to support Excel file reading with pandas.
2. Infrastructure Changes
Dockerfile
:
Updated Spark download URL to archive.apache.org to ensure reliable downloading of version 3.5.0.
Verification Results
Docker Execution
The application was successfully run inside a Docker container.

Command:

docker compose run spark-test python main.py
Output:

[Step 1] Creating dummy Excel file: sample_excel.xlsx
[Step 2] Reading Excel via Pandas & Converting to Spark...
[Step 3] Spark DataFrame from Excel:
root
 |-- Product: string (nullable = true)
 |-- Price: long (nullable = true)
 |-- Stock: long (nullable = true)
+-------+-----+-----+
|Product|Price|Stock|
+-------+-----+-----+
| Laptop| 1200|   50|
|  Mouse|   25|  200|
|Monitor|  300|   30|
+-------+-----+-----+
Next Steps
You can now add your own Excel files to the project and read them using the pattern shown in 
run_excel_example
.
FROM python:3.11-slim-bullseye

# 1. Java 및 필수 라이브러리 설치 (오타 수정: --no-install-recommends)
RUN apt-get update && apt-get install -y --no-install-recommends \
  openjdk-17-jdk \
  curl \
  libzstd1 \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

# 2. Spark 환경 변수 설정
ENV SPARK_VERSION=3.5.0
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# 3. Spark 다운로드 및 설치
# archive.apache.org 대신 dlcdn.apache.org(최신 미러)를 시도하고, 안되면 archive를 씁니다.
RUN curl -L --retry 5 "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" -o /tmp/spark.tgz && \
  mkdir -p ${SPARK_HOME} && \
  tar -xzf /tmp/spark.tgz -C ${SPARK_HOME} --strip-components=1 && \
  rm /tmp/spark.tgz

# 4. 라이브러리 설치
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. 소스 복사
COPY . .

CMD ["pytest"]
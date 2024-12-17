from fastapi import HTTPException, status
from prometheus_client import Gauge
import requests

import pyspark

def stream_parquet_rows(spark: pyspark.sql.SparkSession, file_path, METRICS_URL: str, username: str):
        size_in_bytes = 0
        try:
            # Используем итератор по частям данных для чтения и отправки
            df = spark.read.parquet(file_path)
            size_in_bytes = df.rdd.map(lambda row: len(str(row))).sum()
            for row in df.rdd.toLocalIterator():  # Прямой доступ к RDD
                yield row.asDict().__str__().encode("utf-8") + b"\n"  # Преобразуем в строку JSON
        except Exception as e:
            print(f"Ошибка при чтении или передаче данных: {str(e)}")
            raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Ошибка при чтении или передаче данных: {str(e)}"
                )
        finally:
            # Завершение SparkSession после передачи данных
            # active_requests_gauge.dec()
            try:
                requests.post(f"{METRICS_URL}/received_historical_data", json={"value": size_in_bytes})
                requests.post(f"{METRICS_URL}/active_requests", json={"value": -1})
                requests.delete(f"{METRICS_URL}/active_users", json={"value": username})
            except Exception:
                print("stream_parquet_rows: METRICS SERVER NOT READY")
            spark.stop()
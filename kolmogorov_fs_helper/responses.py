from fastapi import HTTPException, status
from prometheus_client import Gauge

import pyspark

def stream_parquet_rows(spark: pyspark.sql.SparkSession, file_path, active_requests_gauge: Gauge):
        try:
            # Используем итератор по частям данных для чтения и отправки
            df = spark.read.parquet(file_path)
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
            active_requests_gauge.dec()
            spark.stop()
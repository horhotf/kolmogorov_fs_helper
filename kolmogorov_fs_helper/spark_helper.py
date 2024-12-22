from threading import Lock
import psycopg2
from .db import db_status_uid
from fastapi import status

def stop_spark_task(uid: str, active_tasks:dict, task_lock: Lock, psycopg2_connection_pool: psycopg2.pool.SimpleConnectionPool):
    with task_lock:
        if uid in active_tasks:
            spark_session = active_tasks.pop(uid, None)
            if spark_session:
                try:
                    spark_session.stop()
                    db_status_uid(psycopg2_connection_pool, uid = uid, func="STOPED")
                    return {"status": "STOPPED", "uid": uid}, status.HTTP_200_OK
                except Exception as e:
                    return {"status": "ERROR", "message": str(e), "uid": uid}, status.HTTP_500_INTERNAL_SERVER_ERROR
        return {"status": "NOT_FOUND", "uid": uid}, status.HTTP_404_NOT_FOUND
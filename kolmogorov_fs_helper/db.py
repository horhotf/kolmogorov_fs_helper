from psycopg2.pool import SimpleConnectionPool

import re

from fastapi import HTTPException, status

def init_db_uid(connection_pool: SimpleConnectionPool):
    conn = connection_pool.getconn()
    cur = conn.cursor()

    query_init = """
    CREATE TABLE IF NOT EXISTS uid_status (
        uid TEXT,
        status TEXT,
        feature_view TEXT,
        version INTEGER,
        fields TEXT,
        filtred_params TEXT,
        file TEXT,
        update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        logs TEXT
    );
    """

    cur.execute(query_init)
    conn.commit()

    cur.close()
    connection_pool.putconn(conn)

def db_check_fv(connection_pool:SimpleConnectionPool, table_name:str, version: int, fields:str = None, filtred_params:str = None):
    conn = connection_pool.getconn()
    cur = conn.cursor()

    query_check_fv = f"""
            SELECT EXISTS(SELECT 1 FROM uid_status 
            WHERE feature_view = '{table_name}' and version = '{version}'
        """
    if fields != None:
        query_check_fv = query_check_fv + f" and fields = '{fields}'"
    
    if filtred_params != None:
        query_check_fv = query_check_fv + f" and filtred_params = '{filtred_params}'"
    
    query_check_fv = query_check_fv + ");"

    cur.execute(query_check_fv)
    check = cur.fetchone()[0]

    if check:
        query_get_file = f"""
            SELECT file, status FROM uid_status 
            WHERE feature_view = '{table_name}' and version = '{version}'
        """
        if fields != None:
            query_get_file = query_get_file + f" and fields = '{fields}'"
        
        if filtred_params != None:
            query_get_file = query_get_file + f" and filtred_params = '{filtred_params}'"
        
        query_get_file = query_get_file + ";"

        cur.execute(query_get_file)
        response = cur.fetchone()

        cur.close()
        connection_pool.putconn(conn)
        return response[0], response[1]
    
    cur.close()
    connection_pool.putconn(conn)
    return 0, 0

def db_status_uid(
    connection_pool:SimpleConnectionPool, 
    func:str, 
    version:int = None,
    uid:str = None, 
    file:str = None, 
    feature_view:str = None, 
    fields:str = None, 
    filtred_params:str = None,
    logs:str = None
):
    try:
        conn = connection_pool.getconn()
        cur = conn.cursor()


        if func == "ADD":
            query_add = f"""
                INSERT INTO uid_status (uid, status, feature_view, version, fields, filtred_params, file, update_timestamp) 
                VALUES ('{uid}', 'CREATE', '{feature_view}', '{version}', '{fields}', '{filtred_params}', 'None', CURRENT_TIMESTAMP);
            """

            cur.execute(query_add)
            conn.commit()
            connection_pool.putconn(conn)
            return 0
    except Exception as e:
        print(str(e))

    query_check_uid = f"""
        SELECT EXISTS(SELECT 1 FROM uid_status WHERE uid = '{uid}');
    """

    cur.execute(query_check_uid)
    if not cur.fetchone()[0]:
        connection_pool.putconn(conn)
        raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="uid not found"
                )

    if func == "UPDATE":
        query_update = f"""
            UPDATE uid_status
            SET status = 'COMPLETE', update_timestamp = CURRENT_TIMESTAMP, file = '{file}'
            WHERE uid = '{uid}';
        """

        cur.execute(query_update)
        conn.commit()
        connection_pool.putconn(conn)

        return 0

    if func == "FAILED":
        logs = re.sub(r'[\'"]', '', logs)
        logs = re.sub(r'\s+', ' ', logs)

        query_failed = f"""
            UPDATE uid_status
            SET status = 'FAILED', update_timestamp = CURRENT_TIMESTAMP, logs = '{logs}'
            WHERE uid = '{uid}';
        """

        cur.execute(query_failed)
        conn.commit()
        connection_pool.putconn(conn)

        return 0
    
    if func == "STOPED":
        query_stoped = f"""
            UPDATE uid_status
            SET status = 'STOPED', update_timestamp = CURRENT_TIMESTAMP
            WHERE uid = '{uid}';
        """

        cur.execute(query_stoped)
        conn.commit()
        connection_pool.putconn(conn)

        return 0

    if func == "DELETED":
        query_deleted = f"""
            UPDATE uid_status
            SET status = 'DELETED', update_timestamp = CURRENT_TIMESTAMP
            WHERE file = '{file}';
        """

        cur.execute(query_deleted)
        conn.commit()
        connection_pool.putconn(conn)

        return 0

    if func == "GET_STATUS":
        query_get_status = f"""
            SELECT status from uid_status
            WHERE uid = '{uid}';
        """
        cur.execute(query_get_status)
        connection_pool.putconn(conn)
        return cur.fetchone()[0]


    if func == "GET_FILE":
        query_get_file = f"""
            SELECT file from uid_status
            WHERE uid = '{uid}';
        """
        cur.execute(query_get_file)
        connection_pool.putconn(conn)
        return cur.fetchone()[0]

    if func == "GET_LOGS":
        query_get_file = f"""
            SELECT logs from uid_status
            WHERE uid = '{uid}';
        """
        cur.execute(query_get_file)
        connection_pool.putconn(conn)
        return cur.fetchone()[0]

    cur.close()
    connection_pool.putconn(conn)
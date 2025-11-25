# -*- coding: utf-8 -*-
"""
util/db_utils.py
Các hàm tiện ích cho database: connect, lock, logging
"""

import pymysql
from pymysql.constants import CLIENT
from datetime import datetime
from util.metadata import get_all_metadata

def connect_db(host, port, user, password, local_infile=True, **kwargs):
    """Kết nối database với cấu hình"""
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        charset="utf8mb4",
        autocommit=True,
        local_infile=local_infile,
        client_flag=CLIENT.LOCAL_FILES
    )

def run_query(conn, sql, params=None):
    """Thực thi câu SQL và trả về kết quả"""
    with conn.cursor() as cur:
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        try:
            return cur.fetchall()
        except Exception:
            return None

# ============== LOCK ==============
def acquire_lock(conn, lock_name: str, timeout_sec: int = 3) -> bool:
    """Acquire MySQL named lock để tránh job chạy chồng"""
    with conn.cursor() as cur:
        cur.execute("SELECT GET_LOCK(%s, %s)", (lock_name, timeout_sec))
        row = cur.fetchone()
        return bool(row and row[0] == 1)

def release_lock(conn, lock_name: str):
    """Release MySQL named lock"""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
    except Exception:
        pass

# ============== LOGGING ==============
def ensure_control_schema(conn):
    """Đảm bảo control schema và log_history table tồn tại"""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS control;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS control.log_history (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                step VARCHAR(32),
                started_at DATETIME,
                finished_at DATETIME,
                row_count INT,
                status_txt VARCHAR(16),
                message TEXT,
                run_by VARCHAR(128),
                host_name VARCHAR(128),
                pid INT,
                session_id CHAR(36),
                script_path VARCHAR(512),
                git_rev VARCHAR(64),
                client_user VARCHAR(128),
                src_ip VARCHAR(64)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

def write_log(conn, step: str, started: datetime, finished: datetime, 
              row_count: int, status: str, message: str):
    """Ghi log vào control.log_history với đầy đủ metadata"""
    ensure_control_schema(conn)
    
    # Lấy metadata
    meta = get_all_metadata()
    
    # Lấy MySQL current user
    with conn.cursor() as cur:
        cur.execute("SELECT CURRENT_USER();")
        mysql_user = cur.fetchone()[0] if cur.rowcount else None
        
        # Insert log
        cur.execute("""
            INSERT INTO control.log_history
            (step, started_at, finished_at, row_count, status_txt, message,
             run_by, host_name, pid, session_id, script_path, git_rev, client_user, src_ip)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """, (
            step, started, finished, row_count, status, message,
            meta["run_by"], meta["host_name"], meta["pid"], meta["session_id"],
            meta["script_path"], meta["git_rev"], mysql_user, meta["src_ip"]
        ))
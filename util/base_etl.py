"""
Base ETL class để tránh lặp code giữa các bước ETL
"""
import sys
from datetime import datetime
from typing import Optional
from abc import ABC, abstractmethod

from util.config import load_config, get_db_config, get_email_config
from util.metadata import SESSION_ID, RUN_BY, HOST_NAME, PID
from util.db_utils import connect_db, acquire_lock, release_lock, write_log
from util.email_utils import send_mail, build_error_mail


class BaseETL(ABC):
    """Base class cho tất cả ETL jobs"""
    
    def __init__(self, job_name: str, lock_suffix: str = ""):
        self.job_name = job_name
        self.lock_name = f"{job_name}:{lock_suffix}" if lock_suffix else job_name
        self.conn = None
        self.started_at = None
        self.row_count = 0
        
        # Load config
        load_config()
        self.db_cfg = get_db_config()
        self.email_cfg = get_email_config()
    
    def connect(self, local_infile: bool = False):
        """Kết nối database"""
        self.conn = connect_db(
            host=self.db_cfg["host"],
            port=self.db_cfg["port"],
            user=self.db_cfg["user"],
            password=self.db_cfg["password"],
            local_infile=local_infile
        )
    
    def acquire_job_lock(self, timeout_sec: int = 3) -> bool:
        """Acquire lock cho job"""
        if not self.conn:
            self.connect()
        
        if not acquire_lock(self.conn, self.lock_name, timeout_sec):
            # 1.2.2a Thông báo =" Bị khóa bởi  job khác  lock=({self.lock_name})"
            msg = f"Bị khoá bởi job khác (lock={self.lock_name})"
            write_log(self.conn, self.job_name, self.started_at, 
                     datetime.now(), 0, "SKIPPED", msg)
            print(msg)
            return False
        return True
    
    def release_job_lock(self):
        """Release lock cho job"""
        if self.conn:
            release_lock(self.conn, self.lock_name)
    
    def log_success(self, message: str = ""):
        """Ghi log thành công"""
        if self.conn:
            write_log(self.conn, self.job_name, self.started_at,
                     datetime.now(), self.row_count, "OK", message)
    
    def log_error(self, error: Exception):
        """Ghi log lỗi"""
        try:
            if not self.conn:
                self.connect()
                #1.1.4a Thông báo ="không thể kết nối db"
            write_log(self.conn, self.job_name, self.started_at,
                     datetime.now(), self.row_count, "FAILED", str(error))
        except Exception:
            pass
    
    def send_error_email(self, error: Exception):
        """Gửi email cảnh báo lỗi"""
        try:
            subject, body = build_error_mail(
                job=self.job_name,
                err=error,
                host=HOST_NAME,
                user=RUN_BY,
                session=SESSION_ID,
                pid=PID,
                script=sys.argv[0]
            )
            send_mail(self.email_cfg["send_user"], subject, body)
        except Exception as mail_err:
            print(f"[WARN] Không thể gửi email cảnh báo: {mail_err}")

    #Đóng kết nối cleanup(): đóng DB, release lock
    def cleanup(self):
        """Cleanup resources"""
        self.release_job_lock()
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
    
    @abstractmethod
    def execute(self):
        """Main logic của ETL job - phải implement ở subclass"""
        pass
    
    def run(self):
        """Template method - chạy toàn bộ flow"""
        self.started_at = datetime.now()
        
        try:
            print(f"[{SESSION_ID}] Starting {self.job_name}...")
            
            # 1.1.4 Kết nối db thành công?
            self.connect(local_infile=getattr(self, 'need_local_infile', False))
            # 1.2.2 Lấy đc lock?
            if not self.acquire_job_lock():
                sys.exit(0)
            
            # Chạy logic chính
            self.execute()
            
            #Log success(rowcount)
            self.log_success(f"Completed with {self.row_count} rows")
            print(f"✓ {self.job_name} hoàn tất ({self.row_count} rows)")
            
        except Exception as e:
            # Log_error(e) ghi control.log_history (status =FAIL,error_msg)
            self.log_error(e)
            #  Gửi email thông báo lỗi đến admin send_error_email(e)
            self.send_error_email(e)
            print(f"✗ Lỗi {self.job_name}: {e}", file=sys.stderr)
            sys.exit(1)
            
        finally:
            self.cleanup()
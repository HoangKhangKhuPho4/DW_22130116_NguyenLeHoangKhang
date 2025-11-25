# -*- coding: utf-8 -*-
"""
util/email_utils.py
Gửi email & tạo nội dung cảnh báo ETL
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import os
import sys

# Lazy load email config khi cần
_EMAIL_CONFIG = None


def _get_email_config():
    """Lazy load email config từ DB"""
    global _EMAIL_CONFIG
    
    if _EMAIL_CONFIG is None:
        try:
            from util.config import get_email_config
            _EMAIL_CONFIG = get_email_config()
        except Exception as e:
            print(f"[WARN] Không thể load email config: {e}")
            _EMAIL_CONFIG = {
                "email_user": "",
                "email_pass": "",
                "send_user": ""
            }
    
    return _EMAIL_CONFIG


def send_mail(to_email: str, subject: str, body: str):
    """
    Gửi email qua Gmail SMTP SSL
    
    Args:
        to_email: Email người nhận
        subject: Tiêu đề email
        body: Nội dung email
    """
    config = _get_email_config()
    email_user = config["email_user"]
    email_pass = config["email_pass"]
    
    # Kiểm tra config
    if not email_user or not email_pass:
        print("[WARN] Email config chưa được thiết lập. Bỏ qua gửi email.")
        return
    
    # Tạo message
    msg = MIMEMultipart()
    msg["From"] = email_user
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))
    
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as server:
            server.login(email_user, email_pass)
            server.send_message(msg)
        print(f"✓ Đã gửi email -> {to_email}")
        
    except smtplib.SMTPAuthenticationError:
        print("[ERROR] Lỗi xác thực email. Kiểm tra EMAIL_USER và EMAIL_PASS trong config DB.")
        
    except smtplib.SMTPException as e:
        print(f"[ERROR] Lỗi SMTP: {e}")
        
    except Exception as e:
        print(f"[WARN] Không thể gửi email: {e}")


def build_error_mail(job: str, err: Exception, host: str, user: str,
                     session: str, pid: int, script: str = None, extra: str = None):
    """
    Tạo nội dung email cảnh báo lỗi ETL
    
    Args:
        job: Tên job bị lỗi
        err: Exception object
        host: Hostname
        user: User chạy job
        session: Session ID
        pid: Process ID
        script: Đường dẫn script (optional)
        extra: Thông tin bổ sung (optional)
    
    Returns:
        tuple: (subject, body)
    """
    subject = f"⚠️ [DW ETL] Job {job} bị lỗi"
    
    # Script path
    script_path = os.path.abspath(script or sys.argv[0]) if (script or sys.argv[0]) else "N/A"
    
    # Build body
    body_parts = [
        "="*60,
        f"ETL JOB ERROR REPORT",
        "="*60,
        "",
        f"Job Name    : {job}",
        f"Status      : FAILED",
        f"Time        : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "--- Environment Info ---",
        f"Host        : {host}",
        f"User        : {user}",
        f"Session ID  : {session}",
        f"Process ID  : {pid}",
        f"Script Path : {script_path}",
    ]
    
    # Thêm thông tin extra nếu có
    if extra:
        body_parts.extend([
            "",
            "--- Additional Info ---",
            extra
        ])
    
    # Thêm error details
    body_parts.extend([
        "",
        "--- Error Details ---",
        f"Type        : {type(err).__name__}",
        f"Message     : {str(err)}",
        "",
        "--- Full Traceback ---",
    ])
    
    # Thêm traceback nếu có
    try:
        import traceback
        tb = ''.join(traceback.format_exception(type(err), err, err.__traceback__))
        body_parts.append(tb)
    except Exception:
        body_parts.append(str(err))
    
    body_parts.extend([
        "",
        "="*60,
        "Please check the logs and database for more details.",
        "="*60
    ])
    
    body = "\n".join(body_parts)
    
    return subject, body


def build_success_mail(job: str, row_count: int, elapsed_seconds: float,
                       host: str, user: str, session: str, message: str = ""):
    """
    Tạo nội dung email thông báo thành công (optional)
    
    Args:
        job: Tên job
        row_count: Số dòng đã xử lý
        elapsed_seconds: Thời gian chạy (giây)
        host: Hostname
        user: User chạy job
        session: Session ID
        message: Message bổ sung
    
    Returns:
        tuple: (subject, body)
    """
    subject = f"✓ [DW ETL] Job {job} hoàn thành"
    
    body_parts = [
        "="*60,
        f"ETL JOB SUCCESS REPORT",
        "="*60,
        "",
        f"Job Name    : {job}",
        f"Status      : SUCCESS",
        f"Time        : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "--- Statistics ---",
        f"Rows        : {row_count:,}",
        f"Duration    : {elapsed_seconds:.2f} seconds",
        "",
        "--- Environment Info ---",
        f"Host        : {host}",
        f"User        : {user}",
        f"Session ID  : {session}",
    ]
    
    if message:
        body_parts.extend([
            "",
            "--- Message ---",
            message
        ])
    
    body_parts.extend([
        "",
        "="*60
    ])
    
    body = "\n".join(body_parts)
    
    return subject, body


def send_error_notification(job: str, err: Exception, host: str, user: str,
                           session: str, pid: int, script: str = None):
    """
    Helper function: Tạo và gửi email cảnh báo lỗi trong một bước
    
    Args:
        job: Tên job bị lỗi
        err: Exception object
        host: Hostname
        user: User chạy job
        session: Session ID
        pid: Process ID
        script: Đường dẫn script (optional)
    """
    try:
        config = _get_email_config()
        to_email = config.get("send_user", "")
        
        if not to_email:
            print("[WARN] SEND_USER chưa được cấu hình. Bỏ qua gửi email.")
            return
        
        subject, body = build_error_mail(job, err, host, user, session, pid, script)
        send_mail(to_email, subject, body)
        
    except Exception as mail_err:
        print(f"[WARN] Không thể gửi email cảnh báo: {mail_err}")


# Backward compatibility
def get_email_credentials():
    """
    Deprecated: Sử dụng get_email_config() từ util.config thay thế
    """
    print("[DEPRECATED] get_email_credentials() is deprecated. Use config.get_email_config() instead.")
    config = _get_email_config()
    return config["email_user"], config["email_pass"]
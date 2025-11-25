import os
import sys
import socket
import getpass
import uuid
import subprocess

# Import BASE_DIR an toàn
try:
    from util.config import BASE_DIR
except Exception:
    # fallback: luôn lấy thư mục project bằng cách đi lên 1 cấp từ util/
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ---------------- SESSION METADATA ---------------- #

SESSION_ID = str(uuid.uuid4())
RUN_BY = os.getenv("RUN_BY") or getpass.getuser()
HOST_NAME = socket.gethostname()
PID = os.getpid()

# Đường dẫn file script chính
SCRIPT_PATH = os.path.abspath(sys.argv[0])


def get_git_revision():
    """Lấy git commit hiện tại (nếu không có git → trả về None)."""
    git_dir = os.path.join(BASE_DIR, ".git")

    if not os.path.exists(git_dir):
        return None

    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=BASE_DIR,
            stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        return None


def get_source_ip():
    """Lấy địa chỉ IP outbound (an toàn, không crash)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return None


GIT_REV = get_git_revision()
SRC_IP = get_source_ip()


def get_all_metadata():
    """Trả metadata dạng dict."""
    return {
        "session_id": SESSION_ID,
        "run_by": RUN_BY,
        "host_name": HOST_NAME,
        "pid": PID,
        "script_path": SCRIPT_PATH,
        "git_rev": GIT_REV,
        "src_ip": SRC_IP
    }

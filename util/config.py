import os
import pymysql
from typing import Dict, Any

# Thư mục gốc project (project/)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

# Cache để tránh query DB nhiều lần
_CONFIG_CACHE: Dict[str, str] = {}
_DB_CONNECTION_PARAMS = None


def _get_db_connection_from_env():
    """Lấy thông tin kết nối DB từ biến môi trường (chỉ dùng lần đầu)"""
    global _DB_CONNECTION_PARAMS
    if _DB_CONNECTION_PARAMS is None:
        _DB_CONNECTION_PARAMS = {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", "3306")),
            "user": os.getenv("DB_USER", "root"),
            "password": os.getenv("DB_PASS", "")
        }
    return _DB_CONNECTION_PARAMS

# 1 Load config
def load_config():
    """Load toàn bộ config từ DB vào cache"""
    global _CONFIG_CACHE

    if _CONFIG_CACHE:
        return  # Đã load rồi

    conn_params = _get_db_connection_from_env()

    try:
        conn = pymysql.connect(
            host=conn_params["host"],
            port=conn_params["port"],
            user=conn_params["user"],
            password=conn_params["password"],
            charset="utf8mb4"
        )

        with conn.cursor() as cur:
            # Đảm bảo control schema và table tồn tại
            cur.execute("CREATE SCHEMA IF NOT EXISTS `control`;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS `control`.`config` (
                    ConfigKey VARCHAR(128) PRIMARY KEY,
                    ConfigValue TEXT,
                    Description VARCHAR(255),
                    UpdateTS DATETIME DEFAULT CURRENT_TIMESTAMP 
                             ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # Load toàn bộ config vào cache
            cur.execute("SELECT ConfigKey, ConfigValue FROM `control`.`config`;")
            for row in cur.fetchall():
                _CONFIG_CACHE[row[0]] = row[1]

        conn.close()
    #load được file không?
    except Exception as e:
        #lo
        raise RuntimeError(f"Không thể load config từ DB: {e}")


def get_config(key: str, default: str = "") -> str:
    """Lấy giá trị config theo key"""
    if not _CONFIG_CACHE:
        load_config()
    return _CONFIG_CACHE.get(key, default).strip()


def get_config_int(key: str, default: int = 0) -> int:
    """Lấy giá trị config dạng int"""
    val = get_config(key, str(default))
    try:
        return int(val)
    except ValueError:
        return default


def get_config_float(key: str, default: float = 0.0) -> float:
    """Lấy giá trị config dạng float"""
    val = get_config(key, str(default))
    try:
        return float(val)
    except ValueError:
        return default


# ===================== GROUPED CONFIG GETTERS =====================

def get_db_config() -> Dict[str, Any]:
    """Lấy cấu hình database"""
    return {
        "host": get_config("DB_HOST", "localhost"),
        "port": get_config_int("DB_PORT", 3306),
        "user": get_config("DB_USER", "root"),
        "password": get_config("DB_PASS", ""),
        "db_name": get_config("DB_NAME", "dw"),
        "stg_schema": get_config("STG_SCHEMA", "stg"),
        "stg_table": get_config("STG_TABLE", "crypto_usd_snapshot"),
        "mart_schema": get_config("DB_MART_SCHEMA", "data_mart")
    }


def get_staging_config() -> Dict[str, Any]:
    """Lấy cấu hình staging"""
    csv_path = get_config("CSV_PATH", "DW_data/crypto_usd_latest.csv")

    # Nếu là đường dẫn tương đối thì ghép với BASE_DIR
    if not os.path.isabs(csv_path):
        csv_path = os.path.join(BASE_DIR, csv_path)

    return {
        "csv_path": csv_path,
        "snapshot_mode": get_config("SNAPSHOT_MODE", "replace").lower()
    }


def get_extract_config() -> Dict[str, Any]:
    """Lấy cấu hình extract + API"""
    out_dir = get_config("EXT_OUT_DIR", "DW_data")
    if not os.path.isabs(out_dir):
        out_dir = os.path.join(BASE_DIR, out_dir)

    return {
        "vs_currency": get_config("EXT_VS_CURRENCY", "usd"),
        "per_page": get_config_int("EXT_PER_PAGE", 100),
        "pages": get_config_int("EXT_PAGES", 3),
        "sleep_page": get_config_float("EXT_SLEEP_PAGE", 1.2),
        "out_dir": out_dir,
        "base_url": get_config("API_BASE_URL", "https://api.coingecko.com/api/v3"),
        "coins_path": get_config("API_COINS_MARKETS_PATH", "/coins/markets")
    }


def get_email_config() -> Dict[str, str]:
    """Lấy cấu hình email"""
    return {
        "email_user": get_config("EMAIL_USER", ""),
        "email_pass": get_config("EMAIL_PASS", ""),
        "send_user": get_config("SEND_USER", "")
    }


def reload_config():
    """Force reload config từ DB (nếu cần)"""
    global _CONFIG_CACHE
    _CONFIG_CACHE = {}
    load_config()

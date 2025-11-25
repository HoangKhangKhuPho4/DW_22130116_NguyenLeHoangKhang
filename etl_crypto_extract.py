import os
import time
from datetime import datetime
import requests
import pandas as pd

from util.config import get_extract_config
from util.metadata import SESSION_ID, RUN_BY, HOST_NAME, PID
from util.base_etl import BaseETL

# Cột cần giữ lại
KEEP_COLS = [
    "id", "symbol", "name", "market_cap_rank",
    "current_price", "market_cap", "total_volume",
    "high_24h", "low_24h",
    "price_change_24h", "price_change_percentage_24h",
    "circulating_supply", "total_supply", "max_supply",
    "ath", "ath_change_percentage", "ath_date",
    "atl", "atl_change_percentage", "atl_date",
    "last_updated"
]


class ExtractCoinGecko(BaseETL):
    """Extract data từ CoinGecko API"""
    
    def __init__(self):
        # Load config extract
        self.cfg = get_extract_config()
        
        # Hệ thống khởi tạo BaseETL và acquire job lock
        super().__init__(
            job_name="extract",
            lock_suffix=f"{self.cfg['vs_currency']}"
        )
        
        # Tạo output directory
        os.makedirs(self.cfg["out_dir"], exist_ok=True)

    # fetch_page(page) gọi api request.get retry tối đa 3 lần
    def fetch_page(self, page: int) -> list:
        """Lấy dữ liệu 1 trang từ CoinGecko API"""
        base = f"{self.cfg['base_url'].rstrip('/')}{self.cfg['coins_path']}"
        headers = {"User-Agent": "DW-ETL/Extract"}
        params = {
            "vs_currency": self.cfg["vs_currency"],
            "per_page": self.cfg["per_page"],
            "page": page,
            "order": "market_cap_desc",
            "price_change_percentage": "24h"
        }
        
        for attempt in range(3):
            try:
                r = requests.get(base, params=params, headers=headers, timeout=30)
                r.raise_for_status()
                return r.json()
            except Exception as e:
                print(f"[WARN] Lỗi tải page {page}: {e}, retry {attempt + 1}/3...")
                time.sleep(2 * (attempt + 1))
        # fail 3 lần?
        raise RuntimeError(f"Failed to fetch page {page} after 3 retries")

    # Ghi dữ liệu ra 2 file
    def safe_write_csv(self, df: pd.DataFrame, path: str):
        """Ghi CSV với retry nếu file đang bị lock"""
        for attempt in range(3):
            try:
                df.to_csv(path, index=False, encoding="utf-8", lineterminator="\n")
                return
            except PermissionError:
                print(f"[WARN] File đang mở: {path}, retry {attempt + 1}/3...")
                time.sleep(2 * (attempt + 1))
        #1.4.2 ghi file thành công?
        raise PermissionError(f"Không thể ghi file (đang bị khóa): {path}")

    # excute() chạy ExtractCoinGecko
    def execute(self):
        """Main extraction logic"""
        # Extract data từ CoinGecko
        print(f"Đang lấy {self.cfg['pages']} trang từ CoinGecko...")
        all_rows = []
        
        for page in range(1, self.cfg["pages"] + 1):
            print(f"  Fetching page {page}/{self.cfg['pages']}...")
            all_rows.extend(self.fetch_page(page))
            time.sleep(self.cfg["sleep_page"])
        
        # Transform sang DataFrame
        df = pd.json_normalize(all_rows)
        keep = [c for c in KEEP_COLS if c in df.columns]
        df = df[keep].copy()
        
        # Thêm metadata
        ts = datetime.now()
        df["etl_ingest_ts"] = ts.isoformat(timespec="seconds")
        df["etl_source"] = "coingecko_coins_markets"
        df["etl_session_id"] = SESSION_ID
        df["etl_run_by"] = RUN_BY
        df["etl_host"] = HOST_NAME
        
        # Lưu file
        stamp = ts.strftime("%Y%m%d_%H%M%S")
        vs = self.cfg["vs_currency"]
        dated = os.path.join(self.cfg["out_dir"], 
                            f"crypto_{vs}_{stamp}_{HOST_NAME}_{PID}.csv")
        latest = os.path.join(self.cfg["out_dir"], 
                             f"crypto_{vs}_latest.csv")
        
        self.safe_write_csv(df, dated)
        self.safe_write_csv(df, latest)
        
        self.row_count = len(df)
        
        print(f"\n✓ Đã lưu dữ liệu thành công!")
        print(f"  • Latest: {latest}")
        print(f"  • Rows: {self.row_count}")


if __name__ == "__main__":
    ExtractCoinGecko().run()
import os
import csv
from pathlib import Path

from util.config import get_staging_config, get_db_config
from util.db_utils import run_query
from util.base_etl import BaseETL

# Cột staging
KNOWN_COLS = [
    "id", "symbol", "name", "market_cap_rank",
    "current_price", "market_cap", "total_volume",
    "high_24h", "low_24h",
    "price_change_24h", "price_change_percentage_24h",
    "circulating_supply", "total_supply", "max_supply",
    "ath", "ath_change_percentage", "ath_date",
    "atl", "atl_change_percentage", "atl_date",
    "last_updated",
    "etl_ingest_ts", "etl_source"
]

DDL_COL_TYPES = {
    "id": "VARCHAR(64) NOT NULL",
    "symbol": "VARCHAR(32)",
    "name": "VARCHAR(128)",
    "market_cap_rank": "INT",
    "current_price": "DECIMAL(20,8)",
    "market_cap": "DECIMAL(30,2)",
    "total_volume": "DECIMAL(30,2)",
    "high_24h": "DECIMAL(20,8)",
    "low_24h": "DECIMAL(20,8)",
    "price_change_24h": "DECIMAL(20,8)",
    "price_change_percentage_24h": "DECIMAL(10,4)",
    "circulating_supply": "DECIMAL(30,8)",
    "total_supply": "DECIMAL(30,8)",
    "max_supply": "DECIMAL(30,8)",
    "ath": "DECIMAL(20,8)",
    "ath_change_percentage": "DECIMAL(10,4)",
    "ath_date": "DATETIME NULL",
    "atl": "DECIMAL(20,8)",
    "atl_change_percentage": "DECIMAL(10,4)",
    "atl_date": "DATETIME NULL",
    "last_updated": "DATETIME NULL",
    "etl_ingest_ts": "DATETIME NULL",
    "etl_source": "VARCHAR(64)"
}


class LoadStaging(BaseETL):
    """Load CSV vào staging table"""

    need_local_infile = True

    def __init__(self):
        # GỌI SUPER TRƯỚC — để db_cfg, conn, logger, lock hoạt động đúng
        super().__init__(
            job_name="load_staging"
        )

        # Sau super(): db_cfg đã tồn tại
        self.stg_cfg = get_staging_config()

        # Đặt lock_suffix chuẩn
        self.lock_suffix = f"{self.db_cfg['stg_schema']}.{self.db_cfg['stg_table']}"

        self.csv_path = self.stg_cfg["csv_path"]
        self.csv_mysql = str(Path(self.csv_path).resolve()).replace("\\", "/")

    def read_csv_header(self) -> list:
        """Đọc header từ CSV"""
        with open(self.csv_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            header = next(reader)
            return [h.strip() for h in header]

    def ensure_staging_table(self):
        """Tạo schema & table nếu chưa có"""
        schema = self.db_cfg["stg_schema"]
        table = self.db_cfg["stg_table"]

        run_query(self.conn, f"CREATE SCHEMA IF NOT EXISTS `{schema}`;")

        cols_sql = [f"`{c}` {DDL_COL_TYPES[c]}" for c in KNOWN_COLS]
        run_query(self.conn, f"""
            CREATE TABLE IF NOT EXISTS `{schema}`.`{table}` (
                {", ".join(cols_sql)},
                PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

    def handle_snapshot_mode(self):
        """Xử lý chế độ snapshot"""
        schema = self.db_cfg["stg_schema"]
        table = self.db_cfg["stg_table"]

        if self.stg_cfg["snapshot_mode"] == "replace":
            print("  TRUNCATE staging table...")
            run_query(self.conn, f"TRUNCATE TABLE `{schema}`.`{table}`;")
        else:
            print("  Append mode...")

    def load_data_infile(self, header: list):
        """Load CSV bằng LOAD DATA LOCAL INFILE"""
        schema = self.db_cfg["stg_schema"]
        table = self.db_cfg["stg_table"]

        load_cols_clause, present_known = [], set()
        dummy_idx = 0

        for h in header:
            if h in KNOWN_COLS:
                load_cols_clause.append(h)
                present_known.add(h)
            else:
                dummy_idx += 1
                load_cols_clause.append(f"@dummy{dummy_idx}")

        missing = [c for c in KNOWN_COLS if c not in present_known]
        set_clause = ("SET " + ", ".join([f"`{c}`=NULL" for c in missing])) if missing else ""

        run_query(self.conn, f"""
            LOAD DATA LOCAL INFILE '{self.csv_mysql}'
            INTO TABLE `{schema}`.`{table}`
            CHARACTER SET utf8
            FIELDS TERMINATED BY ',' ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            ({", ".join(load_cols_clause)})
            {set_clause};
        """)

    def fix_datetime_nulls(self):
        schema = self.db_cfg["stg_schema"]
        table = self.db_cfg["stg_table"]

        run_query(self.conn, f"""
            UPDATE `{schema}`.`{table}`
            SET ath_date      = NULLIF(ath_date,      '0000-00-00 00:00:00'),
                atl_date      = NULLIF(atl_date,      '0000-00-00 00:00:00'),
                last_updated  = NULLIF(last_updated,  '0000-00-00 00:00:00'),
                etl_ingest_ts = NULLIF(etl_ingest_ts, '0000-00-00 00:00:00');
        """)

    def execute(self):
        if not os.path.exists(self.csv_path):
            raise FileNotFoundError(f"Không tìm thấy CSV: {self.csv_path}")

        header = self.read_csv_header()
        print(f"  CSV: {self.csv_mysql}")
        print(f"  Header: {len(header)} columns")

        try:
            run_query(self.conn, "SET GLOBAL local_infile=1;")
        except Exception:
            pass

        self.ensure_staging_table()
        self.handle_snapshot_mode()

        run_query(self.conn, "SET SESSION sql_mode = REPLACE(@@sql_mode, 'NO_ZERO_DATE', '');")
        run_query(self.conn, "SET SESSION sql_mode = REPLACE(@@sql_mode, 'NO_ZERO_IN_DATE', '');")

        print("  Loading data...")
        self.load_data_infile(header)
        self.fix_datetime_nulls()

        schema = self.db_cfg["stg_schema"]
        table = self.db_cfg["stg_table"]
        self.row_count = run_query(
            self.conn,
            f"SELECT COUNT(*) FROM `{schema}`.`{table}`;"
        )[0][0]

        print(f"  Loaded: {self.row_count} rows")


if __name__ == "__main__":
    LoadStaging().run()

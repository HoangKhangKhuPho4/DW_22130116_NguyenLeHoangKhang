from util.db_utils import run_query
from util.base_etl import BaseETL


class TransformDW(BaseETL):
    """Transform staging data và load vào Data Warehouse"""

    def __init__(self):
        # Gọi BaseETL trước để tạo db_cfg, conn, logger, session_meta
        super().__init__(job_name="transform_dw")

        # Sau super(): db_cfg đã tồn tại
        self.dw = self.db_cfg["db_name"]
        self.stg = self.db_cfg["stg_schema"]
        self.stg_table = self.db_cfg["stg_table"]

        # Set lock_suffix CHUẨN
        self.lock_suffix = f"{self.stg}.{self.stg_table}"

    def ensure_dw_tables(self):
        """Tạo DW schema và các tables nếu chưa có"""
        run_query(self.conn, f"CREATE SCHEMA IF NOT EXISTS `{self.dw}`;")

        # dim_coin
        run_query(self.conn, f"""
            CREATE TABLE IF NOT EXISTS `{self.dw}`.`dim_coin` (
                CoinKey   INT AUTO_INCREMENT PRIMARY KEY,
                CoinID    VARCHAR(64) NOT NULL,
                Symbol    VARCHAR(32),
                CoinName  VARCHAR(128),
                Name      VARCHAR(128),
                UNIQUE KEY uq_dim_coin_coinid (CoinID)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # dim_date
        run_query(self.conn, f"""
            CREATE TABLE IF NOT EXISTS `{self.dw}`.`dim_date` (
                DateKey    INT NOT NULL PRIMARY KEY,
                FullDate   DATE,
                DayOfWeek  VARCHAR(16),
                Month      TINYINT,
                Quarter    TINYINT,
                Year       SMALLINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # fact_crypto_snapshot
        run_query(self.conn, f"""
            CREATE TABLE IF NOT EXISTS `{self.dw}`.`fact_crypto_snapshot` (
                CoinKey             INT NOT NULL,
                DateKey             INT NOT NULL,
                MarketCapRank       INT,
                Price               DECIMAL(20,8),
                MarketCap           DECIMAL(30,2),
                Volume24h           DECIMAL(30,2),
                High24h             DECIMAL(20,8),
                Low24h              DECIMAL(20,8),
                PriceChange24h      DECIMAL(20,8),
                PctChange24h        DECIMAL(10,4),
                Ath                 DECIMAL(20,8),
                AthChangePct        DECIMAL(10,4),
                Atl                 DECIMAL(20,8),
                AtlChangePct        DECIMAL(10,4),
                LastUpdated         DATETIME,
                TransformTS         DATETIME,
                PRIMARY KEY (CoinKey, DateKey),
                CONSTRAINT fk_fact_coin FOREIGN KEY (CoinKey) 
                    REFERENCES `{self.dw}`.`dim_coin` (CoinKey),
                CONSTRAINT fk_fact_date FOREIGN KEY (DateKey) 
                    REFERENCES `{self.dw}`.`dim_date` (DateKey)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

    def upsert_dim_coin(self):
        """Upsert vào dim_coin"""
        run_query(self.conn, f"""
            INSERT INTO `{self.dw}`.`dim_coin` 
            (CoinID, Symbol, CoinName, Name)
            SELECT s.id, s.symbol, s.name, s.name
            FROM `{self.stg}`.`{self.stg_table}` s
            ON DUPLICATE KEY UPDATE
                Symbol=VALUES(Symbol),
                CoinName=VALUES(CoinName),
                Name=VALUES(Name);
        """)

    def upsert_dim_date(self):
        """Upsert vào dim_date"""
        run_query(self.conn, f"""
            INSERT INTO `{self.dw}`.`dim_date`
            (DateKey, FullDate, DayOfWeek, Month, Quarter, Year)
            SELECT
                CAST(DATE_FORMAT(s.last_updated, '%Y%m%d') AS SIGNED),
                DATE(s.last_updated),
                DATE_FORMAT(s.last_updated, '%W'),
                MONTH(s.last_updated),
                QUARTER(s.last_updated),
                YEAR(s.last_updated)
            FROM `{self.stg}`.`{self.stg_table}` s
            WHERE s.last_updated IS NOT NULL
            ON DUPLICATE KEY UPDATE
                FullDate=VALUES(FullDate),
                DayOfWeek=VALUES(DayOfWeek),
                Month=VALUES(Month),
                Quarter=VALUES(Quarter),
                Year=VALUES(Year);
        """)

    def upsert_fact(self):
        """Upsert vào fact_crypto_snapshot"""
        run_query(self.conn, f"""
            INSERT INTO `{self.dw}`.`fact_crypto_snapshot` (
                CoinKey, DateKey,
                MarketCapRank, Price, MarketCap, Volume24h,
                High24h, Low24h, PriceChange24h, PctChange24h,
                Ath, AthChangePct, Atl, AtlChangePct,
                LastUpdated, TransformTS
            )
            SELECT
                dc.CoinKey,
                CAST(DATE_FORMAT(s.last_updated, '%Y%m%d') AS SIGNED),
                s.market_cap_rank, s.current_price, s.market_cap, s.total_volume,
                s.high_24h, s.low_24h, s.price_change_24h, s.price_change_percentage_24h,
                s.ath, s.ath_change_percentage, s.atl, s.atl_change_percentage,
                s.last_updated, NOW()
            FROM `{self.stg}`.`{self.stg_table}` s
            JOIN `{self.dw}`.`dim_coin` dc 
                ON dc.CoinID = s.id
            WHERE s.last_updated IS NOT NULL
            ON DUPLICATE KEY UPDATE
                MarketCapRank=VALUES(MarketCapRank),
                Price=VALUES(Price),
                MarketCap=VALUES(MarketCap),
                Volume24h=VALUES(Volume24h),
                High24h=VALUES(High24h),
                Low24h=VALUES(Low24h),
                PriceChange24h=VALUES(PriceChange24h),
                PctChange24h=VALUES(PctChange24h),
                Ath=VALUES(Ath),
                AthChangePct=VALUES(AthChangePct),
                Atl=VALUES(Atl),
                AtlChangePct=VALUES(AtlChangePct),
                LastUpdated=VALUES(LastUpdated),
                TransformTS=VALUES(TransformTS);
        """)

    def execute(self):
        """Main transformation logic"""
        print("  Creating DW tables...")
        self.ensure_dw_tables()

        print("  Upserting dim_coin...")
        self.upsert_dim_coin()

        print("  Upserting dim_date...")
        self.upsert_dim_date()

        print("  Upserting fact_crypto_snapshot...")
        self.upsert_fact()

        # Count rows
        self.row_count = run_query(self.conn, f"""
            SELECT COUNT(*) 
            FROM `{self.stg}`.`{self.stg_table}` 
            WHERE last_updated IS NOT NULL;
        """)[0][0]

        print(f"  Transformed: {self.row_count} rows")


if __name__ == "__main__":
    TransformDW().run()

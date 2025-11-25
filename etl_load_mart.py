from util.db_utils import run_query
from util.base_etl import BaseETL


class LoadDataMart(BaseETL):
    """Load Data Mart từ Data Warehouse"""
    
    def __init__(self):
        super().__init__(
            job_name="load_mart",
            lock_suffix="data_mart"
        )
        
        self.dw = self.db_cfg["db_name"]
        self.mart = self.db_cfg["mart_schema"]
    
    def ensure_mart_tables(self):
        """Tạo các bảng Data Mart nếu chưa có"""
        run_query(self.conn, f"CREATE SCHEMA IF NOT EXISTS `{self.mart}`;")
        
        # overview_daily
        run_query(self.conn, f"""
            CREATE TABLE IF NOT EXISTS `{self.mart}`.`overview_daily` (
                DateKey INT PRIMARY KEY,
                TotalCoins INT,
                TotalMarketCap DECIMAL(38,2),
                TotalVolume DECIMAL(38,2),
                Top1_Coin VARCHAR(128),
                Top1_MarketCap DECIMAL(30,2),
                CreateTS DATETIME
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        
        # analyst_snapshot
        run_query(self.conn, f"""
            CREATE TABLE IF NOT EXISTS `{self.mart}`.`analyst_snapshot` (
                CoinKey INT,
                DateKey INT,
                CoinName VARCHAR(128),
                Symbol VARCHAR(32),
                MarketCapRank INT,
                Price DECIMAL(20,8),
                MarketCap DECIMAL(30,2),
                Volume24h DECIMAL(30,2),
                PctChange24h DECIMAL(10,4),
                Year SMALLINT,
                Month TINYINT,
                DayOfWeek VARCHAR(16),
                CreateTS DATETIME,
                PRIMARY KEY (CoinKey, DateKey)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
    
    def load_overview_daily(self):
        """Tổng hợp dữ liệu thị trường theo ngày"""
        run_query(self.conn, f"""
            INSERT INTO `{self.mart}`.`overview_daily`
            (DateKey, TotalCoins, TotalMarketCap, TotalVolume, Top1_Coin, Top1_MarketCap, CreateTS)
            SELECT
                d.DateKey,
                COUNT(f.CoinKey) AS TotalCoins,
                SUM(f.MarketCap) AS TotalMarketCap,
                SUM(f.Volume24h) AS TotalVolume,
                (
                    SELECT dc2.CoinName
                    FROM `{self.dw}`.`fact_crypto_snapshot` f2
                    JOIN `{self.dw}`.`dim_coin` dc2 ON dc2.CoinKey = f2.CoinKey
                    WHERE f2.DateKey = f.DateKey
                    ORDER BY f2.MarketCap DESC
                    LIMIT 1
                ) AS Top1_Coin,
                (
                    SELECT f3.MarketCap
                    FROM `{self.dw}`.`fact_crypto_snapshot` f3
                    WHERE f3.DateKey = f.DateKey
                    ORDER BY f3.MarketCap DESC
                    LIMIT 1
                ) AS Top1_MarketCap,
                NOW()
            FROM `{self.dw}`.`fact_crypto_snapshot` f
            JOIN `{self.dw}`.`dim_date` d ON d.DateKey = f.DateKey
            GROUP BY d.DateKey
            ON DUPLICATE KEY UPDATE
                TotalCoins=VALUES(TotalCoins),
                TotalMarketCap=VALUES(TotalMarketCap),
                TotalVolume=VALUES(TotalVolume),
                Top1_Coin=VALUES(Top1_Coin),
                Top1_MarketCap=VALUES(Top1_MarketCap),
                CreateTS=VALUES(CreateTS);
        """)
    
    def load_analyst_snapshot(self):
        """Snapshot chi tiết từng coin"""
        run_query(self.conn, f"""
            INSERT INTO `{self.mart}`.`analyst_snapshot`
            (CoinKey, DateKey, CoinName, Symbol, MarketCapRank, Price, MarketCap, 
             Volume24h, PctChange24h, Year, Month, DayOfWeek, CreateTS)
            SELECT
                f.CoinKey, f.DateKey,
                c.CoinName, c.Symbol,
                f.MarketCapRank, f.Price, f.MarketCap, f.Volume24h, f.PctChange24h,
                d.Year, d.Month, d.DayOfWeek, NOW()
            FROM `{self.dw}`.`fact_crypto_snapshot` f
            JOIN `{self.dw}`.`dim_coin` c ON f.CoinKey = c.CoinKey
            JOIN `{self.dw}`.`dim_date` d ON f.DateKey = d.DateKey
            ON DUPLICATE KEY UPDATE
                Price=VALUES(Price),
                MarketCap=VALUES(MarketCap),
                Volume24h=VALUES(Volume24h),
                PctChange24h=VALUES(PctChange24h),
                CreateTS=VALUES(CreateTS);
        """)
    
    def execute(self):
        """Main loading logic"""
        print("  Creating Data Mart tables...")
        self.ensure_mart_tables()
        
        print("  Loading overview_daily...")
        self.load_overview_daily()
        
        print("  Loading analyst_snapshot...")
        self.load_analyst_snapshot()
        
        # Count rows
        self.row_count = run_query(self.conn, 
            f"SELECT COUNT(*) FROM `{self.mart}`.`analyst_snapshot`;")[0][0]
        
        print(f"  Loaded: {self.row_count} rows")


if __name__ == "__main__":
    LoadDataMart().run()
-- =====================================================
-- Script khởi tạo config database
-- Chạy script này trước khi chạy ETL pipeline
-- =====================================================

-- Tạo schema control
CREATE SCHEMA IF NOT EXISTS `control`;

-- Tạo bảng config
CREATE TABLE IF NOT EXISTS `control`.`config` (
    ConfigKey VARCHAR(128) PRIMARY KEY,
    ConfigValue TEXT,
    Description VARCHAR(255),
    UpdateTS DATETIME DEFAULT CURRENT_TIMESTAMP 
             ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Xóa dữ liệu cũ
TRUNCATE TABLE `control`.`config`;

-- =====================================================
-- INSERT CONFIG MỚI
-- =====================================================

INSERT INTO `control`.`config` (ConfigKey, ConfigValue, Description) VALUES

-- ========== API / EXTRACT CONFIG ==========
('EXT_OUT_DIR', 'DW_data', 'Thư mục lưu file CSV'),
('EXT_VS_CURRENCY', 'usd', 'Đơn vị tiền tệ'),
('EXT_PER_PAGE', '100', 'Số record mỗi trang API'),
('EXT_PAGES', '3', 'Số trang API'),
('EXT_SLEEP_PAGE', '1.2', 'Delay lấy API'),
('API_BASE_URL', 'https://api.coingecko.com/api/v3', 'Base URL API'),
('API_COINS_MARKETS_PATH', '/coins/markets', 'Path API coins'),

-- ========== DATABASE CONFIG ==========
('DB_HOST', 'localhost', 'MySQL host'),
('DB_PORT', '3306', 'MySQL port'),
('DB_USER', 'root', 'MySQL user'),
('DB_PASS', '', 'MySQL password'),
('DB_NAME', 'dw', 'Schema DW'),

-- ========== STAGING CONFIG ==========
('CSV_PATH', 'DW_data/crypto_usd_latest.csv', 'Đường dẫn CSV'),
('STG_SCHEMA', 'stg', 'Schema Staging'),
('STG_TABLE', 'crypto_usd_snapshot', 'Bảng staging'),
('SNAPSHOT_MODE', 'replace', 'Chế độ snapshot'),

-- ========== LOGGING CONFIG ==========
('LOG_LEVEL', 'INFO', 'Mức log'),

-- ========== EMAIL CONFIG ==========
('EMAIL_USER', 'huyvu10012003@gmail.com', 'Gửi email'),
('EMAIL_PASS', 'edls sbiq gnup wzrl', 'App password Gmail'),
('SEND_USER', 'huyvu10012003@gmail.com', 'Email nhận cảnh báo'),

-- ========== DATA MART CONFIG ==========
('DB_MART_SCHEMA', 'data_mart', 'Schema Data Mart');

-- Kiểm tra kết quả
SELECT * FROM `control`.`config` ORDER BY ConfigKey;

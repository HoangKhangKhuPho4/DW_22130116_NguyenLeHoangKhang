@echo off
setlocal

rem ====== Cấu hình đường dẫn ======
set PROJ=D:\DW_crypto

rem === CHỌN PYTHON (ưu tiên đường dẫn tuyệt đối) ===
set PY=C:\Users\ADMIN\AppData\Local\Programs\Python\Python314\python.exe
rem set PY=C:\Windows\py.exe

cd /d "%PROJ%"
set PYTHONIOENCODING=utf-8
chcp 65001 >nul

echo ==================================================
echo [START] ETL pipeline at %date% %time%
echo Using Python: %PY%
echo --------------------------------------------------

rem ====== CHẠY CÁC FILE PY THEO THỨ TỰ ======
echo ▶️ Extract từ CoinGecko...
"%PY%" etl_crypto_extract.py || goto :error

echo ▶️ Load dữ liệu vào staging...
"%PY%" etl_load_staging.py || goto :error

echo ▶️ Transform & Load vào DW...
"%PY%" etl_trans_load_dw.py || goto :error

echo ▶️ Transform & Load vào DW...
"%PY%" etl_load_mart.py || goto :error
echo --------------------------------------------------
echo ✅ Hoàn tất ETL thành công lúc %time%
exit /b 0

:error
echo --------------------------------------------------
echo ❌ ETL thất bại ở bước trên. Xem log trong MySQL (control.log_history)
exit /b 1

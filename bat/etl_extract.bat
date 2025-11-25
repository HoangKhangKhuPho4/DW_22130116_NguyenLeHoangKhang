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
echo [START] STEP 1 - Extract từ CoinGecko at %date% %time%
echo Using Python: %PY%
echo --------------------------------------------------

"%PY%" etl_crypto_extract.py
if errorlevel 1 (
    echo ❌ Extract FAILED. Xem log trong MySQL (control.log_history)
    exit /b 1
)

echo ✅ Extract OK lúc %time%
exit /b 0

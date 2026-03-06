@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo ============================================
echo   Data Cleaning Tool - Web
echo   Starting...
echo ============================================
echo.
echo   Local:  http://localhost:8501
echo.
echo   Close this window to stop.
echo ============================================

streamlit run streamlit_app.py --server.address 0.0.0.0 --server.port 8501 --browser.gatherUsageStats false
pause

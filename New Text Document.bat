@echo off
cd /d "%~dp0"
set PYTHONPATH=%cd%\src

REM install deps if missing
python -c "import pandas, yaml, numpy" 2>nul || (
  echo Installing required packages...
  pip install pandas numpy pyyaml >nul
)

python -m swing_systems.bin.diagnose_strategies --universe configs\universe.yaml --outdir docs
echo.
echo Output written to docs\diagnostics.csv and docs\diagnostics.html
pause

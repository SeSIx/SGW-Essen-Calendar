@echo off
echo SGW Essen Termine Scraper
echo ========================
echo.

if "%1"=="-new" (
    echo Starte manuelle Termineingabe...
    python sgw_essen_scraper.py -new
) else if "%1"=="-list" (
    echo Zeige vorhandene Termine...
    python sgw_essen_scraper.py --list
) else if "%1"=="-help" (
    echo Zeige Hilfe...
    python sgw_essen_scraper.py --help
) else (
    echo Starte normales Scraping...
    python sgw_essen_scraper.py
)

echo.
echo Drücken Sie eine beliebige Taste zum Beenden...
pause >nul



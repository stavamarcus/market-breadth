"""
Konfigurace pro Market Breadth projekt
Obsahuje pouze konstanty a cesty - žádnou logiku!
"""

from pathlib import Path

# ========================================
# ZÁKLADNÍ CESTY PROJEKTU
# ========================================

BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = BASE_DIR / "data"
TICKERS_DIR = DATA_DIR / "tickers"
PRICES_DIR = DATA_DIR / "prices"
SNAPSHOTS_DIR = DATA_DIR / "snapshots"
OUTPUT_DIR = BASE_DIR / "output"
LOGS_DIR = BASE_DIR / "logs"

# ========================================
# PARAMETRY STAHOVÁNÍ DAT
# ========================================

HISTORY_TRADING_DAYS = 500
PRICE_INTERVAL = "1d"
AUTO_ADJUST = True
MAX_TICKERS_PER_BATCH = 50
REQUEST_DELAY = 0.5

# ========================================
# ZDROJE TICKERŮ
# ========================================

SP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
SP500_FALLBACK_CSV = TICKERS_DIR / "sp500.csv"

# ========================================
# PARAMETRY PRO BREADTH VÝPOČTY
# ========================================

MA_WINDOWS = (50, 100, 200)

# McClellan EMA periody
MCCLELLAN_EMA_SHORT = 19
MCCLELLAN_EMA_LONG = 39

# ========================================
# DOSTATEČNOST DAT (COVERAGE)
# ========================================

MIN_COVERAGE_PERCENT = 90  # minimální % pokrytí
MIN_VALID_TICKERS = 450     # minimální počet validních tickerů

# ========================================
# KONTROLA STÁŘÍ DAT
# ========================================

MAX_DATA_AGE_DAYS = 3
DATA_STALE_WARNING = "⚠️ DATA JSOU ZASTARALÁ"

# ========================================
# INTERPRETAČNÍ PÁSMA (JEN PRO ZOBRAZENÍ V "i")
# ========================================
# DŮLEŽITÉ: Tyto hodnoty se NEPOUŽÍVAJÍ pro logiku!
# Slouží POUZE jako textová nápověda v HTML reportu.

INTERPRETATION_BANDS = {
    "MA50": {
        "low": (0, 40, "Slabá participace"),
        "medium": (40, 60, "Neutrální"),
        "high": (60, 100, "Silná participace")
    },
    "MA100": {
        "low": (0, 45, "Slabá střednědobá struktura"),
        "medium": (45, 60, "Neutrální"),
        "high": (60, 100, "Silná střednědobá struktura")
    },
    "MA200": {
        "low": (0, 45, "Slabý strukturální trend"),
        "medium": (45, 65, "Neutrální"),
        "high": (65, 100, "Silný strukturální trend")
    },
    "AD": {
        "low": (0, 45, "Více klesajících akcií"),
        "medium": (45, 55, "Vyrovnaný trh"),
        "high": (55, 100, "Více rostoucích akcií")
    },
    "HL": {
        "low": (0, 40, "Dominují nová minima"),
        "medium": (40, 60, "Vyrovnaný poměr"),
        "high": (60, 100, "Dominují nová maxima")
    },
    "MCCLELLAN": {
        "low": (-100, -50, "Zpomalení/pokles participace"),
        "medium": (-50, 50, "Neutrální momentum"),
        "high": (50, 100, "Zrychlení participace")
    }
}

# ========================================
# LOGOVÁNÍ
# ========================================

LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
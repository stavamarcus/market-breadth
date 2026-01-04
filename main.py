from pathlib import Path

# Kořen projektu (tam, kde je main.py)
BASE_DIR = Path(__file__).resolve().parent

# Složky, které projekt potřebuje
REQUIRED_DIRS = [
    BASE_DIR / "data",
    BASE_DIR / "data" / "prices",
    BASE_DIR / "data" / "snapshots",
    BASE_DIR / "data" / "tickers",
    BASE_DIR / "output",
    BASE_DIR / "logs",
]

def ensure_directories():
    for directory in REQUIRED_DIRS:
        directory.mkdir(parents=True, exist_ok=True)

if __name__ == "__main__":
    ensure_directories()
    print("✅ Projektové složky jsou připravené.")

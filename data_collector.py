"""
Data Collector - stahování dat z Yahoo Finance
VERZE 1.3 - přesná statistika (saved/protected/error)
"""

import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup

import config

# Zajistit, že složky existují
config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
config.PRICES_DIR.mkdir(parents=True, exist_ok=True)
config.TICKERS_DIR.mkdir(parents=True, exist_ok=True)

# Nastavení loggeru
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT,
    datefmt=config.LOG_DATE_FORMAT,
    handlers=[
        logging.FileHandler(
            config.LOGS_DIR / f"data_collector_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
            encoding='utf-8'
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def sanitize_ticker(ticker):
    """Opraví ticker pro Yahoo Finance (tečka → pomlčka)."""
    return ticker.replace('.', '-')


def fetch_sp500_tickers_from_wiki():
    """Stáhne seznam S&P 500 tickerů z Wikipedie."""
    try:
        logger.info(f"Stahuji seznam S&P 500 tickeru z: {config.SP500_WIKI_URL}")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(config.SP500_WIKI_URL, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'id': 'constituents'})
        
        if not table:
            logger.error("Tabulka 'constituents' nenalezena na Wikipedii")
            return None
        
        tickers = []
        rows = table.find_all('tr')[1:]
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) > 0:
                ticker = cols[0].text.strip()
                tickers.append(ticker)
        
        if len(tickers) < 400:
            logger.error(f"Prilis malo tickeru nacteno z Wikipedie: {len(tickers)}")
            return None
        
        logger.info(f"OK - Uspesne nacteno {len(tickers)} tickeru z Wikipedie")
        return tickers
        
    except Exception as e:
        logger.error(f"Chyba pri stahovani z Wikipedie: {e}")
        return None


def load_fallback_tickers():
    """Načte tickery z lokálního fallback CSV."""
    try:
        if not config.SP500_FALLBACK_CSV.exists():
            logger.warning(f"Fallback CSV neexistuje: {config.SP500_FALLBACK_CSV}")
            return []
        
        df = pd.read_csv(config.SP500_FALLBACK_CSV)
        tickers = df['ticker'].tolist()
        
        logger.info(f"OK - Nacteno {len(tickers)} tickeru z fallback CSV")
        return tickers
        
    except Exception as e:
        logger.error(f"Chyba pri cteni fallback CSV: {e}")
        return []


def save_fallback_tickers(tickers):
    """Uloží tickery do fallback CSV."""
    try:
        df = pd.DataFrame({'ticker': tickers})
        df.to_csv(config.SP500_FALLBACK_CSV, index=False)
        logger.info(f"OK - Fallback CSV aktualizovan: {len(tickers)} tickeru")
    except Exception as e:
        logger.error(f"Chyba pri ukladani fallback CSV: {e}")


def get_sp500_tickers():
    """Získá seznam S&P 500 tickerů (Wikipedia → fallback)."""
    tickers = fetch_sp500_tickers_from_wiki()
    
    if tickers:
        save_fallback_tickers(tickers)
        return tickers
    
    logger.warning("VAROVANI: Wikipedia fetch failed, using fallback CSV")
    tickers = load_fallback_tickers()
    
    if not tickers:
        logger.error("KRITICKA CHYBA: Ani fallback CSV neni k dispozici!")
        raise Exception("Nelze ziskat seznam tickeru")
    
    return tickers


def download_ticker_data(ticker, start_date):
    """Stáhne historická data pro jeden ticker."""
    try:
        data = yf.download(
            ticker,
            start=start_date,
            interval=config.PRICE_INTERVAL,
            auto_adjust=config.AUTO_ADJUST,
            progress=False
        )
        
        if data.empty:
            logger.warning(f"VAROVANI {ticker}: Prazdna data")
            return None
        
        if len(data) < config.HISTORY_TRADING_DAYS * 0.8:
            logger.warning(f"VAROVANI {ticker}: Prilis malo dat ({len(data)} dnu)")
            return None
        
        logger.info(f"OK {ticker}: {len(data)} dnu stahnuto")
        return data
        
    except Exception as e:
        logger.error(f"CHYBA {ticker}: {e}")
        return None


def save_ticker_data(ticker, data):
    """
    Uloží data tickeru do CSV s atomickou ochranou.
    
    Returns:
        str: "saved" | "protected" | "error"
    """
    try:
        filepath = config.PRICES_DIR / f"{ticker}.csv"
        
        # Kontrola existujícího souboru
        if filepath.exists():
            try:
                old_data = pd.read_csv(filepath, index_col=0, parse_dates=True)
                
                new_first_date = data.index[0]
                new_last_date = data.index[-1]
                old_first_date = old_data.index[0]
                old_last_date = old_data.index[-1]
                
                # OCHRANA 1: Poslední datum nesmí být starší
                if new_last_date < old_last_date:
                    logger.warning(
                        f"{ticker}: Nova data jsou STARSI "
                        f"({new_last_date.date()} < {old_last_date.date()}), NEPREPISUJI!"
                    )
                    return "protected"
                
                # OCHRANA 2: První datum nesmí být novější
                if new_first_date > old_first_date:
                    logger.warning(
                        f"{ticker}: Nova data zacinaji POZDEJI "
                        f"({new_first_date.date()} > {old_first_date.date()}), NEPREPISUJI!"
                    )
                    return "protected"
                
                # OCHRANA 3: Počet řádků (max 10 dnů úbytku)
                min_acceptable_rows = len(old_data) - 10
                
                if len(data) < min_acceptable_rows:
                    logger.warning(
                        f"{ticker}: Nova data maji MENE radku "
                        f"({len(data)} vs {len(old_data)}), NEPREPISUJI!"
                    )
                    return "protected"
                
                logger.info(
                    f"{ticker}: Nova data jsou lepsi "
                    f"({new_last_date.date()}, {len(data)} radku), PREPISUJI"
                )
                    
            except Exception as e:
                logger.warning(f"{ticker}: Nelze nacist stary soubor: {e}")
        
        # ATOMICKÉ UKLÁDÁNÍ
        temp_path = filepath.with_suffix('.tmp')
        
        try:
            data.to_csv(temp_path)
            temp_path.replace(filepath)
            logger.debug(f"Ulozeno {ticker}: {len(data)} radku")
            return "saved"
            
        except Exception as e:
            if temp_path.exists():
                temp_path.unlink()
            raise e
        
    except Exception as e:
        logger.error(f"CHYBA ukladani {ticker}: {e}")
        return "error"


def download_all_tickers(tickers):
    """Stáhne data pro všechny tickery."""
    start_date = datetime.now() - timedelta(days=config.HISTORY_TRADING_DAYS * 1.5)
    
    stats = {
        'total': len(tickers),
        'success': 0,
        'protected': 0,
        'failed': 0,
        'empty': 0
    }
    
    logger.info(f"Zacinam stahovani {len(tickers)} tickeru...")
    logger.info(f"Pocatecni datum: {start_date.date()}")
    
    for i, ticker in enumerate(tickers, 1):
        clean_ticker = sanitize_ticker(ticker)
        
        if ticker != clean_ticker:
            logger.info(f"[{i}/{len(tickers)}] Stahuji {ticker} (Yahoo: {clean_ticker})...")
        else:
            logger.info(f"[{i}/{len(tickers)}] Stahuji {ticker}...")
        
        data = download_ticker_data(clean_ticker, start_date)
        
        if data is not None and not data.empty:
            result = save_ticker_data(clean_ticker, data)
            
            if result == "saved":
                stats['success'] += 1
            elif result == "protected":
                stats['protected'] += 1
            elif result == "error":
                stats['failed'] += 1
                
        elif data is not None and data.empty:
            stats['empty'] += 1
        else:
            stats['failed'] += 1
        
        # Rate limiting
        if i % config.MAX_TICKERS_PER_BATCH == 0:
            logger.info(f"Pauza po {config.MAX_TICKERS_PER_BATCH} tickerech...")
            time.sleep(config.REQUEST_DELAY * 2)
        else:
            time.sleep(config.REQUEST_DELAY)
    
    logger.info("=" * 60)
    logger.info(f"STATISTIKY STAHOVANI:")
    logger.info(f"  Celkem:       {stats['total']}")
    logger.info(f"  Ulozeno:      {stats['success']}")
    logger.info(f"  Chraneno:     {stats['protected']}")
    logger.info(f"  Prazdne:      {stats['empty']}")
    logger.info(f"  Chybne:       {stats['failed']}")
    logger.info("=" * 60)
    
    return stats


def run_data_collection():
    """Hlavní funkce."""
    logger.info("Market Breadth Data Collector - START (v1.3 - final)")
    logger.info("=" * 60)
    
    try:
        tickers = get_sp500_tickers()
        logger.info(f"Celkem tickeru k stazeni: {len(tickers)}")
        
        stats = download_all_tickers(tickers)
        
        # SPRÁVNÁ STATISTIKA POKRYTÍ
        effective_ok = stats['success'] + stats['protected']
        coverage_pct = (effective_ok / stats['total'] * 100) if stats['total'] > 0 else 0
        
        logger.info(f"Efektivni pokryti: {effective_ok}/{stats['total']} ({coverage_pct:.1f}%)")
        
        if effective_ok < config.MIN_VALID_TICKERS:
            logger.warning(
                f"VAROVANI: Pouze {effective_ok} validnich tickeru "
                f"(minimum: {config.MIN_VALID_TICKERS})"
            )
        
        if coverage_pct < config.MIN_COVERAGE_PERCENT:
            logger.warning(
                f"VAROVANI: Pokryti pouze {coverage_pct:.1f}% "
                f"(minimum: {config.MIN_COVERAGE_PERCENT}%)"
            )
        
        logger.info("Data Collector - HOTOVO")
        return stats
        
    except Exception as e:
        logger.error(f"KRITICKA CHYBA: {e}")
        raise


if __name__ == "__main__":
    run_data_collection()
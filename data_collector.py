"""
Data Collector - stahování dat z Yahoo Finance
Chrání stará data, používá fallback, loguje vše
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

# Nastavení loggeru (BEZ EMOJI - Windows problém!)
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT,
    datefmt=config.LOG_DATE_FORMAT,
    handlers=[
        logging.FileHandler(
            config.LOGS_DIR / f"data_collector_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
            encoding='utf-8'  # DŮLEŽITÉ pro Windows!
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def sanitize_ticker(ticker):
    """
    Opraví ticker pro Yahoo Finance (tečka → pomlčka).
    
    Příklad: BRK.B → BRK-B
    """
    return ticker.replace('.', '-')


def fetch_sp500_tickers_from_wiki():
    """
    Stáhne seznam S&P 500 tickerů z Wikipedie.
    
    Returns:
        list: Seznam tickerů, nebo None při chybě
    """
    try:
        logger.info(f"Stahuji seznam S&P 500 tickeru z: {config.SP500_WIKI_URL}")
        
        # User-Agent aby Wikipedia neblokovala (KRITICKÉ!)
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
        rows = table.find_all('tr')[1:]  # přeskočit hlavičku
        
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
    """
    Načte tickery z lokálního fallback CSV.
    """
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
    """
    Uloží tickery do fallback CSV.
    """
    try:
        df = pd.DataFrame({'ticker': tickers})
        df.to_csv(config.SP500_FALLBACK_CSV, index=False)
        logger.info(f"OK - Fallback CSV aktualizovan: {len(tickers)} tickeru")
        
    except Exception as e:
        logger.error(f"Chyba pri ukladani fallback CSV: {e}")


def get_sp500_tickers():
    """
    Získá seznam S&P 500 tickerů (Wikipedia → fallback).
    """
    # Pokus o stažení z Wikipedie
    tickers = fetch_sp500_tickers_from_wiki()
    
    if tickers:
        # Úspěch → aktualizuj fallback
        save_fallback_tickers(tickers)
        return tickers
    
    # Chyba → použij fallback
    logger.warning("VAROVANI: Wikipedia fetch failed, using fallback CSV")
    tickers = load_fallback_tickers()
    
    if not tickers:
        logger.error("KRITICKA CHYBA: Ani fallback CSV neni k dispozici!")
        raise Exception("Nelze ziskat seznam tickeru (ani Wikipedia ani fallback)")
    
    return tickers


def download_ticker_data(ticker, start_date):
    """
    Stáhne historická data pro jeden ticker.
    """
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
    Uloží data tickeru do CSV.
    """
    try:
        filepath = config.PRICES_DIR / f"{ticker}.csv"
        data.to_csv(filepath)
        logger.debug(f"Ulozeno {ticker}: {filepath}")
        
    except Exception as e:
        logger.error(f"CHYBA ukladani {ticker}: {e}")


def download_all_tickers(tickers):
    """
    Stáhne data pro všechny tickery.
    """
    start_date = datetime.now() - timedelta(days=config.HISTORY_TRADING_DAYS * 1.5)
    
    stats = {
        'total': len(tickers),
        'success': 0,
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
            save_ticker_data(clean_ticker, data)
            stats['success'] += 1
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
    logger.info(f"  Celkem:    {stats['total']}")
    logger.info(f"  Uspesne:   {stats['success']}")
    logger.info(f"  Prazdne:   {stats['empty']}")
    logger.info(f"  Chybne:    {stats['failed']}")
    logger.info("=" * 60)
    
    return stats


def run_data_collection():
    """
    Hlavní funkce.
    """
    logger.info("Market Breadth Data Collector - START")
    logger.info("=" * 60)
    
    try:
        tickers = get_sp500_tickers()
        logger.info(f"Celkem tickeru k stazeni: {len(tickers)}")
        
        stats = download_all_tickers(tickers)
        
        coverage_pct = (stats['success'] / stats['total']) * 100
        
        if stats['success'] < config.MIN_VALID_TICKERS:
            logger.warning(f"VAROVANI: Pouze {stats['success']} validnich tickeru (minimum: {config.MIN_VALID_TICKERS})")
        
        if coverage_pct < config.MIN_COVERAGE_PERCENT:
            logger.warning(f"VAROVANI: Pokryti pouze {coverage_pct:.1f}% (minimum: {config.MIN_COVERAGE_PERCENT}%)")
        
        logger.info("Data Collector - HOTOVO")
        return stats
        
    except Exception as e:
        logger.error(f"KRITICKA CHYBA: {e}")
        raise


if __name__ == "__main__":
    run_data_collection()
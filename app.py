from flask import Flask, jsonify, render_template
import yfinance as yf
import feedparser
import time
import threading
from datetime import datetime
import pytz

app = Flask(__name__)

# ── Symbol configuration ───────────────────────────────────────────────────────

SYMBOLS = {
    'indices': [
        ('^GSPC',  'S&P 500'),
        ('^IXIC',  'NASDAQ'),
        ('^DJI',   'DOW'),
        ('^FTSE',  'FTSE 100'),
        ('^N225',  'Nikkei 225'),
        ('^GDAXI', 'DAX'),
        ('^OSEBX', 'Oslo Børs'),
    ],
    'commodities': [
        ('GC=F', 'Gold'),
        ('SI=F', 'Silver'),
        ('CL=F', 'WTI Oil'),
        ('BZ=F', 'Brent'),
        ('NG=F', 'Nat Gas'),
        ('HG=F', 'Copper'),
        ('ZW=F', 'Wheat'),
        ('ZC=F', 'Corn'),
    ],
    'forex': [
        ('EURUSD=X', 'EUR/USD'),
        ('GBPUSD=X', 'GBP/USD'),
        ('USDJPY=X', 'USD/JPY'),
        ('USDNOK=X', 'USD/NOK'),
        ('EURNOK=X', 'EUR/NOK'),
        ('GBPNOK=X', 'GBP/NOK'),
        ('USDCHF=X', 'USD/CHF'),
    ],
    'crypto': [
        ('BTC-USD', 'Bitcoin'),
        ('ETH-USD', 'Ethereum'),
        ('SOL-USD', 'Solana'),
        ('XRP-USD', 'XRP'),
    ],
    'no_stocks': [
        ('EQNR.OL', 'Equinor'),
        ('DNB.OL',  'DNB Bank'),
        ('MOWI.OL', 'Mowi'),
        ('TEL.OL',  'Telenor'),
        ('NHY.OL',  'Norsk Hydro'),
        ('ORK.OL',  'Orkla'),
        ('SALM.OL', 'SalMar'),
        ('YAR.OL',  'Yara'),
    ],
    'global_stocks': [
        ('AAPL',  'Apple'),
        ('MSFT',  'Microsoft'),
        ('NVDA',  'NVIDIA'),
        ('TSLA',  'Tesla'),
        ('AMZN',  'Amazon'),
        ('META',  'Meta'),
        ('GOOGL', 'Alphabet'),
        ('JPM',   'JPMorgan'),
        ('XOM',   'ExxonMobil'),
        ('BRK-B', 'Berkshire'),
    ],
}

EARNINGS_WATCH = ['AAPL', 'MSFT', 'NVDA', 'META', 'AMZN', 'GOOGL', 'TSLA', 'JPM', 'XOM']

NEWS_FEEDS = [
    ('https://feeds.reuters.com/reuters/businessNews', 'Reuters'),
    ('https://www.cnbc.com/id/100003114/device/rss/rss.html', 'CNBC'),
    ('https://finance.yahoo.com/news/rssindex', 'Yahoo Finance'),
    ('https://feeds.marketwatch.com/marketwatch/topstories/', 'MarketWatch'),
]

# ── Simple thread-safe cache ───────────────────────────────────────────────────

_cache: dict = {}
_cache_ts: dict = {}
_lock = threading.Lock()


def get_cached(key: str, fn, ttl: int = 60):
    with _lock:
        now = time.time()
        if key not in _cache or now - _cache_ts.get(key, 0) > ttl:
            try:
                _cache[key] = fn()
                _cache_ts[key] = now
            except Exception as e:
                print(f"[fetch error] {key}: {e}")
                if key not in _cache:
                    return None
        return _cache.get(key)


# ── Data fetchers ──────────────────────────────────────────────────────────────

def fmt_price(price: float, symbol: str) -> str:
    if price is None:
        return 'N/A'
    if symbol in ('USDJPY=X',) or price > 500:
        return f"{price:,.2f}"
    elif price < 1:
        return f"{price:.4f}"
    else:
        return f"{price:,.2f}"


def fetch_all_market_data() -> dict:
    all_pairs = []
    for key, pairs in SYMBOLS.items():
        all_pairs.extend(pairs)

    sym_list = [s for s, _ in all_pairs]
    result = {}

    try:
        tickers = yf.download(
            sym_list, period='5d', interval='1d',
            progress=False, auto_adjust=True
        )
        close = tickers['Close'] if 'Close' in tickers else tickers

        for sym in sym_list:
            col = sym if sym in close.columns else None
            if col is None:
                continue
            prices = close[col].dropna()
            if len(prices) >= 2:
                price = float(prices.iloc[-1])
                prev  = float(prices.iloc[-2])
                change = price - prev
                pct    = (change / prev) * 100
                result[sym] = {
                    'price': price,
                    'change': change,
                    'pct': pct,
                    'fmt': fmt_price(price, sym),
                }
    except Exception as e:
        print(f"Batch download error: {e}")
        # Fall back to individual fetches
        for sym in sym_list:
            try:
                t = yf.Ticker(sym)
                hist = t.history(period='5d')
                if len(hist) >= 2:
                    price = float(hist['Close'].iloc[-1])
                    prev  = float(hist['Close'].iloc[-2])
                    change = price - prev
                    pct    = (change / prev) * 100
                    result[sym] = {
                        'price': price,
                        'change': change,
                        'pct': pct,
                        'fmt': fmt_price(price, sym),
                    }
            except Exception:
                pass

    return result


def fetch_news() -> list:
    articles = []
    for feed_url, source in NEWS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:7]:
                articles.append({
                    'title':     entry.get('title', '').strip(),
                    'link':      entry.get('link', '#'),
                    'published': entry.get('published', entry.get('updated', '')),
                    'source':    source,
                })
        except Exception as e:
            print(f"News error [{source}]: {e}")
    return articles[:30]


def fetch_earnings() -> list:
    result = []
    for sym in EARNINGS_WATCH:
        try:
            t = yf.Ticker(sym)
            cal = t.calendar
            if cal is None:
                continue
            # cal can be a DataFrame or dict depending on yfinance version
            date_val = None
            if hasattr(cal, 'get'):
                dates = cal.get('Earnings Date', [])
                if dates:
                    date_val = str(dates[0])[:10]
            elif hasattr(cal, 'loc'):
                try:
                    dates = cal.loc['Earnings Date']
                    date_val = str(dates.iloc[0])[:10]
                except Exception:
                    pass
            if date_val and date_val != 'NaT':
                result.append({'symbol': sym, 'date': date_val})
        except Exception:
            pass
    result.sort(key=lambda x: x['date'])
    return result


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    symbols_config = {}
    for key, pairs in SYMBOLS.items():
        symbols_config[key] = pairs
    return render_template('index.html', symbols=SYMBOLS)


@app.route('/api/market')
def api_market():
    data = get_cached('market', fetch_all_market_data, ttl=30)
    return jsonify(data or {})


@app.route('/api/news')
def api_news():
    data = get_cached('news', fetch_news, ttl=300)
    return jsonify(data or [])


@app.route('/api/earnings')
def api_earnings():
    data = get_cached('earnings', fetch_earnings, ttl=3600)
    return jsonify(data or [])


@app.route('/api/all')
def api_all():
    return jsonify({
        'market':    get_cached('market',   fetch_all_market_data, ttl=30)  or {},
        'news':      get_cached('news',     fetch_news,            ttl=300) or [],
        'earnings':  get_cached('earnings', fetch_earnings,        ttl=3600) or [],
        'timestamp': datetime.utcnow().isoformat(),
    })


# ── Background preload ─────────────────────────────────────────────────────────

def _preload():
    time.sleep(0.5)
    print("[bloomberg] Preloading market data …")
    fetch_all_market_data()
    print("[bloomberg] Preloading news …")
    fetch_news()
    print("[bloomberg] Preloading earnings …")
    fetch_earnings()
    print("[bloomberg] Ready.")

threading.Thread(target=_preload, daemon=True).start()

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 8080))
    app.run(debug=False, host='0.0.0.0', port=port)

import calendar
import html
import re
import threading
import time
import warnings
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta

import feedparser
import pandas as pd
import requests
import yfinance as yf
from flask import Flask, jsonify, render_template, request

warnings.filterwarnings('ignore')
app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True

# ── Symbols ────────────────────────────────────────────────────────────────────

SYMBOLS = {
    'indices': [
        ('^GSPC',     'S&P 500'),
        ('^IXIC',     'Nasdaq'),
        ('^DJI',      'Dow Jones'),
        ('^RUT',      'Russell 2000'),
        ('OSEBX.OL',  'Oslo Børs (OSEBX)'),
        ('^STOXX50E', 'Euro Stoxx 50'),
        ('^GDAXI',    'DAX'),
        ('^FTSE',     'FTSE 100'),
        ('^N225',     'Nikkei 225'),
        ('^HSI',      'Hang Seng'),
        ('^VIX',      'VIX'),
    ],
    'commodities': [
        ('BZ=F', 'Brent'),
        ('CL=F', 'WTI'),
        ('NG=F', 'Naturgass'),
        ('GC=F', 'Gull'),
        ('SI=F', 'Sølv'),
        ('PL=F', 'Platina'),
        ('HG=F', 'Kobber'),
        ('ALI=F', 'Aluminium'),
        ('ZW=F', 'Hvete'),
        ('KC=F', 'Kaffe'),
        ('CC=F', 'Kakao'),
    ],
    'forex': [
        ('USDNOK=X', 'USD/NOK'),
        ('EURNOK=X', 'EUR/NOK'),
        ('GBPNOK=X', 'GBP/NOK'),
        ('SEKNOK=X', 'SEK/NOK'),
        ('DKKNOK=X', 'DKK/NOK'),
        ('CHFNOK=X', 'CHF/NOK'),
        ('EURSEK=X', 'EUR/SEK'),
        ('EURUSD=X', 'EUR/USD'),
        ('GBPUSD=X', 'GBP/USD'),
        ('USDJPY=X', 'USD/JPY'),
        ('DX-Y.NYB', 'Dollarindeks'),
    ],
    'crypto': [
        ('BTC-USD',  'Bitcoin'),
        ('ETH-USD',  'Ethereum'),
        ('SOL-USD',  'Solana'),
        ('XRP-USD',  'XRP'),
        ('BNB-USD',  'BNB'),
        ('DOGE-USD', 'Dogecoin'),
        ('ADA-USD',  'Cardano'),
        ('AVAX-USD', 'Avalanche'),
        ('LINK-USD', 'Chainlink'),
        ('LTC-USD',  'Litecoin'),
        ('DOT-USD',  'Polkadot'),
    ],
    'oslo': [
        ('EQNR.OL',  'Equinor'),
        ('DNB.OL',   'DNB Bank'),
        ('NHY.OL',   'Norsk Hydro'),
        ('TEL.OL',   'Telenor'),
        ('MOWI.OL',  'Mowi'),
        ('AKRBP.OL', 'Aker BP'),
        ('KOG.OL',   'Kongsberg Gruppen'),
        ('YAR.OL',   'Yara'),
        ('ORK.OL',   'Orkla'),
        ('SALM.OL',  'SalMar'),
        ('GJF.OL',   'Gjensidige'),
        ('STB.OL',   'Storebrand'),
        ('SUBC.OL',  'Subsea 7'),
        ('VAR.OL',   'Vår Energi'),
        ('FRO.OL',   'Frontline'),
        ('NOD.OL',   'Nordic Semiconductor'),
        ('TOM.OL',   'Tomra'),
        ('AKER.OL',  'Aker'),
        ('VEND.OL',  'Vend Marketplaces'),
        ('TGS.OL',   'TGS'),
        ('HAFNI.OL', 'Hafnia'),
        ('WAWI.OL',  'Wallenius Wilhelmsen'),
        ('BAKKA.OL', 'Bakkafrost'),
        ('LSG.OL',   'Lerøy Seafood'),
        ('NAS.OL',   'Norwegian Air'),
        ('AUTO.OL',  'AutoStore'),
        ('SCATC.OL', 'Scatec'),
        ('MPCC.OL',  'MPC Container'),
    ],
    'us': [
        ('AAPL',  'Apple'),
        ('MSFT',  'Microsoft'),
        ('NVDA',  'NVIDIA'),
        ('GOOGL', 'Alphabet'),
        ('AMZN',  'Amazon'),
        ('META',  'Meta'),
        ('TSLA',  'Tesla'),
        ('AVGO',  'Broadcom'),
        ('ORCL',  'Oracle'),
        ('NFLX',  'Netflix'),
        ('AMD',   'AMD'),
        ('PLTR',  'Palantir'),
        ('CRM',   'Salesforce'),
        ('ADBE',  'Adobe'),
        ('ASML',  'ASML'),
        ('JPM',   'JPMorgan'),
        ('V',     'Visa'),
        ('MA',    'Mastercard'),
        ('GS',    'Goldman Sachs'),
        ('BAC',   'Bank of America'),
        ('BRK-B', 'Berkshire Hathaway'),
        ('XOM',   'ExxonMobil'),
        ('CVX',   'Chevron'),
        ('LLY',   'Eli Lilly'),
        ('NVO',   'Novo Nordisk'),
        ('UNH',   'UnitedHealth'),
        ('JNJ',   'Johnson & Johnson'),
        ('WMT',   'Walmart'),
        ('COST',  'Costco'),
        ('KO',    'Coca-Cola'),
        ('CAT',   'Caterpillar'),
        ('BA',    'Boeing'),
    ],
    'sectors': [
        ('XLK',  'Teknologi'),
        ('XLF',  'Finans'),
        ('XLE',  'Energi'),
        ('XLV',  'Helse'),
        ('XLI',  'Industri'),
        ('XLY',  'Forbruk (syklisk)'),
        ('XLP',  'Dagligvarer'),
        ('XLC',  'Kommunikasjon'),
        ('XLRE', 'Eiendom'),
        ('XLU',  'Kraft'),
        ('XLB',  'Materialer'),
    ],
}

ALL_SYMBOLS = [s for group in SYMBOLS.values() for s, _ in group]
STOCK_SYMBOLS = [s for s, _ in SYMBOLS['oslo'] + SYMBOLS['us']]
FUNDAMENTAL_SYMBOLS = STOCK_SYMBOLS + [s for s, _ in SYMBOLS['crypto']]
INSIDER_SYMBOLS = ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'TSLA', 'AVGO', 'JPM', 'LLY']

CORR_SYMBOLS = [('^GSPC', 'S&P 500'), ('OSEBX.OL', 'OSEBX'), ('BZ=F', 'Brent'),
                ('GC=F', 'Gull'), ('BTC-USD', 'Bitcoin'), ('USDNOK=X', 'USD/NOK'),
                ('^TNX', 'US 10Y')]

YIELD_SYMBOLS = [('^IRX', '3M', 0.25), ('^FVX', '5Y', 5), ('^TNX', '10Y', 10), ('^TYX', '30Y', 30)]

SYMBOL_RE = re.compile(r'^[A-Za-z0-9.\-=^&]{1,20}$')

# ── News ───────────────────────────────────────────────────────────────────────

NEWS_FEEDS = [
    ('https://www.cnbc.com/id/100003114/device/rss/rss.html',  'CNBC',        False),
    ('https://www.cnbc.com/id/10000664/device/rss/rss.html',   'CNBC',        False),
    ('https://feeds.bloomberg.com/markets/news.rss',           'Bloomberg',   False),
    ('https://www.ft.com/markets?format=rss',                  'FT',          False),
    ('https://feeds.marketwatch.com/marketwatch/topstories/',  'MarketWatch', False),
    ('https://www.investing.com/rss/news_25.rss',              'Investing',   False),
    ('https://e24.no/rss2',                                    'E24',         True),
]
NEWS_MAX_AGE_H = 36

# Prefix match on word boundaries, so 'aksje' also hits 'aksjer'/'aksjen'.
E24_KEYWORDS = [
    'aksje', 'børs', 'osebx', 'kursfall', 'kursoppgang', 'kursmål', 'short', 'megler',
    'analytiker', 'fond', 'etf', 'indeks', 'rente', 'inflasjon', 'norges bank',
    'sentralbank', 'bnp', 'resesjon', 'arbeidsledig', 'konsumpris', 'kpi',
    'pengepolitikk', 'statsbudsjett', 'olje', 'gass', 'brent', 'opec', 'råvare', 'gull',
    'equinor', 'aker', 'hydro', 'dnb', 'krone', 'dollar', 'euro', 'valuta', 'resultat',
    'kvartal', 'ebitda', 'omsetning', 'utbytte', 'emisjon', 'fusjon', 'oppkjøp',
    'notering', 'børsnotering', 'investor', 'obligasjon', 'kreditt', 'bank', 'sparebank',
    'oljefond', 'nbim', 'fed', 'fomc', 'ecb', 'imf', 'toll', 'handelskrig', 'sanksjon',
    'konkurs', 'laks', 'strømpris', 'boligpris', 'milliard',
]
E24_RE = re.compile(r'\b(' + '|'.join(re.escape(k) for k in E24_KEYWORDS) + r')', re.I)

# ── Macro calendar (times in Oslo time) ───────────────────────────────────────
# Verified against federalreserve.gov, ecb.europa.eu, norges-bank.no and bls.gov.

MACRO_CALENDAR = [
    {'date': '2026-09-24', 'time': '10:00', 'region': 'NO', 'event': 'Norges Bank rentebeslutning', 'cat': 'SENTRALBANK', 'imp': 'H'},
    {'date': '2026-10-14', 'time': '14:30', 'region': 'US', 'event': 'KPI (CPI) september',         'cat': 'INFLASJON',   'imp': 'H'},
    {'date': '2026-10-28', 'time': '19:00', 'region': 'US', 'event': 'FOMC rentebeslutning',        'cat': 'SENTRALBANK', 'imp': 'H'},
    {'date': '2026-10-29', 'time': '14:15', 'region': 'EU', 'event': 'ECB rentebeslutning',         'cat': 'SENTRALBANK', 'imp': 'H'},
    {'date': '2026-11-05', 'time': '10:00', 'region': 'NO', 'event': 'Norges Bank rentebeslutning', 'cat': 'SENTRALBANK', 'imp': 'H'},
    {'date': '2026-11-06', 'time': '14:30', 'region': 'US', 'event': 'Sysselsetting (NFP) oktober', 'cat': 'ARBEID',      'imp': 'H'},
    {'date': '2026-11-10', 'time': '14:30', 'region': 'US', 'event': 'KPI (CPI) oktober',           'cat': 'INFLASJON',   'imp': 'H'},
    {'date': '2026-12-04', 'time': '14:30', 'region': 'US', 'event': 'Sysselsetting (NFP) november','cat': 'ARBEID',      'imp': 'H'},
    {'date': '2026-12-09', 'time': '20:00', 'region': 'US', 'event': 'FOMC rentebeslutning + prognoser', 'cat': 'SENTRALBANK', 'imp': 'H'},
    {'date': '2026-12-10', 'time': '14:30', 'region': 'US', 'event': 'KPI (CPI) november',          'cat': 'INFLASJON',   'imp': 'H'},
    {'date': '2026-12-17', 'time': '10:00', 'region': 'NO', 'event': 'Norges Bank rentebeslutning + PPR', 'cat': 'SENTRALBANK', 'imp': 'H'},
    {'date': '2026-12-17', 'time': '14:15', 'region': 'EU', 'event': 'ECB rentebeslutning + prognoser', 'cat': 'SENTRALBANK', 'imp': 'H'},
]

# ── Stale-while-revalidate cache ───────────────────────────────────────────────
# Callers never wait for a refresh once a value exists; only the very first
# fetch of a key blocks, and only callers of that same key.

class Cache:
    MAX_KEYS = 400

    def __init__(self):
        self._data, self._ts = {}, {}
        self._key_locks = defaultdict(threading.Lock)
        self._refreshing = set()
        self._meta = threading.Lock()

    def _store(self, key, value):
        if value or key not in self._data:
            self._data[key] = value
            self._ts[key] = time.time()
        if len(self._data) > self.MAX_KEYS:
            for k in sorted(self._ts, key=self._ts.get)[:50]:
                self._data.pop(k, None)
                self._ts.pop(k, None)

    def _run(self, key, fn):
        try:
            self._store(key, fn())
        except Exception as e:
            print(f'[cache] {key}: {e}')
        finally:
            with self._meta:
                self._refreshing.discard(key)

    def get(self, key, fn, ttl):
        if key in self._data:
            if time.time() - self._ts[key] > ttl:
                with self._meta:
                    start = key not in self._refreshing
                    self._refreshing.add(key)
                if start:
                    threading.Thread(target=self._run, args=(key, fn), daemon=True).start()
            return self._data[key]
        with self._key_locks[key]:
            if key not in self._data:
                with self._meta:
                    self._refreshing.add(key)
                self._run(key, fn)
            return self._data.get(key)

    def age(self, key):
        return time.time() - self._ts[key] if key in self._ts else None


cache = Cache()

# ── Helpers ────────────────────────────────────────────────────────────────────

def _num(v):
    try:
        f = float(v)
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


def _close_frame(data, syms):
    if data is None or data.empty:
        return pd.DataFrame()
    if isinstance(data.columns, pd.MultiIndex):
        return data['Close']
    return data[['Close']].rename(columns={'Close': syms[0]})


# yf.download keeps module-global state and deadlocks when called from
# several threads at once, so all bulk downloads go through this lock.
_download_lock = threading.Lock()


def yf_download(syms, **kw):
    with _download_lock:
        return yf.download(syms, progress=False, auto_adjust=True, timeout=20, **kw)


def _download_prices(syms):
    out = {}
    try:
        data = yf_download(syms, period='1mo', interval='1d')
        close = _close_frame(data, syms)
        for s in syms:
            if s not in close.columns:
                continue
            p = close[s].dropna()
            if len(p) < 2:
                continue
            last, prev = float(p.iloc[-1]), float(p.iloc[-2])
            out[s] = {
                'price':  last,
                'change': last - prev,
                'pct':    (last - prev) / prev * 100 if prev else 0.0,
                'date':   str(p.index[-1])[:10],
                'prev_date': str(p.index[-2])[:10],
                'spark':  [round(float(x), 6) for x in p.iloc[-22:]],
            }
    except Exception as e:
        print(f'[prices] {syms[:3]}…: {e}')
    return out


def _skipped_weekdays(q):
    start = date.fromisoformat(q['prev_date']) + timedelta(days=1)
    end = date.fromisoformat(q['date']) - timedelta(days=1)
    return start <= end and len(pd.bdate_range(start, end)) > 0


def _fill_session_gaps(result):
    # Yahoo's daily history sometimes drops a whole session (all of Oslo Børs on
    # 2026-10-19), so "prev" would be two sessions back. Hourly data still has
    # the missing day; use its last close as the previous close.
    suspects = [s for s, q in result.items() if _skipped_weekdays(q)]
    groups = defaultdict(list)
    for s in suspects:
        groups[s.rsplit('.', 1)[1] if '.' in s else ''].append(s)
    for syms in groups.values():
        try:
            close = _close_frame(yf_download(syms, period='5d', interval='1h'), syms)
        except Exception as e:
            print(f'[gaps] {syms[:3]}…: {e}')
            continue
        for s in syms:
            if s not in close.columns:
                continue
            q = result[s]
            p = close[s].dropna()
            by_day = p.groupby(p.index.date).last()
            lo, hi = date.fromisoformat(q['prev_date']), date.fromisoformat(q['date'])
            between = [d for d in by_day.index if lo < d < hi]
            if not between:
                continue
            prev = float(by_day[between[-1]])
            q.update(prev_date=str(between[-1]), change=q['price'] - prev,
                     pct=(q['price'] - prev) / prev * 100 if prev else 0.0)
    if suspects:
        print(f'[gaps] sjekket {len(suspects)} symboler med manglende handelsdag')


def fetch_prices(syms, chunk=20):
    result = {}
    for i in range(0, len(syms), chunk):
        result.update(_download_prices(syms[i:i + chunk]))
    missing = [s for s in syms if s not in result]
    for s in missing:
        result.update(_download_prices([s]))
    _fill_session_gaps(result)
    return result

# ── Fetchers ───────────────────────────────────────────────────────────────────

def fetch_market():
    data = fetch_prices(ALL_SYMBOLS)
    print(f'[market] {len(data)}/{len(ALL_SYMBOLS)} symboler')
    return data


INFO_FIELDS = ['shortName', 'longName', 'currency', 'marketCap', 'trailingPE', 'forwardPE',
               'trailingEps', 'enterpriseToEbitda', 'priceToBook', 'beta', 'dividendYield',
               'shortPercentOfFloat', 'targetMeanPrice', 'recommendationKey',
               'numberOfAnalystOpinions', 'fiftyTwoWeekLow', 'fiftyTwoWeekHigh',
               'volume', 'averageVolume', 'sector', 'industry']


# yfinance fetches a cookie/crumb lazily; concurrent first calls race and
# Yahoo answers 401 "Invalid Crumb", so the first one runs alone.
_session_ready = threading.Event()
_session_lock = threading.Lock()


def ensure_yahoo_session():
    if _session_ready.is_set():
        return
    with _session_lock:
        if not _session_ready.is_set():
            try:
                yf.Ticker('AAPL').info
            except Exception as e:
                print(f'[session] {e}')
            _session_ready.set()


def _info(sym):
    ensure_yahoo_session()
    try:
        info = yf.Ticker(sym).info or {}
    except Exception as e:
        print(f'[info] {sym}: {e}')
        return sym, None
    return sym, {k: info.get(k) for k in INFO_FIELDS}


def fetch_fundamentals():
    with ThreadPoolExecutor(max_workers=6) as ex:
        results = dict(ex.map(_info, FUNDAMENTAL_SYMBOLS))
    for sym in [s for s, v in results.items() if not v or v.get('marketCap') is None]:
        _, v = _info(sym)
        if v:
            results[sym] = v
    out = {s: v for s, v in results.items() if v}
    print(f'[fundamentals] {len(out)}/{len(FUNDAMENTAL_SYMBOLS)}')
    return out


def _earnings_date(sym):
    ensure_yahoo_session()
    try:
        cal = yf.Ticker(sym).calendar
        dates = cal.get('Earnings Date') if isinstance(cal, dict) else None
        if dates:
            return sym, str(dates[0])[:10]
    except Exception:
        pass
    return sym, None


def fetch_earnings():
    names = dict(SYMBOLS['oslo'] + SYMBOLS['us'])
    with ThreadPoolExecutor(max_workers=6) as ex:
        results = list(ex.map(_earnings_date, STOCK_SYMBOLS))
    cutoff = (date.today() - timedelta(days=3)).isoformat()
    horizon = (date.today() + timedelta(days=90)).isoformat()
    rows = [{'symbol': s, 'name': names.get(s, s), 'date': d}
            for s, d in results if d and cutoff <= d <= horizon]
    return sorted(rows, key=lambda r: r['date'])


def fetch_yields():
    syms = [s for s, _, _ in YIELD_SYMBOLS]
    prices = fetch_prices(syms)
    out = []
    for s, label, years in YIELD_SYMBOLS:
        p = prices.get(s)
        out.append({'sym': s, 'label': label, 'years': years,
                    'value': round(p['price'], 3) if p else None,
                    'change': round(p['change'], 3) if p else None,
                    'month_ago': round(p['spark'][0], 3) if p else None})
    return out


def _parse_entry_time(entry):
    t = entry.get('published_parsed') or entry.get('updated_parsed')
    return calendar.timegm(t) if t else None


def _fetch_feed(feed):
    url, source, needs_filter = feed
    try:
        r = requests.get(url, timeout=8, headers={'User-Agent': 'Mozilla/5.0 NylundTerminal'})
        parsed = feedparser.parse(r.content)
    except Exception as e:
        print(f'[news] {source}: {e}')
        return []
    items = []
    for e in parsed.entries[:40]:
        title = html.unescape(e.get('title', '')).strip()
        link = e.get('link', '')
        ts = _parse_entry_time(e)
        if not title or not link.startswith('http') or not ts:
            continue
        if needs_filter:
            tags = ' '.join(t.get('term', '') for t in e.get('tags', []))
            if not E24_RE.search(f'{title} {tags}'):
                continue
        items.append({'title': title, 'link': link, 'source': source, 'ts': ts})
    return items[:15]


def fetch_news():
    with ThreadPoolExecutor(max_workers=len(NEWS_FEEDS)) as ex:
        batches = list(ex.map(_fetch_feed, NEWS_FEEDS))
    min_ts = time.time() - NEWS_MAX_AGE_H * 3600
    seen, out = set(), []
    for item in sorted((i for b in batches for i in b), key=lambda i: -i['ts']):
        key = re.sub(r'\W+', '', item['title'].lower())[:80]
        if key in seen or item['ts'] < min_ts:
            continue
        seen.add(key)
        out.append(item)
    return out[:60]


def _insider(sym):
    ensure_yahoo_session()
    rows = []
    try:
        df = yf.Ticker(sym).insider_transactions
    except Exception:
        return rows
    if df is None or df.empty:
        return rows
    for _, r in df.head(25).iterrows():
        text = str(r.get('Text', '')).lower()
        if 'sale' in text or 'sold' in text:
            kind = 'SALG'
        elif 'purchase' in text or 'bought' in text:
            kind = 'KJØP'
        elif 'option' in text or 'exercise' in text:
            kind = 'OPSJON'
        elif 'gift' in text:
            kind = 'GAVE'
        elif 'award' in text or 'grant' in text:
            kind = 'TILDELING'
        else:
            continue
        rows.append({
            'date':     str(r.get('Start Date', ''))[:10],
            'symbol':   sym,
            'person':   str(r.get('Insider', '')).title(),
            'position': str(r.get('Position', '') or ''),
            'type':     kind,
            'shares':   _num(r.get('Shares')),
            'value':    _num(r.get('Value')),
        })
    return rows


def fetch_insider():
    with ThreadPoolExecutor(max_workers=5) as ex:
        trades = [t for batch in ex.map(_insider, INSIDER_SYMBOLS) for t in batch]
    trades.sort(key=lambda t: t['date'], reverse=True)
    return trades[:40]


def fetch_correlation():
    syms = [s for s, _ in CORR_SYMBOLS]
    df = yf_download(syms, period='6mo', interval='1d')
    close = _close_frame(df, syms).ffill()
    rets = close.pct_change(fill_method=None).dropna(how='all')
    corr = rets.corr()
    labels = [l for s, l in CORR_SYMBOLS if s in corr.columns]
    present = [s for s, _ in CORR_SYMBOLS if s in corr.columns]
    matrix = [[round(float(corr.loc[a, b]), 2) for b in present] for a in present]
    return {'labels': labels, 'matrix': matrix}


def _sma(values, n):
    return values.rolling(n).mean()


def _rsi(close, n=14):
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return 100 - 100 / (1 + gain / loss)


CHART_WINDOWS = {'1mo': 31, '3mo': 92, '6mo': 183, 'ytd': None, '1y': 366, '2y': 731, '5y': 1827}


def fetch_chart(symbol, period):
    days = CHART_WINDOWS[period]
    start_window = date(date.today().year, 1, 1) if days is None else date.today() - timedelta(days=days)
    hist = yf.Ticker(symbol).history(start=start_window - timedelta(days=320),
                                     interval='1d', auto_adjust=True)
    if hist.empty:
        return {'error': 'Ingen data'}
    c = hist['Close']
    frame = pd.DataFrame({
        'open': hist['Open'], 'high': hist['High'], 'low': hist['Low'], 'close': c,
        'volume': hist['Volume'], 'ma20': _sma(c, 20), 'ma50': _sma(c, 50),
        'ma200': _sma(c, 200), 'rsi': _rsi(c),
    })
    frame = frame[frame.index.date >= start_window]

    def col(name, digits=4):
        return [None if pd.isna(v) else round(float(v), digits) for v in frame[name]]

    return {
        'symbol': symbol,
        'dates':  [str(d)[:10] for d in frame.index],
        'open': col('open'), 'high': col('high'), 'low': col('low'), 'close': col('close'),
        'volume': [0 if pd.isna(v) else int(v) for v in frame['volume']],
        'ma20': col('ma20'), 'ma50': col('ma50'), 'ma200': col('ma200'), 'rsi': col('rsi', 2),
    }


SEARCH_TYPES = {'EQUITY', 'ETF', 'INDEX', 'CRYPTOCURRENCY', 'CURRENCY', 'FUTURE', 'MUTUALFUND'}


def search_symbols(q):
    res = yf.Search(q, max_results=12, news_count=0, enable_fuzzy_query=True)
    out = []
    for item in res.quotes:
        sym = item.get('symbol')
        if not sym or item.get('quoteType') not in SEARCH_TYPES or not SYMBOL_RE.match(sym):
            continue
        out.append({
            'symbol':   sym,
            'name':     item.get('longname') or item.get('shortname') or sym,
            'exchange': item.get('exchDisp') or item.get('exchange') or '',
            'type':     item.get('typeDisp') or item.get('quoteType'),
        })
    return out


def fetch_quote(sym):
    ensure_yahoo_session()
    t = yf.Ticker(sym)
    info = t.info or {}
    hist = t.history(period='5d', interval='1d', auto_adjust=True)
    if hist.empty and not info.get('regularMarketPrice'):
        return {'error': 'Fant ikke symbolet'}
    closes = hist['Close'].dropna()
    price = float(closes.iloc[-1]) if len(closes) else _num(info.get('regularMarketPrice'))
    prev = float(closes.iloc[-2]) if len(closes) >= 2 else _num(info.get('previousClose'))
    summary = (info.get('longBusinessSummary') or '').strip()
    out = {k: info.get(k) for k in INFO_FIELDS}
    out.update({
        'symbol':   sym,
        'name':     info.get('longName') or info.get('shortName') or sym,
        'exchange': info.get('fullExchangeName') or info.get('exchange') or '',
        'type':     info.get('quoteType', ''),
        'country':  info.get('country'),
        'website':  info.get('website') if str(info.get('website', '')).startswith('http') else None,
        'summary':  summary[:700] + ('…' if len(summary) > 700 else ''),
        'price':    price,
        'change':   price - prev if price is not None and prev else None,
        'pct':      (price - prev) / prev * 100 if price is not None and prev else None,
        'dayLow':   info.get('dayLow'),
        'dayHigh':  info.get('dayHigh'),
    })
    return out

# ── Routes ─────────────────────────────────────────────────────────────────────

def _valid_symbol(sym):
    return bool(SYMBOL_RE.match(sym or ''))


@app.route('/')
def index():
    return render_template('index.html', symbols=SYMBOLS)


@app.route('/api/market')
def api_market():
    return jsonify({'data': cache.get('market', fetch_market, ttl=30) or {},
                    'age': cache.age('market')})


@app.route('/api/fundamentals')
def api_fundamentals():
    return jsonify(cache.get('fundamentals', fetch_fundamentals, ttl=3600) or {})


@app.route('/api/news')
def api_news():
    return jsonify(cache.get('news', fetch_news, ttl=120) or [])


@app.route('/api/earnings')
def api_earnings():
    return jsonify(cache.get('earnings', fetch_earnings, ttl=6 * 3600) or [])


@app.route('/api/yields')
def api_yields():
    return jsonify(cache.get('yields', fetch_yields, ttl=300) or [])


@app.route('/api/macro')
def api_macro():
    cutoff = (date.today() - timedelta(days=10)).isoformat()
    return jsonify([e for e in MACRO_CALENDAR if e['date'] >= cutoff])


@app.route('/api/insider')
def api_insider():
    return jsonify(cache.get('insider', fetch_insider, ttl=3 * 3600) or [])


@app.route('/api/correlation')
def api_correlation():
    return jsonify(cache.get('correlation', fetch_correlation, ttl=6 * 3600)
                   or {'labels': [], 'matrix': []})


@app.route('/api/chart/<path:symbol>')
def api_chart(symbol):
    period = request.args.get('period', '6mo')
    if period not in CHART_WINDOWS or not _valid_symbol(symbol):
        return jsonify({'error': 'Ugyldig forespørsel'}), 400
    return jsonify(cache.get(f'chart:{symbol}:{period}',
                             lambda: fetch_chart(symbol, period), ttl=300))


@app.route('/api/search')
def api_search():
    q = request.args.get('q', '').strip()[:40]
    if len(q) < 1:
        return jsonify([])
    return jsonify(cache.get(f'search:{q.lower()}', lambda: search_symbols(q), ttl=3600) or [])


@app.route('/api/quote/<path:symbol>')
def api_quote(symbol):
    if not _valid_symbol(symbol):
        return jsonify({'error': 'Ugyldig symbol'}), 400
    return jsonify(cache.get(f'quote:{symbol}', lambda: fetch_quote(symbol), ttl=180))


@app.route('/api/quotes')
def api_quotes():
    syms = sorted({s for s in request.args.get('s', '').split(',') if _valid_symbol(s)})[:40]
    if not syms:
        return jsonify({})
    return jsonify(cache.get('quotes:' + ','.join(syms), lambda: fetch_prices(syms), ttl=60) or {})

# ── Warm-up ────────────────────────────────────────────────────────────────────

def _preload():
    for key, fn, ttl in [('market', fetch_market, 30), ('news', fetch_news, 120),
                         ('yields', fetch_yields, 300), ('fundamentals', fetch_fundamentals, 3600),
                         ('earnings', fetch_earnings, 6 * 3600), ('insider', fetch_insider, 3 * 3600),
                         ('correlation', fetch_correlation, 6 * 3600)]:
        cache.get(key, fn, ttl)
    print('[nylund] Klar.')


threading.Thread(target=_preload, daemon=True).start()

if __name__ == '__main__':
    import os
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), threaded=True)

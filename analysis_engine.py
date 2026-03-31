#!/usr/bin/env python3
"""
GeoMacro Intel v5 — Quantitative Analysis Engine
Kahneman Framework: hipótesis → pre-mortem → calibración
"""

import os, json, math, time, datetime, statistics
from urllib.request import urlopen, Request
from urllib.error import URLError
from urllib.parse import urlencode

ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
FRED_KEY      = os.environ.get("FRED_API_KEY", "")
NEWS_KEY      = os.environ.get("NEWS_API_KEY", "")

NOW      = datetime.datetime.utcnow()
TODAY    = NOW.strftime("%Y-%m-%d")
QUARTER  = f"Q{math.ceil(NOW.month/3)} {NOW.year}"
DATE_ES  = NOW.strftime("%d/%m/%Y %H:%M UTC")

ASSETS = [
    {"id":"GC=F",  "name":"Oro",         "type":"precious", "currency":"USD", "unit":"oz"},
    {"id":"SI=F",  "name":"Plata",        "type":"precious", "currency":"USD", "unit":"oz"},
    {"id":"CL=F",  "name":"Crudo WTI",    "type":"energy",   "currency":"USD", "unit":"bbl"},
    {"id":"NG=F",  "name":"Gas Natural",  "type":"energy",   "currency":"USD", "unit":"MMBtu"},
    {"id":"HG=F",  "name":"Cobre",        "type":"industrial","currency":"USD", "unit":"lb"},
    {"id":"ALI=F", "name":"Aluminio",     "type":"industrial","currency":"USD", "unit":"t"},
    {"id":"LIT",   "name":"ETF Litio",    "type":"critical", "currency":"USD", "unit":"share"},
    {"id":"COPX",  "name":"ETF Cobre",    "type":"industrial","currency":"USD", "unit":"share"},
]

REGIONS = [
    {"id":"VNM","name":"Vietnam",     "region":"SE Asia"},
    {"id":"IND","name":"India",       "region":"South Asia"},
    {"id":"POL","name":"Polonia",     "region":"CEE"},
    {"id":"BRA","name":"Brasil",      "region":"LATAM"},
    {"id":"SAU","name":"Arabia S.",   "region":"MENA"},
    {"id":"NGA","name":"Nigeria",     "region":"África Sub."},
]

# ── HTTP helper ───────────────────────────────────────
def fetch(url, headers=None, timeout=15):
    try:
        req = Request(url, headers=headers or {"User-Agent": "GeoMacroIntel/5.0"})
        with urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"  WARN fetch({url[:60]}...): {e}")
        return None

def post_json(url, payload, headers):
    import urllib.request
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"  ERROR post({url}): {e}")
        return None

# ── CAPA 1: Fetch de datos ────────────────────────────

def fetch_yahoo_prices(ticker, period="2y"):
    """Descarga serie histórica de Yahoo Finance (no oficial, sin key)."""
    end   = int(time.time())
    start = end - (730 * 86400)  # 2 años
    url   = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
             f"?interval=1d&period1={start}&period2={end}")
    d = fetch(url)
    if not d:
        return None
    try:
        result = d["chart"]["result"][0]
        closes = result["indicators"]["quote"][0]["close"]
        times  = result["timestamp"]
        prices = [(t, c) for t, c in zip(times, closes) if c is not None]
        return prices
    except Exception as e:
        print(f"  WARN Yahoo {ticker}: {e}")
        return None

def fetch_fred(series_id):
    """Fetch de serie FRED — tipos, DXY, yield curve, M2."""
    if not FRED_KEY:
        return None
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit=60")
    d = fetch(url)
    if not d:
        return None
    try:
        obs = [(o["date"], float(o["value"])) for o in d["observations"]
               if o["value"] not in (".", "")]
        return obs
    except:
        return None

def fetch_world_bank_gdp(iso):
    url = (f"https://api.worldbank.org/v2/country/{iso}/indicator/"
           f"NY.GDP.MKTP.KD.ZG?format=json&mrv=4&per_page=4")
    d = fetch(url)
    if not d or not d[1]:
        return None
    vals = [o["value"] for o in d[1] if o["value"] is not None]
    return vals  # [más reciente, ..., más antiguo]

def fetch_fx():
    d = fetch("https://api.frankfurter.app/latest?from=USD&to=EUR,INR,BRL,MXN,PLN,AED,THB")
    return d.get("rates", {}) if d else {}

def fetch_eia_oil_inventory():
    """EIA inventarios crudos EEUU (proxy de supply/demand global)."""
    if not FRED_KEY:
        return None
    obs = fetch_fred("WCRSTUS1")  # Crude Oil Stocks
    if not obs or len(obs) < 2:
        return None
    latest = obs[0][1]
    prev   = obs[1][1]
    avg5y  = statistics.mean([o[1] for o in obs[:260]]) if len(obs) >= 260 else None
    return {"latest": latest, "prev": prev, "avg5y": avg5y,
            "vs_avg": round((latest / avg5y - 1) * 100, 1) if avg5y else None}

# ── CAPA 2: Indicadores técnicos (matemática pura) ───

def sma(prices, n):
    if len(prices) < n:
        return None
    return sum(prices[-n:]) / n

def ema(prices, n):
    if len(prices) < n:
        return None
    k = 2 / (n + 1)
    e = sum(prices[:n]) / n
    for p in prices[n:]:
        e = p * k + e * (1 - k)
    return e

def rsi(prices, n=14):
    if len(prices) < n + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(prices)):
        d = prices[i] - prices[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains[-n:]) / n
    avg_l = sum(losses[-n:]) / n
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return round(100 - 100 / (1 + rs), 2)

def macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal:
        return None, None
    e_fast = ema(prices, fast)
    e_slow = ema(prices, slow)
    macd_line = e_fast - e_slow
    # Signal line: EMA of MACD (simplified)
    macd_vals = []
    for i in range(slow, len(prices) + 1):
        ef = ema(prices[:i], fast)
        es = ema(prices[:i], slow)
        if ef and es:
            macd_vals.append(ef - es)
    sig = ema(macd_vals, signal) if len(macd_vals) >= signal else None
    hist = round(macd_line - sig, 4) if sig else None
    return round(macd_line, 4), hist

def bollinger(prices, n=20, std_mult=2):
    if len(prices) < n:
        return None, None, None
    window = prices[-n:]
    mid    = sum(window) / n
    std    = statistics.stdev(window)
    return round(mid - std_mult * std, 4), round(mid, 4), round(mid + std_mult * std, 4)

def atr(highs, lows, closes, n=14):
    """Average True Range — proxy de volatilidad real."""
    trs = []
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i],
                 abs(highs[i] - closes[i-1]),
                 abs(lows[i]  - closes[i-1]))
        trs.append(tr)
    if len(trs) < n:
        return None
    return round(sum(trs[-n:]) / n, 4)

def annualized_volatility(closes, window=126):
    """Volatilidad anualizada (6 meses ~ 126 días hábiles)."""
    if len(closes) < window + 1:
        closes_w = closes
    else:
        closes_w = closes[-window:]
    returns = [math.log(closes_w[i] / closes_w[i-1])
               for i in range(1, len(closes_w))
               if closes_w[i-1] > 0]
    if len(returns) < 5:
        return None
    return round(statistics.stdev(returns) * math.sqrt(252) * 100, 2)

def max_drawdown(closes, window=126):
    """Máximo drawdown en el período."""
    c = closes[-window:] if len(closes) >= window else closes
    peak = c[0]
    mdd  = 0
    for p in c:
        if p > peak:
            peak = p
        dd = (peak - p) / peak
        if dd > mdd:
            mdd = dd
    return round(mdd * 100, 2)

def var_parametric(closes, confidence=0.95, horizon=1):
    """VaR paramétrico diario a confidence%."""
    returns = [math.log(closes[i] / closes[i-1])
               for i in range(1, len(closes))
               if closes[i-1] > 0]
    if len(returns) < 30:
        return None
    mu  = statistics.mean(returns)
    sig = statistics.stdev(returns)
    # z-score para 95%: 1.645
    z = {0.90: 1.282, 0.95: 1.645, 0.99: 2.326}.get(confidence, 1.645)
    daily_var = (mu + z * sig) * math.sqrt(horizon)
    return round(abs(daily_var) * 100, 3)

def kelly_fraction(win_prob, win_return, loss_return):
    """Kelly Criterion para sizing de posición."""
    if loss_return == 0:
        return 0
    b = abs(win_return / loss_return)
    k = (b * win_prob - (1 - win_prob)) / b
    # Kelly fraccionario: usar 25% del Kelly completo (más conservador)
    return round(max(0, min(k * 0.25, 0.20)), 4)

def technical_score(closes, rsi_val, macd_hist, bb_low, bb_mid, bb_high):
    """
    Score técnico 0-100 calculado matemáticamente.
    Positivo = alcista, negativo = bajista, neutral = esperar.
    """
    if not closes or len(closes) < 50:
        return 50

    price = closes[-1]
    score = 50  # neutro por defecto

    # SMA crosses (weight: 30%)
    s20  = sma(closes, 20)
    s50  = sma(closes, 50)
    s200 = sma(closes, 200) if len(closes) >= 200 else None
    if s20 and s50:
        if s20 > s50:
            score += 8
        else:
            score -= 8
    if s200:
        if price > s200:
            score += 7
        else:
            score -= 7

    # RSI (weight: 25%)
    if rsi_val:
        if rsi_val < 30:
            score += 15   # sobreventa = oportunidad
        elif rsi_val < 45:
            score += 8
        elif rsi_val > 70:
            score -= 15   # sobrecompra = riesgo
        elif rsi_val > 55:
            score -= 5

    # MACD histogram (weight: 20%)
    if macd_hist is not None:
        if macd_hist > 0:
            score += 10
        else:
            score -= 10

    # Bollinger position (weight: 15%)
    if bb_low and bb_high:
        bb_range = bb_high - bb_low
        if bb_range > 0:
            bb_pos = (price - bb_low) / bb_range  # 0=bajo, 1=alto
            if bb_pos < 0.2:
                score += 8   # cerca de banda inferior = compra
            elif bb_pos > 0.8:
                score -= 8

    # Momentum 3M (weight: 10%)
    if len(closes) >= 63:
        mom = (closes[-1] / closes[-63] - 1) * 100
        if mom > 5:
            score += 5
        elif mom < -5:
            score -= 5

    return max(0, min(100, round(score)))

def fundamental_score_commodity(gdp_vals, fx_rates, eia_data, asset_type):
    """Score fundamental 0-100 para commodities según macro."""
    score = 50

    # PIB de mercados emergentes (proxy demanda)
    if gdp_vals:
        avg_gdp = statistics.mean(gdp_vals)
        if avg_gdp > 5:
            score += 12
        elif avg_gdp > 3:
            score += 6
        elif avg_gdp < 1:
            score -= 8

    # Ciclo dólar (DXY proxy: EUR/USD)
    eur = fx_rates.get("EUR")
    if eur:
        # EUR fuerte = USD débil = positivo para commodities en USD
        if eur > 1.08:
            score += 8
        elif eur < 1.00:
            score -= 8

    # Inventarios petróleo (para energía)
    if asset_type == "energy" and eia_data and eia_data.get("vs_avg") is not None:
        inv_vs_avg = eia_data["vs_avg"]
        if inv_vs_avg < -5:   # inventarios bajos = precio sube
            score += 12
        elif inv_vs_avg > 5:  # inventarios altos = precio baja
            score -= 10

    # Minerales críticos: demanda estructural positiva (transición energética)
    if asset_type == "critical":
        score += 10

    return max(0, min(100, round(score)))

def composite_score(tech_score, fund_score, horizon="medium"):
    """Score compuesto ponderado por horizonte temporal."""
    weights = {
        "short":  (0.70, 0.30),   # corto: más técnico
        "medium": (0.40, 0.60),   # medio: más fundamental
        "long":   (0.20, 0.80),   # largo: casi todo fundamental
    }
    wt, wf = weights.get(horizon, (0.40, 0.60))
    return round(wt * tech_score + wf * fund_score)

def entry_exit_levels(closes, atr_val):
    """Calcula niveles de entrada, stop-loss y target con R:R 2:1."""
    if not closes or not atr_val:
        return None
    price = closes[-1]
    stop  = round(price - 1.5 * atr_val, 4)   # stop 1.5×ATR
    target = round(price + 3.0 * atr_val, 4)  # target 3×ATR → R:R = 2:1
    return {
        "entry":  round(price, 4),
        "stop":   stop,
        "target": target,
        "rr":     2.0,
        "stop_pct":   round((price - stop) / price * 100, 2),
        "target_pct": round((target - price) / price * 100, 2),
    }

# ── CAPA 3: Claude con Kahneman Framework ────────────

def claude_call(prompt, system="", max_tokens=800):
    """Llamada directa a la API de Anthropic."""
    if not ANTHROPIC_KEY:
        return None
    payload = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": max_tokens,
        "system": system,
        "messages": [{"role": "user", "content": prompt}]
    }
    headers = {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01"
    }
    r = post_json("https://api.anthropic.com/v1/messages", payload, headers)
    if not r:
        return None
    return r.get("content", [{}])[0].get("text", "")

def kahneman_analysis(asset, quant_data, macro_context):
    """
    3 llamadas secuenciales implementando el framework Kahneman:
    1. Hipótesis con chain-of-thought
    2. Pre-mortem obligatorio
    3. Calibración con probabilidad explícita
    """
    name    = asset["name"]
    score   = quant_data["composite_score"]
    tech    = quant_data["technical_score"]
    fund    = quant_data["fundamental_score"]
    vol     = quant_data.get("volatility")
    rsi_val = quant_data.get("rsi")
    levels  = quant_data.get("levels")
    kelly   = quant_data.get("kelly")

    context = f"""
ACTIVO: {name} ({asset['type']})
FECHA: {DATE_ES} — {QUARTER}
SCORE COMPUESTO: {score}/100 (técnico {tech}, fundamental {fund})
RSI(14): {rsi_val}
VOLATILIDAD ANUALIZADA: {vol}%
NIVELES: entrada ${levels['entry'] if levels else 'N/A'}, stop ${levels['stop'] if levels else 'N/A'}, target ${levels['target'] if levels else 'N/A'}
KELLY FRACTION: {kelly} (sizing recomendado: {round((kelly or 0)*100,1)}% de cartera)
MACRO: {macro_context}
"""

    system = """Eres un analista cuantitativo senior con experiencia en commodities y mercados emergentes.
Razonas de forma estructurada, separas hechos de inferencias, y siempre cuantificas la incertidumbre.
Respondes en español. Eres conciso y preciso."""

    # ── Llamada 1: Hipótesis con chain-of-thought ──
    prompt1 = f"""Datos cuantitativos calculados matemáticamente:
{context}

Construye la tesis de inversión para horizonte 1-6 meses.
INSTRUCCIÓN CRÍTICA: Razona paso a paso antes de concluir.
1. Qué dicen los datos técnicos exactamente
2. Qué dicen los fundamentales macro
3. Cómo se combinan ambas señales
4. Cuál es la tesis resultante (2-3 frases máx)
5. Probabilidad inicial estimada de éxito: X%

Sé específico con los números. No generalices."""

    thesis = claude_call(prompt1, system, max_tokens=600)
    time.sleep(0.5)  # evitar rate limit

    # ── Llamada 2: Pre-mortem (Kahneman §24) ──
    prompt2 = f"""Tesis generada para {name}:
{thesis}

PRE-MORTEM OBLIGATORIO (técnica Kahneman):
Asume que esta tesis FALLÓ completamente en 6 meses. El precio se movió en contra.
¿Por qué falló? Identifica los 3 riesgos más probables que invalidarían esta tesis.
Para cada riesgo: (a) descripción, (b) probabilidad estimada, (c) señal de alerta temprana.
Sé implacable. El objetivo es encontrar los puntos ciegos."""

    premortem = claude_call(prompt2, system, max_tokens=500)
    time.sleep(0.5)

    # ── Llamada 3: Calibración final ──
    prompt3 = f"""Tesis: {thesis}
Pre-mortem: {premortem}
Score cuantitativo: {score}/100

CALIBRACIÓN FINAL:
Integra la tesis y el pre-mortem. Produce:
1. Señal: COMPRAR / VENDER / ESPERAR
2. Probabilidad calibrada: p=X% ± Y% (intervalo de confianza)
3. Horizonte óptimo de entrada: inmediato / esperar corrección / esperar confirmación
4. Condición de invalidación: "salir si [condición concreta]"
5. Una frase de síntesis para el dashboard

Formato JSON estricto:
{{"signal":"COMPRAR|VENDER|ESPERAR","prob":65,"prob_interval":12,"horizon":"str","invalidation":"str","summary":"str","conviction":"alta|media|baja"}}"""

    calibration_raw = claude_call(prompt3, system, max_tokens=300)
    time.sleep(0.5)

    # Parsear calibración
    calibration = {}
    if calibration_raw:
        try:
            start = calibration_raw.find("{")
            end   = calibration_raw.rfind("}") + 1
            if start != -1 and end > start:
                calibration = json.loads(calibration_raw[start:end])
        except:
            calibration = {"signal": "ESPERAR", "prob": 50, "summary": calibration_raw[:200]}

    return {
        "thesis":      thesis or "",
        "premortem":   premortem or "",
        "calibration": calibration,
    }

# ── CAPA 4: Análisis por región ───────────────────────

def analyze_region(region, fx_rates):
    """Análisis macro por región con datos World Bank."""
    gdp_vals = fetch_world_bank_gdp(region["id"])
    score = 50
    gdp_latest = None
    gdp_trend = "estable"

    if gdp_vals and len(gdp_vals) >= 2:
        gdp_latest = round(gdp_vals[0], 2)
        trend = gdp_vals[0] - gdp_vals[1]
        if trend > 0.5:
            gdp_trend = "acelerando"
            score += 15
        elif trend < -0.5:
            gdp_trend = "desacelerando"
            score -= 10
        if gdp_latest > 5:
            score += 12
        elif gdp_latest > 3:
            score += 6
        elif gdp_latest < 1:
            score -= 10

    # FX stability proxy
    currency_map = {
        "IND": "INR", "BRA": "BRL", "MEX": "MXN",
        "POL": "PLN", "SAU": "SAR", "NGA": "NGN"
    }
    cur = currency_map.get(region["id"])
    fx_val = fx_rates.get(cur) if cur else None

    return {
        "id":         region["id"],
        "name":       region["name"],
        "region":     region["region"],
        "gdp_latest": gdp_latest,
        "gdp_trend":  gdp_trend,
        "score":      max(0, min(100, score)),
        "currency":   cur,
        "fx_usd":     round(fx_val, 4) if fx_val else None,
    }

# ── Main pipeline ─────────────────────────────────────

def run():
    print(f"\n{'='*60}")
    print(f"GeoMacro Intel v5 — {DATE_ES}")
    print(f"{'='*60}\n")

    results = {
        "generated_at": DATE_ES,
        "quarter":      QUARTER,
        "assets":       [],
        "regions":      [],
        "macro":        {},
        "ranking":      [],
        "alerts":       [],
        "audit_log":    [],
    }

    # ── 1. Datos macro globales ──
    print("→ Fetching macro data...")
    fx_rates = fetch_fx()
    eia      = fetch_eia_oil_inventory()

    fred_data = {}
    if FRED_KEY:
        for series, label in [
            ("FEDFUNDS",   "fed_funds_rate"),
            ("T10Y2Y",     "yield_curve_spread"),
            ("DTWEXBGS",   "dxy_index"),
            ("CPIAUCSL",   "cpi_yoy"),
        ]:
            obs = fetch_fred(series)
            if obs:
                fred_data[label] = round(obs[0][1], 3)
                print(f"   FRED {label}: {obs[0][1]:.3f}")

    results["macro"] = {
        "fx_rates":  fx_rates,
        "fred":      fred_data,
        "eia_crude": eia,
        "timestamp": DATE_ES,
    }

    macro_context = (
        f"FED funds: {fred_data.get('fed_funds_rate','N/A')}%, "
        f"Yield curve (10Y-2Y): {fred_data.get('yield_curve_spread','N/A')}bps, "
        f"DXY: {fred_data.get('dxy_index','N/A')}, "
        f"EUR/USD: {fx_rates.get('EUR','N/A')}, "
        f"INR/USD: {fx_rates.get('INR','N/A')}, "
        f"Inventarios petróleo vs avg5Y: {eia.get('vs_avg','N/A') if eia else 'N/A'}%"
    )

    # ── 2. Análisis por región ──
    print("\n→ Analyzing regions...")
    for reg in REGIONS:
        print(f"   {reg['name']}...")
        r = analyze_region(reg, fx_rates)
        results["regions"].append(r)
        time.sleep(0.3)

    # ── 3. Análisis cuantitativo por activo ──
    print("\n→ Quantitative analysis per asset...")
    for asset in ASSETS:
        print(f"   {asset['name']} ({asset['id']})...")
        log_entry = {"asset": asset["name"], "ticker": asset["id"], "timestamp": DATE_ES}

        prices_raw = fetch_yahoo_prices(asset["id"])
        if not prices_raw or len(prices_raw) < 50:
            print(f"   SKIP {asset['id']}: insufficient price data")
            log_entry["status"] = "skipped_no_data"
            results["audit_log"].append(log_entry)
            continue

        closes  = [p[1] for p in prices_raw]
        # Para activos sin high/low usamos close como proxy
        highs   = closes
        lows    = closes

        # ── Indicadores técnicos ──
        rsi_val    = rsi(closes)
        macd_l, macd_h = macd(closes)
        bb_low, bb_mid, bb_high = bollinger(closes)
        atr_val    = atr(highs, lows, closes)
        vol        = annualized_volatility(closes)
        mdd        = max_drawdown(closes)
        var95      = var_parametric(closes)
        tech       = technical_score(closes, rsi_val, macd_h, bb_low, bb_mid, bb_high)

        # ── Score fundamental ──
        gdp_sample = fetch_world_bank_gdp("IND")  # India como proxy demanda emergente
        fund       = fundamental_score_commodity(
            gdp_sample, fx_rates, eia, asset["type"])

        comp = composite_score(tech, fund, "medium")

        # ── Niveles operativos ──
        levels = entry_exit_levels(closes, atr_val)

        # ── Position sizing (Kelly) ──
        win_prob   = comp / 100
        win_ret    = (levels["target_pct"] / 100) if levels else 0.06
        loss_ret   = (levels["stop_pct"] / 100)   if levels else 0.03
        kelly      = kelly_fraction(win_prob, win_ret, loss_ret)

        quant_data = {
            "price":             round(closes[-1], 4),
            "price_1m_ago":      round(closes[-21], 4) if len(closes) >= 21 else None,
            "price_3m_ago":      round(closes[-63], 4) if len(closes) >= 63 else None,
            "momentum_1m":       round((closes[-1]/closes[-21]-1)*100, 2) if len(closes)>=21 else None,
            "momentum_3m":       round((closes[-1]/closes[-63]-1)*100, 2) if len(closes)>=63 else None,
            "rsi":               rsi_val,
            "macd_line":         macd_l,
            "macd_histogram":    macd_h,
            "sma20":             round(sma(closes,20), 4) if sma(closes,20) else None,
            "sma50":             round(sma(closes,50), 4) if sma(closes,50) else None,
            "sma200":            round(sma(closes,200), 4) if len(closes)>=200 and sma(closes,200) else None,
            "bollinger_low":     bb_low,
            "bollinger_mid":     bb_mid,
            "bollinger_high":    bb_high,
            "atr":               atr_val,
            "volatility":        vol,
            "max_drawdown_6m":   mdd,
            "var_95_daily":      var95,
            "technical_score":   tech,
            "fundamental_score": fund,
            "composite_score":   comp,
            "levels":            levels,
            "kelly":             kelly,
        }

        log_entry["scores"] = {"tech": tech, "fund": fund, "composite": comp}
        log_entry["status"]  = "quantitative_complete"

        # ── Kahneman framework (3 llamadas Claude) ──
        analysis = {"thesis": "", "premortem": "", "calibration": {}}
        if ANTHROPIC_KEY:
            print(f"   → Kahneman analysis for {asset['name']}...")
            analysis = kahneman_analysis(asset, quant_data, macro_context)
            log_entry["claude_calls"] = 3
            log_entry["signal"] = analysis["calibration"].get("signal", "ESPERAR")
        else:
            log_entry["claude_calls"] = 0
            log_entry["signal"] = "NO_KEY"

        results["assets"].append({
            "meta":        asset,
            "quant":       quant_data,
            "analysis":    analysis,
        })
        results["audit_log"].append(log_entry)
        time.sleep(1)  # respetar rate limits

    # ── 4. Ranking global ──
    print("\n→ Building ranking...")
    ranked = sorted(
        [a for a in results["assets"] if a.get("quant")],
        key=lambda x: x["quant"]["composite_score"],
        reverse=True
    )
    results["ranking"] = [
        {
            "rank":      i + 1,
            "name":      a["meta"]["name"],
            "type":      a["meta"]["type"],
            "score":     a["quant"]["composite_score"],
            "signal":    a["analysis"]["calibration"].get("signal", "—"),
            "prob":      a["analysis"]["calibration"].get("prob"),
            "summary":   a["analysis"]["calibration"].get("summary", ""),
            "price":     a["quant"]["price"],
            "vol":       a["quant"]["volatility"],
            "kelly":     a["quant"]["kelly"],
        }
        for i, a in enumerate(ranked)
    ]

    # ── 5. Alertas automáticas ──
    print("\n→ Generating alerts...")
    for a in results["assets"]:
        q = a["quant"]
        cal = a["analysis"]["calibration"]
        name = a["meta"]["name"]

        # Alerta RSI sobreventa
        if q.get("rsi") and q["rsi"] < 30:
            results["alerts"].append({
                "type":    "RSI_OVERSOLD",
                "asset":   name,
                "value":   q["rsi"],
                "message": f"{name}: RSI={q['rsi']} — zona de sobreventa, posible rebote",
                "severity": "high",
            })

        # Alerta score alto + señal compra
        if q["composite_score"] >= 70 and cal.get("signal") == "COMPRAR":
            results["alerts"].append({
                "type":    "BUY_SIGNAL",
                "asset":   name,
                "value":   q["composite_score"],
                "message": f"{name}: score {q['composite_score']}/100 + señal COMPRAR (p={cal.get('prob')}%)",
                "severity": "high",
            })

        # Alerta drawdown elevado
        if q.get("max_drawdown_6m") and q["max_drawdown_6m"] > 20:
            results["alerts"].append({
                "type":    "HIGH_DRAWDOWN",
                "asset":   name,
                "value":   q["max_drawdown_6m"],
                "message": f"{name}: drawdown máximo 6M = {q['max_drawdown_6m']}% — riesgo elevado",
                "severity": "medium",
            })

    print(f"\n→ {len(results['alerts'])} alertas generadas")
    print(f"→ {len(results['assets'])} activos analizados")
    print(f"→ {len(results['regions'])} regiones analizadas")

    # ── 6. Guardar results.json ──
    out_path = os.path.join(os.path.dirname(__file__), "results.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n✓ Guardado: {out_path}")
    print(f"✓ Pipeline completado — {DATE_ES}\n")

if __name__ == "__main__":
    run()

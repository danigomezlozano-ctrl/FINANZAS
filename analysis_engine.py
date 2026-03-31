#!/usr/bin/env python3
"""
GeoMacro Intel v7 — Sistema Completo Integrado
================================================
MÓDULO 1: Trading — Commodities + Recursos (diario)
  - Series de precios Yahoo Finance (RSI, MACD, Bollinger, ATR, VaR, Kelly)
  - Score técnico 40% + fundamental 60%
  - Kahneman: hipótesis → pre-mortem → calibración con probabilidad

MÓDULO 2: Geoestrategico (diario)
  - 6 regiones emergentes con PIB World Bank + FX Frankfurter + noticias
  - Kahneman completo para cada región
  - Riesgos geopolíticos globales con datos reales

MÓDULO 3: Cartera Core (lunes de cada trimestre)
  - MSCI World + NVIDIA + Bitcoin
  - Pesos actuales vs target, drift, alertas concentración
  - Revisión de conviction Kahneman trimestral (¿sigue válida la tesis?)
  - NO señales técnicas de corto plazo — filosofía largo plazo intacta

MÓDULO 4: Auditoría unificada
  - Log de cada llamada API con timestamp, datos usados, resultado
  - Separación visual clara entre estrategias
  - Historial acumulado en results.json (git como base de datos)
"""

import os, json, math, time, datetime, statistics, urllib.request, urllib.parse
from urllib.request import urlopen, Request

# ── Credenciales ──────────────────────────────────────
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
FRED_KEY      = os.environ.get("FRED_API_KEY", "")
NEWS_KEY      = os.environ.get("NEWS_API_KEY", "")

# Cartera core desde secrets
PORTFOLIO_EUR = float(os.environ.get("PORTFOLIO_VALUE_EUR", "6601.14"))
UNITS_BTC     = float(os.environ.get("UNITS_BTC",   "0.019975"))
UNITS_NVDA    = float(os.environ.get("UNITS_NVDA",  "13.167854"))
UNITS_MSCI    = float(os.environ.get("UNITS_MSCI",  "33.142387"))

# Targets cartera core (sistema v8)
TARGET_MSCI = 0.70
TARGET_NVDA = 0.10
TARGET_BTC  = 0.05

NOW     = datetime.datetime.utcnow()
TODAY   = NOW.strftime("%Y-%m-%d")
QUARTER = f"Q{math.ceil(NOW.month/3)} {NOW.year}"
DATE_ES = NOW.strftime("%d/%m/%Y %H:%M UTC")

# ¿Es lunes de trimestre? (revisión core)
IS_QUARTERLY = (NOW.weekday() == 0 and NOW.day <= 7 and NOW.month in [1,4,7,10])
# Para forzar revisión core siempre en CI, usar variable de entorno
if os.environ.get("FORCE_CORE_REVIEW"):
    IS_QUARTERLY = True

# ── Activos trading ───────────────────────────────────
TRADING_ASSETS = [
    {"id":"GC=F",  "name":"Oro",         "type":"precious",    "unit":"USD/oz"},
    {"id":"SI=F",  "name":"Plata",        "type":"precious",    "unit":"USD/oz"},
    {"id":"CL=F",  "name":"Crudo WTI",    "type":"energy",      "unit":"USD/bbl"},
    {"id":"NG=F",  "name":"Gas Natural",  "type":"energy",      "unit":"USD/MMBtu"},
    {"id":"HG=F",  "name":"Cobre",        "type":"industrial",  "unit":"USD/lb"},
    {"id":"ALI=F", "name":"Aluminio",     "type":"industrial",  "unit":"USD/t"},
    {"id":"LIT",   "name":"ETF Litio",    "type":"critical",    "unit":"USD/share"},
    {"id":"COPX",  "name":"ETF Cobre",    "type":"industrial",  "unit":"USD/share"},
]

# ── Regiones geoestrategicas ──────────────────────────
REGIONS = [
    {"id":"VNM","name":"Vietnam",    "region":"SE Asia",    "currency":"VND","sector":"manufactura tech"},
    {"id":"IND","name":"India",      "region":"South Asia", "currency":"INR","sector":"servicios digitales"},
    {"id":"POL","name":"Polonia",    "region":"CEE Europa", "currency":"PLN","sector":"logística / UE hub"},
    {"id":"BRA","name":"Brasil",     "region":"LATAM",      "currency":"BRL","sector":"agro / recursos"},
    {"id":"SAU","name":"Arabia S.",  "region":"MENA",       "currency":"SAR","sector":"energía / diversif."},
    {"id":"NGA","name":"Nigeria",    "region":"África Sub.","currency":"NGN","sector":"fintech / recursos"},
]

# ── Cartera core ──────────────────────────────────────
CORE_ASSETS = [
    {"id":"NVDA",    "name":"NVIDIA",     "ticker":"NVDA",    "currency":"USD"},
    {"id":"BTC-EUR", "name":"Bitcoin",    "ticker":"BTC-EUR", "currency":"EUR"},
    {"id":"IWDA.AS", "name":"MSCI World", "ticker":"IWDA.AS", "currency":"EUR"},
]

# ══════════════════════════════════════════════════════
# HTTP HELPERS
# ══════════════════════════════════════════════════════

def fetch(url, headers=None, timeout=14):
    try:
        req = Request(url, headers=headers or {"User-Agent": "GeoMacroIntel/7.0"})
        with urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"  WARN fetch({url[:55]}): {e}")
        return None

def post_json(url, payload, headers):
    data = json.dumps(payload).encode()
    req  = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"  ERROR post: {e}")
        return None

# ══════════════════════════════════════════════════════
# CAPA 1: DATA FETCHERS
# ══════════════════════════════════════════════════════

def fetch_yahoo(ticker, days=730):
    end   = int(time.time())
    start = end - days * 86400
    url   = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
             f"?interval=1d&period1={start}&period2={end}")
    d = fetch(url)
    if not d: return None
    try:
        res    = d["chart"]["result"][0]
        closes = res["indicators"]["quote"][0]["close"]
        times  = res["timestamp"]
        prices = [(t, c) for t, c in zip(times, closes) if c is not None]
        return prices
    except Exception as e:
        print(f"  WARN Yahoo {ticker}: {e}")
        return None

def fetch_fred(sid):
    if not FRED_KEY: return None
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={sid}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit=60")
    d = fetch(url)
    if not d: return None
    try:
        return [(o["date"], float(o["value"]))
                for o in d["observations"] if o["value"] not in (".", "")]
    except: return None

def fetch_wb_gdp(iso):
    url = (f"https://api.worldbank.org/v2/country/{iso}/indicator/"
           f"NY.GDP.MKTP.KD.ZG?format=json&mrv=4&per_page=4")
    d = fetch(url)
    if not d or not d[1]: return None
    return [o["value"] for o in d[1] if o["value"] is not None]

def fetch_fx():
    d = fetch("https://api.frankfurter.app/latest?from=USD"
              "&to=EUR,INR,BRL,MXN,PLN,AED,THB,SAR,NGN,VND,GBP")
    return d.get("rates", {}) if d else {}

def fetch_eia():
    if not FRED_KEY: return None
    obs = fetch_fred("WCRSTUS1")
    if not obs or len(obs) < 2: return None
    latest, prev = obs[0][1], obs[1][1]
    avg5y = statistics.mean([o[1] for o in obs[:260]]) if len(obs) >= 260 else None
    return {"latest": latest, "prev": prev, "avg5y": avg5y,
            "vs_avg": round((latest/avg5y - 1)*100, 1) if avg5y else None}

def fetch_news(query, n=5):
    if not NEWS_KEY: return []
    url = (f"https://newsapi.org/v2/everything"
           f"?q={urllib.parse.quote(query)}"
           f"&language=en&sortBy=publishedAt&pageSize={n}&apiKey={NEWS_KEY}")
    d = fetch(url)
    if not d or d.get("status") != "ok": return []
    return [{"title":   a.get("title", "")[:100],
             "source":  a.get("source", {}).get("name", ""),
             "desc":    a.get("description", "")[:120]}
            for a in d.get("articles", [])[:n]]

# ══════════════════════════════════════════════════════
# CAPA 2: INDICADORES TÉCNICOS (matemática pura, sin IA)
# ══════════════════════════════════════════════════════

def sma(p, n):
    return sum(p[-n:]) / n if len(p) >= n else None

def ema(p, n):
    if len(p) < n: return None
    k = 2 / (n + 1)
    e = sum(p[:n]) / n
    for x in p[n:]: e = x * k + e * (1 - k)
    return e

def rsi(p, n=14):
    if len(p) < n + 1: return None
    g, l = [], []
    for i in range(1, len(p)):
        d = p[i] - p[i-1]
        g.append(max(d, 0)); l.append(max(-d, 0))
    ag = sum(g[-n:]) / n; al = sum(l[-n:]) / n
    if al == 0: return 100.0
    return round(100 - 100 / (1 + ag/al), 2)

def macd(p, fast=12, slow=26, signal=9):
    if len(p) < slow + signal: return None, None
    mv = []
    for i in range(slow, len(p) + 1):
        ef = ema(p[:i], fast); es = ema(p[:i], slow)
        if ef and es: mv.append(ef - es)
    ef = ema(p, fast); es = ema(p, slow)
    if not ef or not es: return None, None
    ml  = ef - es
    sig = ema(mv, signal) if len(mv) >= signal else None
    return round(ml, 4), round(ml - sig, 4) if sig else None

def bollinger(p, n=20, m=2):
    if len(p) < n: return None, None, None
    w = p[-n:]; mid = sum(w)/n; std = statistics.stdev(w)
    return round(mid - m*std, 4), round(mid, 4), round(mid + m*std, 4)

def atr_calc(closes, n=14):
    trs = [max(closes[i] - closes[i-1], abs(closes[i] - closes[i-1]))
           for i in range(1, len(closes))]
    return round(sum(trs[-n:]) / n, 4) if len(trs) >= n else None

def ann_vol(closes, w=126):
    c = closes[-w:] if len(closes) >= w else closes
    rets = [math.log(c[i]/c[i-1]) for i in range(1, len(c)) if c[i-1] > 0]
    return round(statistics.stdev(rets) * math.sqrt(252) * 100, 2) if len(rets) > 5 else None

def max_dd(closes, w=126):
    c = closes[-w:] if len(closes) >= w else closes
    peak = c[0]; mdd = 0
    for x in c:
        if x > peak: peak = x
        dd = (peak - x) / peak
        if dd > mdd: mdd = dd
    return round(mdd * 100, 2)

def var95(closes):
    rets = [math.log(closes[i]/closes[i-1])
            for i in range(1, len(closes)) if closes[i-1] > 0]
    if len(rets) < 30: return None
    mu  = statistics.mean(rets)
    sig = statistics.stdev(rets)
    return round(abs(mu + 1.645 * sig) * 100, 3)

def kelly(win_p, win_r, loss_r):
    if loss_r == 0: return 0
    b = abs(win_r / loss_r)
    k = (b * win_p - (1 - win_p)) / b
    return round(max(0, min(k * 0.25, 0.20)), 4)  # Kelly fraccionario 25%

def tech_score(closes, rsi_v, macd_h, bb_low, bb_high):
    if not closes or len(closes) < 50: return 50
    p = closes[-1]; score = 50
    s20 = sma(closes, 20); s50 = sma(closes, 50)
    s200 = sma(closes, 200) if len(closes) >= 200 else None
    if s20 and s50:  score += 8 if s20 > s50  else -8
    if s200:         score += 7 if p  > s200  else -7
    if rsi_v:
        if rsi_v < 30:   score += 15
        elif rsi_v < 45: score += 8
        elif rsi_v > 70: score -= 15
        elif rsi_v > 55: score -= 5
    if macd_h: score += 10 if macd_h > 0 else -10
    if bb_low and bb_high:
        rng = bb_high - bb_low
        if rng > 0:
            pos = (p - bb_low) / rng
            if pos < 0.2:  score += 8
            elif pos > 0.8: score -= 8
    if len(closes) >= 63:
        mom = (closes[-1]/closes[-63] - 1) * 100
        if mom > 5:  score += 5
        elif mom < -5: score -= 5
    return max(0, min(100, round(score)))

def fund_score_commodity(gdp_vals, fx_rates, eia, asset_type):
    score = 50
    if gdp_vals:
        avg = statistics.mean(gdp_vals)
        if avg > 5:   score += 12
        elif avg > 3: score += 6
        elif avg < 1: score -= 8
    eur = fx_rates.get("EUR")
    if eur:
        if eur > 1.08: score += 8
        elif eur < 1.00: score -= 8
    if asset_type == "energy" and eia and eia.get("vs_avg") is not None:
        if eia["vs_avg"] < -5:  score += 12
        elif eia["vs_avg"] > 5: score -= 10
    if asset_type == "critical": score += 10
    return max(0, min(100, round(score)))

def comp_score(tech, fund):
    # 40% técnico + 60% fundamental (horizonte medio plazo)
    return round(0.40 * tech + 0.60 * fund)

def entry_levels(closes, atr_v):
    if not closes or not atr_v: return None
    p = closes[-1]
    stop   = round(p - 1.5 * atr_v, 4)
    target = round(p + 3.0 * atr_v, 4)
    return {"entry": round(p, 4), "stop": stop, "target": target, "rr": 2.0,
            "stop_pct":   round((p - stop)   / p * 100, 2),
            "target_pct": round((target - p) / p * 100, 2)}

def geo_score(gdp_vals, news_count):
    score = 50
    if gdp_vals:
        avg = statistics.mean(gdp_vals)
        if avg > 5:   score += 15
        elif avg > 3: score += 8
        elif avg < 1: score -= 10
        if len(gdp_vals) >= 2 and gdp_vals[0] > gdp_vals[1]: score += 5
    if news_count > 3: score += 3
    return max(0, min(100, round(score)))

# ══════════════════════════════════════════════════════
# CAPA 3: CLAUDE / KAHNEMAN (3 llamadas por análisis)
# ══════════════════════════════════════════════════════

SYSTEM_PROMPT = """Eres un analista cuantitativo y geoestrategico senior de nivel institucional.
Separas hechos de inferencias. Cuantificas la incertidumbre. Haces pre-mortem siempre.
Evitas el overconfidence y el narrative fallacy. Respondes en español. Eres conciso y preciso."""

def claude(prompt, max_tokens=700):
    if not ANTHROPIC_KEY: return None
    r = post_json(
        "https://api.anthropic.com/v1/messages",
        {"model": "claude-sonnet-4-20250514", "max_tokens": max_tokens,
         "system": SYSTEM_PROMPT,
         "messages": [{"role": "user", "content": prompt}]},
        {"Content-Type": "application/json",
         "x-api-key": ANTHROPIC_KEY,
         "anthropic-version": "2023-06-01"})
    if not r: return None
    return r.get("content", [{}])[0].get("text", "")

def parse_cal(raw):
    if not raw: return {}
    try:
        s = raw.find("{"); e = raw.rfind("}") + 1
        if s != -1 and e > s: return json.loads(raw[s:e])
    except: pass
    return {"signal": "ESPERAR", "prob": 50, "summary": (raw or "")[:200]}

def kahneman_trading(asset, qdata, macro_ctx):
    """3 llamadas Kahneman para activo de trading."""
    name  = asset["name"]
    score = qdata["composite_score"]
    tech  = qdata["technical_score"]
    fund  = qdata["fundamental_score"]
    vol   = qdata.get("volatility")
    rsi_v = qdata.get("rsi")
    lv    = qdata.get("levels")
    kf    = qdata.get("kelly")

    ctx = (f"ACTIVO: {name} ({asset['type']}) | {DATE_ES} | {QUARTER}\n"
           f"Score compuesto: {score}/100 (técnico {tech}, fundamental {fund})\n"
           f"RSI14: {rsi_v} | Volatilidad anual: {vol}% | Kelly sizing: {round((kf or 0)*100,1)}%\n"
           f"Entrada: ${lv['entry'] if lv else 'N/A'} | "
           f"Stop: ${lv['stop'] if lv else 'N/A'} | "
           f"Target: ${lv['target'] if lv else 'N/A'} (R:R 2:1)\n"
           f"MACRO GLOBAL: {macro_ctx}")

    # Hipótesis con chain-of-thought
    t1 = claude(
        f"{ctx}\n\nTESIS DE TRADING — horizonte 1-6 meses:\n"
        f"1. Señales técnicas exactas (RSI, MACD, posición Bollinger)\n"
        f"2. Fundamentales macro que apoyan o contradicen\n"
        f"3. Tesis resultante en 2-3 frases con precio objetivo\n"
        f"4. Probabilidad inicial: X%\nCita los números reales.", 550)
    time.sleep(0.5)

    # Pre-mortem Kahneman
    t2 = claude(
        f"Tesis para {name}: {t1}\n\n"
        f"PRE-MORTEM (Kahneman §24): Esta tesis FALLÓ en 6 meses.\n"
        f"3 riesgos que la invalidaron — para cada uno:\n"
        f"(a) descripción específica, (b) probabilidad, (c) señal de alerta temprana.", 420)
    time.sleep(0.5)

    # Calibración con probabilidad explícita
    t3 = claude(
        f"Tesis: {t1}\nPre-mortem: {t2}\nScore cuantitativo: {score}/100\n\n"
        f"CALIBRACIÓN FINAL — JSON estricto sin texto extra:\n"
        f'{{"signal":"COMPRAR|VENDER|ESPERAR","prob":65,"prob_interval":12,'
        f'"horizon":"string","invalidation":"string",'
        f'"summary":"string 1 frase","conviction":"alta|media|baja"}}', 280)
    time.sleep(0.5)

    return {"thesis": t1 or "", "premortem": t2 or "",
            "calibration": parse_cal(t3)}

def kahneman_geo(region, gdp_vals, fx_rates, news, macro_ctx):
    """3 llamadas Kahneman para análisis geoestrategico de región."""
    name = region["name"]; sec = region["sector"]
    gdp  = gdp_vals[0] if gdp_vals else None
    fx   = fx_rates.get(region["currency"])
    ns   = " | ".join([n["title"] for n in news[:4]]) if news else "sin noticias"

    ctx = (f"REGIÓN: {name} ({region['region']}) | {DATE_ES} | {QUARTER}\n"
           f"Sector principal: {sec}\n"
           f"PIB crecimiento (World Bank): {gdp}%\n"
           f"FX: 1 USD = {fx} {region['currency']}\n"
           f"Noticias recientes: {ns}\n"
           f"MACRO GLOBAL: {macro_ctx}")

    # Hipótesis oportunidad geoestrategica
    t1 = claude(
        f"{ctx}\n\nANÁLISIS GEOESTRATEGICO — horizonte 6-18 meses:\n"
        f"1. Situación macroeconómica actual de {name}\n"
        f"2. Contexto geopolítico y factores de riesgo específicos\n"
        f"3. Oportunidad concreta en {sec} con tamaño estimado\n"
        f"4. Probabilidad de materialización: X%\nDatos reales, sin generalizar.", 550)
    time.sleep(0.5)

    # Pre-mortem geoestrategico
    t2 = claude(
        f"Tesis geoestrategica para {name}: {t1}\n\n"
        f"PRE-MORTEM: Esta oportunidad NO se materializó.\n"
        f"3 factores que la bloquearon (política, FX, competencia, regulación...):\n"
        f"Para cada uno: probabilidad + señal de alerta temprana.", 400)
    time.sleep(0.5)

    # Calibración
    t3 = claude(
        f"Tesis: {t1}\nPre-mortem: {t2}\n\n"
        f"CALIBRACIÓN — JSON estricto:\n"
        f'{{"signal":"OPORTUNIDAD_ALTA|OPORTUNIDAD_MEDIA|ESPERAR|EVITAR",'
        f'"prob":60,"prob_interval":15,"horizon":"string",'
        f'"invalidation":"string","summary":"string 1 frase",'
        f'"conviction":"alta|media|baja","top_risk":"string breve"}}', 300)
    time.sleep(0.5)

    return {"thesis": t1 or "", "premortem": t2 or "",
            "calibration": parse_cal(t3)}

def kahneman_core_review(positions, macro_ctx, news_nvda, news_btc):
    """
    Revisión trimestral de conviction para cartera core.
    UNA sola llamada — no señales técnicas, solo conviction filosófica.
    """
    pos_str = "\n".join([
        f"- {p['name']}: {p['weight_pct']:.1f}% cartera "
        f"(target {p['target_pct']*100:.0f}%, drift {p['drift_pct']:+.1f}%)"
        for p in positions])

    news_str = ""
    if news_nvda: news_str += f"NVIDIA noticias: {' | '.join([n['title'] for n in news_nvda[:3]])}\n"
    if news_btc:  news_str += f"Bitcoin noticias: {' | '.join([n['title'] for n in news_btc[:2]])}\n"

    prompt = (f"REVISIÓN TRIMESTRAL CARTERA CORE — {DATE_ES} — {QUARTER}\n\n"
              f"POSICIONES:\n{pos_str}\n\n"
              f"MACRO ACTUAL: {macro_ctx}\n\n"
              f"{news_str}\n"
              f"TAREA: Revisar si las 3 tesis de largo plazo siguen válidas:\n"
              f"1. MSCI World (70%): diversificación global, crecimiento compuesto largo plazo\n"
              f"2. NVIDIA (10%): liderazgo en infraestructura IA, moat tecnológico\n"
              f"3. Bitcoin (5%): reserva de valor, descorrelación, asimetría positiva\n\n"
              f"Para cada activo: ¿sigue válida la tesis? ¿Ha cambiado algo fundamental?\n"
              f"IMPORTANTE: NO hacer señales de trading. NO analizar RSI ni precio corto plazo.\n"
              f"Solo evaluar si la TESIS FILOSÓFICA de largo plazo sigue intacta.\n\n"
              f"Finaliza con JSON:\n"
              f'{{"msci_conviction":"intacta|debilitada|invalidada",'
              f'"nvda_conviction":"intacta|debilitada|invalidada",'
              f'"btc_conviction":"intacta|debilitada|invalidada",'
              f'"rebalance_needed":true,"rebalance_action":"string o null",'
              f'"summary":"string 2-3 frases síntesis quarterly"}}')

    raw = claude(prompt, 800)
    cal = parse_cal(raw)
    return {"review": raw or "", "calibration": cal}

def kahneman_global_risks(assets_data, regions_data, macro_ctx):
    """Análisis de riesgos geopolíticos globales con todos los datos."""
    asset_sum = "; ".join([
        f"{a['meta']['name']}: {a['quant']['composite_score']}/100 "
        f"({a['analysis']['calibration'].get('signal','?')})"
        for a in assets_data if a.get('quant')])

    region_sum = "; ".join([
        f"{r['name']}: PIB {r.get('gdp_latest','?')}% "
        f"({r.get('analysis',{}).get('calibration',{}).get('signal','?')})"
        for r in regions_data])

    prompt = (f"FECHA: {DATE_ES} — {QUARTER}\n"
              f"MACRO: {macro_ctx}\n"
              f"COMMODITIES: {asset_sum}\n"
              f"REGIONES: {region_sum}\n\n"
              f"Identifica los 5 principales riesgos geopolíticos y macro actuales "
              f"que afectan a estos mercados. Para cada uno:\n"
              f"nombre, nivel 0-100, descripción con impacto concreto, activos afectados.\n"
              f"JSON: [{{'name':'str','level':65,'desc':'str','assets_affected':['str']}}]")

    raw = claude(prompt, 600)
    if not raw: return []
    try:
        s = raw.find("["); e = raw.rfind("]") + 1
        if s != -1 and e > s: return json.loads(raw[s:e])
    except: pass
    return []

# ══════════════════════════════════════════════════════
# MÓDULO 1: TRADING — COMMODITIES + RECURSOS
# ══════════════════════════════════════════════════════

def run_trading_module(fx_rates, eia, macro_ctx):
    print("\n─── MÓDULO 1: TRADING ───────────────────────────")
    results = []
    gdp_ind = fetch_wb_gdp("IND")  # proxy demanda emergente

    for asset in TRADING_ASSETS:
        print(f"   {asset['name']} ({asset['id']})...")
        log = {"module": "trading", "asset": asset["name"],
               "ticker": asset["id"], "timestamp": DATE_ES}

        prices_raw = fetch_yahoo(asset["id"])
        if not prices_raw or len(prices_raw) < 50:
            print(f"     SKIP: datos insuficientes")
            log["status"] = "skipped_no_data"
            results.append({"meta": asset, "quant": None, "analysis": {}, "log": log})
            continue

        closes = [p[1] for p in prices_raw]
        rsi_v           = rsi(closes)
        _, macd_h       = macd(closes)
        bb_low, _, bb_h = bollinger(closes)
        atr_v           = atr_calc(closes)
        vol             = ann_vol(closes)
        mdd             = max_dd(closes)
        var             = var95(closes)
        tech            = tech_score(closes, rsi_v, macd_h, bb_low, bb_h)
        fund            = fund_score_commodity(gdp_ind, fx_rates, eia, asset["type"])
        comp            = comp_score(tech, fund)
        lv              = entry_levels(closes, atr_v)
        kf              = kelly(comp/100,
                                (lv["target_pct"]/100) if lv else 0.06,
                                (lv["stop_pct"]/100)   if lv else 0.03)

        qdata = {
            "price":             round(closes[-1], 4),
            "momentum_1m":       round((closes[-1]/closes[-21]-1)*100, 2) if len(closes)>=21 else None,
            "momentum_3m":       round((closes[-1]/closes[-63]-1)*100, 2) if len(closes)>=63 else None,
            "rsi":               rsi_v,
            "macd_histogram":    macd_h,
            "sma20":             round(sma(closes,20), 4) if sma(closes,20) else None,
            "sma50":             round(sma(closes,50), 4) if sma(closes,50) else None,
            "sma200":            round(sma(closes,200),4) if len(closes)>=200 and sma(closes,200) else None,
            "bollinger_low":     bb_low,
            "bollinger_high":    bb_h,
            "atr":               atr_v,
            "volatility":        vol,
            "max_drawdown_6m":   mdd,
            "var_95_daily":      var,
            "technical_score":   tech,
            "fundamental_score": fund,
            "composite_score":   comp,
            "levels":            lv,
            "kelly":             kf,
        }

        analysis = {"thesis": "", "premortem": "", "calibration": {}}
        if ANTHROPIC_KEY:
            print(f"     → Kahneman 3 llamadas...")
            analysis = kahneman_trading(asset, qdata, macro_ctx)
            log["claude_calls"] = 3

        log["scores"] = {"tech": tech, "fund": fund, "composite": comp}
        log["signal"] = analysis["calibration"].get("signal", "ESPERAR")
        log["status"] = "complete"
        results.append({"meta": asset, "quant": qdata, "analysis": analysis, "log": log})
        time.sleep(1)

    print(f"   ✓ {len(results)} activos de trading analizados")
    return results

# ══════════════════════════════════════════════════════
# MÓDULO 2: GEOESTRATEGICO
# ══════════════════════════════════════════════════════

def run_geo_module(fx_rates, macro_ctx):
    print("\n─── MÓDULO 2: GEOESTRATEGICO ────────────────────")
    results = []

    for reg in REGIONS:
        print(f"   {reg['name']} ({reg['region']})...")
        gdp_vals = fetch_wb_gdp(reg["id"])
        fx_val   = fx_rates.get(reg["currency"])
        news     = fetch_news(f"{reg['name']} economy trade investment {NOW.year}", 5)
        gscore   = geo_score(gdp_vals, len(news))
        gdp_lat  = round(gdp_vals[0], 2) if gdp_vals else None
        gdp_trend = ("acelerando" if gdp_vals and len(gdp_vals)>=2 and gdp_vals[0]>gdp_vals[1]
                     else "desacelerando" if gdp_vals and len(gdp_vals)>=2 and gdp_vals[0]<gdp_vals[1]
                     else "estable" if gdp_vals else "sin datos")

        analysis = {"thesis": "", "premortem": "", "calibration": {}}
        if ANTHROPIC_KEY:
            print(f"     → Kahneman geo 3 llamadas...")
            analysis = kahneman_geo(reg, gdp_vals, fx_rates, news, macro_ctx)

        results.append({
            "id":          reg["id"],
            "name":        reg["name"],
            "region":      reg["region"],
            "sector":      reg["sector"],
            "currency":    reg["currency"],
            "fx_usd":      round(fx_val, 4) if fx_val else None,
            "gdp_latest":  gdp_lat,
            "gdp_trend":   gdp_trend,
            "score":       gscore,
            "news":        news[:3],
            "analysis":    analysis,
        })
        time.sleep(1)

    print(f"   ✓ {len(results)} regiones analizadas")
    return results

# ══════════════════════════════════════════════════════
# MÓDULO 3: CARTERA CORE (trimestral / lunes Q)
# ══════════════════════════════════════════════════════

def run_core_module(fx_rates, macro_ctx):
    print("\n─── MÓDULO 3: CARTERA CORE ──────────────────────")

    # Fetchear precios actuales
    prices = {}
    for asset in CORE_ASSETS:
        raw = fetch_yahoo(asset["ticker"], days=90)
        if raw and len(raw) > 0:
            prices[asset["id"]] = raw[-1][1]
            print(f"   {asset['name']}: {prices[asset['id']]:.2f} {asset['currency']}")

    # Calcular valores en EUR
    eur_usd = fx_rates.get("EUR", 0.87)  # 1 USD = X EUR
    nvda_price_eur = prices.get("NVDA", 0) * eur_usd
    btc_price_eur  = prices.get("BTC-EUR", 0)
    msci_price_eur = prices.get("IWDA.AS", 0)  # ya en EUR

    val_nvda = UNITS_NVDA * nvda_price_eur
    val_btc  = UNITS_BTC  * btc_price_eur
    val_msci = UNITS_MSCI * msci_price_eur if msci_price_eur > 0 else PORTFOLIO_EUR - val_nvda - val_btc

    total = val_msci + val_nvda + val_btc

    positions = [
        {"id": "MSCI",  "name": "MSCI World", "units": UNITS_MSCI,
         "price_eur": msci_price_eur, "value_eur": val_msci,
         "weight_pct": (val_msci/total*100) if total>0 else 0,
         "target_pct": TARGET_MSCI,
         "drift_pct":  (val_msci/total - TARGET_MSCI)*100 if total>0 else 0},
        {"id": "NVDA",  "name": "NVIDIA",     "units": UNITS_NVDA,
         "price_eur": nvda_price_eur, "value_eur": val_nvda,
         "weight_pct": (val_nvda/total*100) if total>0 else 0,
         "target_pct": TARGET_NVDA,
         "drift_pct":  (val_nvda/total - TARGET_NVDA)*100 if total>0 else 0},
        {"id": "BTC",   "name": "Bitcoin",    "units": UNITS_BTC,
         "price_eur": btc_price_eur, "value_eur": val_btc,
         "weight_pct": (val_btc/total*100) if total>0 else 0,
         "target_pct": TARGET_BTC,
         "drift_pct":  (val_btc/total - TARGET_BTC)*100 if total>0 else 0},
    ]

    for p in positions:
        print(f"   {p['name']}: {p['weight_pct']:.1f}% "
              f"(target {p['target_pct']*100:.0f}%, drift {p['drift_pct']:+.1f}%)")

    # Alertas automáticas de drift
    alerts = []
    for p in positions:
        if abs(p["drift_pct"]) > 10:
            alerts.append({
                "type": "DRIFT_ALERT",
                "asset": p["name"],
                "message": f"{p['name']}: {p['weight_pct']:.1f}% actual vs "
                           f"{p['target_pct']*100:.0f}% target (drift {p['drift_pct']:+.1f}%)",
                "severity": "high" if abs(p["drift_pct"]) > 15 else "medium"
            })
        if p["id"] == "NVDA" and p["weight_pct"] > 20:
            alerts.append({
                "type": "CONCENTRATION_ALERT",
                "asset": "NVIDIA",
                "message": f"NVIDIA supera el 20% de la cartera ({p['weight_pct']:.1f}%). "
                           f"Riesgo de concentración en activo individual.",
                "severity": "high"
            })

    # Revisión Kahneman trimestral
    review_data = {"review": "", "calibration": {}, "performed": False}
    if IS_QUARTERLY and ANTHROPIC_KEY:
        print("   → Revisión trimestral Kahneman...")
        news_nvda = fetch_news("NVIDIA earnings AI growth 2026", 3)
        news_btc  = fetch_news("Bitcoin institutional adoption regulation 2026", 2)
        review_data = kahneman_core_review(positions, macro_ctx, news_nvda, news_btc)
        review_data["performed"] = True
        review_data["date"] = DATE_ES
    else:
        review_data["next_review"] = "Próximo lunes de inicio de trimestre"

    print(f"   ✓ Cartera core analizada | Total ~€{total:.0f}")
    return {
        "positions":    positions,
        "total_eur":    round(total, 2),
        "alerts":       alerts,
        "review":       review_data,
        "is_quarterly": IS_QUARTERLY,
        "timestamp":    DATE_ES,
    }

# ══════════════════════════════════════════════════════
# PIPELINE PRINCIPAL
# ══════════════════════════════════════════════════════

def run():
    print(f"\n{'='*58}\nGeoMacro Intel v7 — {DATE_ES}\n{'='*58}")
    print(f"Revisión trimestral core: {'SÍ' if IS_QUARTERLY else 'NO'}\n")

    results = {
        "generated_at":  DATE_ES,
        "quarter":       QUARTER,
        "version":       "7.0",
        "is_quarterly":  IS_QUARTERLY,
        "trading":       [],
        "regions":       [],
        "global_risks":  [],
        "core":          {},
        "macro":         {},
        "ranking":       [],
        "alerts":        [],
        "audit_log":     [],
    }

    # ── Macro global ──
    print("\n→ Datos macro globales...")
    fx_rates = fetch_fx()
    eia      = fetch_eia()
    fred_d   = {}
    if FRED_KEY:
        for sid, lbl in [("FEDFUNDS","fed_funds_rate"), ("T10Y2Y","yield_curve_spread"),
                         ("DTWEXBGS","dxy_index")]:
            obs = fetch_fred(sid)
            if obs:
                fred_d[lbl] = round(obs[0][1], 3)
                print(f"   FRED {lbl}: {obs[0][1]:.3f}")
        # CPI YoY real: (ultimo / hace 12 meses - 1) * 100
        cpi_obs = fetch_fred("CPIAUCSL")
        if cpi_obs and len(cpi_obs) >= 13:
            cpi_yoy = round((cpi_obs[0][1] / cpi_obs[12][1] - 1) * 100, 2)
            fred_d["cpi_yoy"] = cpi_yoy
            print(f"   FRED cpi_yoy: {cpi_yoy}%")

    results["macro"] = {
        "fx_rates":  fx_rates,
        "fred":      fred_d,
        "eia_crude": eia,
        "timestamp": DATE_ES,
    }

    macro_ctx = (f"FED funds: {fred_d.get('fed_funds_rate','N/A')}%, "
                 f"Yield curve 10Y-2Y: {fred_d.get('yield_curve_spread','N/A')}bps, "
                 f"DXY: {fred_d.get('dxy_index','N/A')}, "
                 f"EUR/USD: {fx_rates.get('EUR','N/A')}, "
                 f"USD/INR: {fx_rates.get('INR','N/A')}, "
                 f"Inventarios crudos vs avg5Y: "
                 f"{eia.get('vs_avg','N/A') if eia else 'N/A'}%")

    # ── Módulo 1: Trading ──
    trading_results = run_trading_module(fx_rates, eia, macro_ctx)
    results["trading"] = trading_results
    results["audit_log"].extend([a["log"] for a in trading_results if a.get("log")])

    # ── Módulo 2: Geoestrategico ──
    geo_results = run_geo_module(fx_rates, macro_ctx)
    results["regions"] = geo_results
    for r in geo_results:
        results["audit_log"].append({
            "module": "geo", "asset": r["name"], "timestamp": DATE_ES,
            "scores": {"composite": r["score"]},
            "signal": r["analysis"]["calibration"].get("signal", "—"),
            "claude_calls": 3 if ANTHROPIC_KEY else 0,
            "status": "complete"
        })

    # ── Módulo 3: Core ──
    core_result = run_core_module(fx_rates, macro_ctx)
    results["core"] = core_result
    results["audit_log"].append({
        "module": "core", "asset": "Cartera Core", "timestamp": DATE_ES,
        "scores": {"composite": None},
        "signal": "LARGO_PLAZO",
        "claude_calls": 1 if (IS_QUARTERLY and ANTHROPIC_KEY) else 0,
        "status": "quarterly_review" if IS_QUARTERLY else "monitoring_only"
    })

    # ── Riesgos globales ──
    print("\n→ Riesgos geopolíticos globales...")
    if ANTHROPIC_KEY:
        results["global_risks"] = kahneman_global_risks(
            trading_results, geo_results, macro_ctx)

    # ── Ranking unificado (trading + geo, NO mezcla core) ──
    print("\n→ Ranking unificado...")
    all_items = []

    for a in trading_results:
        if not a.get("quant"): continue
        cal = a["analysis"].get("calibration", {})
        all_items.append({
            "name":       a["meta"]["name"],
            "type":       a["meta"]["type"],
            "category":   "commodity",
            "score":      a["quant"]["composite_score"],
            "signal":     cal.get("signal", "—"),
            "prob":       cal.get("prob"),
            "kelly":      a["quant"].get("kelly"),
            "vol":        a["quant"].get("volatility"),
            "price":      a["quant"].get("price"),
            "summary":    cal.get("summary", ""),
            "conviction": cal.get("conviction", ""),
        })

    for r in geo_results:
        cal = r["analysis"].get("calibration", {})
        all_items.append({
            "name":       r["name"],
            "type":       r["region"],
            "category":   "region",
            "score":      r["score"],
            "signal":     cal.get("signal", "—"),
            "prob":       cal.get("prob"),
            "kelly":      None,
            "vol":        None,
            "price":      None,
            "summary":    cal.get("summary", ""),
            "conviction": cal.get("conviction", ""),
            "gdp":        r.get("gdp_latest"),
            "sector":     r.get("sector"),
        })

    all_items.sort(key=lambda x: x["score"], reverse=True)
    results["ranking"] = [{"rank": i+1, **item} for i, item in enumerate(all_items)]

    # ── Alertas consolidadas ──
    print("\n→ Alertas...")
    for a in trading_results:
        q   = a.get("quant", {}) or {}
        cal = a["analysis"].get("calibration", {})
        nm  = a["meta"]["name"]
        if q.get("rsi") and q["rsi"] < 30:
            results["alerts"].append({"type":"RSI_OVERSOLD","module":"trading","asset":nm,
                "value":q["rsi"],"message":f"{nm}: RSI={q['rsi']} — sobreventa","severity":"high"})
        if q.get("composite_score",0) >= 70 and cal.get("signal") == "COMPRAR":
            results["alerts"].append({"type":"BUY_SIGNAL","module":"trading","asset":nm,
                "value":q["composite_score"],
                "message":f"{nm}: score {q['composite_score']}/100 + COMPRAR (p={cal.get('prob')}%)","severity":"high"})
        if q.get("max_drawdown_6m",0) > 25:
            results["alerts"].append({"type":"HIGH_DRAWDOWN","module":"trading","asset":nm,
                "value":q["max_drawdown_6m"],
                "message":f"{nm}: drawdown máximo 6M={q['max_drawdown_6m']}%","severity":"medium"})

    for r in geo_results:
        cal = r["analysis"].get("calibration", {})
        if cal.get("signal") == "OPORTUNIDAD_ALTA" and cal.get("prob",0) >= 65:
            results["alerts"].append({"type":"GEO_OPPORTUNITY","module":"geo","asset":r["name"],
                "value":r["score"],
                "message":f"{r['name']}: oportunidad alta (p={cal.get('prob')}%)","severity":"high"})

    # Alertas core
    results["alerts"].extend(core_result.get("alerts", []))

    # ── Guardar ──
    total_claude = sum(l.get("claude_calls",0) for l in results["audit_log"])
    print(f"\n✓ Trading: {len(trading_results)} activos")
    print(f"✓ Geo: {len(geo_results)} regiones")
    print(f"✓ Core: {'revisión completa' if IS_QUARTERLY else 'monitorización'}")
    print(f"✓ Riesgos: {len(results['global_risks'])}")
    print(f"✓ Alertas: {len(results['alerts'])}")
    print(f"✓ Llamadas Claude totales: {total_claude}")

    out = os.path.join(os.path.dirname(__file__), "results.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"✓ Guardado: {out}")
    print(f"✓ Pipeline completado — {DATE_ES}\n")

if __name__ == "__main__":
    run()

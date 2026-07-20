#!/usr/bin/env python3
"""
GeoMacro Intel v8 — Sistema Profesional Completo
=================================================
MÓDULO 1: Trading — Commodities (diario, con auto-auditoría previa)
MÓDULO 2: Geoestrategico (diario, Kahneman estructurado v8)
MÓDULO 3: Core Riguroso (Sharpe, drawdown, correlaciones, checklist 10 puntos)
MÓDULO 4: Backtesting (semanal, validación histórica 3 años)
MÓDULO 5: Alertas Telegram (crítico / importante / informativo)
MÓDULO 6: Auto-auditoría (checklist 8 puntos antes de cualquier señal)
"""

import os, json, math, time, datetime, statistics, urllib.request, urllib.parse, inspect
from urllib.request import urlopen, Request
import tjl_validation as tjl
try:
    import anthropic as anthropic_sdk
    ANTHROPIC_SDK = True
except ImportError:
    ANTHROPIC_SDK = False

ANTHROPIC_KEY    = os.environ.get("ANTHROPIC_API_KEY", "")
KAHNEMAN_ENABLED = os.environ.get("KAHNEMAN_ENABLED", "false").lower() == "true"
FRED_KEY         = os.environ.get("FRED_API_KEY", "")
NEWS_KEY         = os.environ.get("NEWS_API_KEY", "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
GIST_TOKEN    = os.environ.get("GIST_TOKEN", "")
GIST_ID       = os.environ.get("GIST_ID", "")  # se rellena tras crear el gist por primera vez

PORTFOLIO_EUR = float(os.environ.get("PORTFOLIO_VALUE_EUR", "6601.14"))
UNITS_BTC     = float(os.environ.get("UNITS_BTC",   "0.019975"))
UNITS_NVDA    = float(os.environ.get("UNITS_NVDA",  "13.167854"))
UNITS_MSCI    = float(os.environ.get("UNITS_MSCI",  "33.142387"))

TARGET_MSCI = 0.70
TARGET_NVDA = 0.10
TARGET_BTC  = 0.05

NOW      = datetime.datetime.now(datetime.timezone.utc)
TODAY    = NOW.strftime("%Y-%m-%d")
QUARTER  = f"Q{math.ceil(NOW.month/3)} {NOW.year}"
DATE_ES  = NOW.strftime("%d/%m/%Y %H:%M UTC")
IS_QUARTERLY = (NOW.weekday() == 0 and NOW.day <= 7 and NOW.month in [1,4,7,10])
IS_WEEKLY    = (NOW.weekday() == 0)
if os.environ.get("FORCE_CORE_REVIEW"): IS_QUARTERLY = True
if os.environ.get("FORCE_BACKTEST"):    IS_WEEKLY    = True

# ── MODELO CLAUDE ACTUALIZADO ─────────────────────────
CLAUDE_MODEL = "claude-sonnet-4-6"  # modelo actual abril 2026

# Universo: 40 acciones USA líquidas (mega/large caps de todos los sectores,
# selección sistemática por liquidez — incluye ganadores Y perdedores, sin cherry-picking).
# Validado out-of-sample con estrategia trend_joined_long: +1.49%/trade, 124 trades OOS.
TRADING_ASSETS = [
    {"id":"NVDA", "name":"NVIDIA",     "type":"tech",       "unit":"USD"},
    {"id":"AMD",  "name":"AMD",        "type":"tech",       "unit":"USD"},
    {"id":"MU",   "name":"Micron",     "type":"tech",       "unit":"USD"},
    {"id":"AVGO", "name":"Broadcom",   "type":"tech",       "unit":"USD"},
    {"id":"QCOM", "name":"Qualcomm",   "type":"tech",       "unit":"USD"},
    {"id":"INTC", "name":"Intel",      "type":"tech",       "unit":"USD"},
    {"id":"MSFT", "name":"Microsoft",  "type":"tech",       "unit":"USD"},
    {"id":"AAPL", "name":"Apple",      "type":"tech",       "unit":"USD"},
    {"id":"GOOGL","name":"Alphabet",   "type":"tech",       "unit":"USD"},
    {"id":"META", "name":"Meta",       "type":"tech",       "unit":"USD"},
    {"id":"AMZN", "name":"Amazon",     "type":"consumer",   "unit":"USD"},
    {"id":"TSLA", "name":"Tesla",      "type":"consumer",   "unit":"USD"},
    {"id":"NFLX", "name":"Netflix",    "type":"consumer",   "unit":"USD"},
    {"id":"DIS",  "name":"Disney",     "type":"consumer",   "unit":"USD"},
    {"id":"NKE",  "name":"Nike",       "type":"consumer",   "unit":"USD"},
    {"id":"SBUX", "name":"Starbucks",  "type":"consumer",   "unit":"USD"},
    {"id":"MCD",  "name":"McDonalds",  "type":"consumer",   "unit":"USD"},
    {"id":"KO",   "name":"CocaCola",   "type":"consumer",   "unit":"USD"},
    {"id":"PEP",  "name":"PepsiCo",    "type":"consumer",   "unit":"USD"},
    {"id":"WMT",  "name":"Walmart",    "type":"consumer",   "unit":"USD"},
    {"id":"JPM",  "name":"JPMorgan",   "type":"financial",  "unit":"USD"},
    {"id":"BAC",  "name":"BofA",       "type":"financial",  "unit":"USD"},
    {"id":"GS",   "name":"Goldman",    "type":"financial",  "unit":"USD"},
    {"id":"V",    "name":"Visa",       "type":"financial",  "unit":"USD"},
    {"id":"MA",   "name":"Mastercard", "type":"financial",  "unit":"USD"},
    {"id":"BA",   "name":"Boeing",     "type":"industrial", "unit":"USD"},
    {"id":"CAT",  "name":"Caterpillar","type":"industrial", "unit":"USD"},
    {"id":"GE",   "name":"GE",         "type":"industrial", "unit":"USD"},
    {"id":"XOM",  "name":"Exxon",      "type":"energy",     "unit":"USD"},
    {"id":"CVX",  "name":"Chevron",    "type":"energy",     "unit":"USD"},
    {"id":"JNJ",  "name":"J&J",        "type":"health",     "unit":"USD"},
    {"id":"PFE",  "name":"Pfizer",     "type":"health",     "unit":"USD"},
    {"id":"LLY",  "name":"EliLilly",   "type":"health",     "unit":"USD"},
    {"id":"UNH",  "name":"UnitedHealth","type":"health",    "unit":"USD"},
    {"id":"MRNA", "name":"Moderna",    "type":"health",     "unit":"USD"},
    {"id":"PYPL", "name":"PayPal",     "type":"financial",  "unit":"USD"},
    {"id":"CRM",  "name":"Salesforce", "type":"tech",       "unit":"USD"},
    {"id":"ORCL", "name":"Oracle",     "type":"tech",       "unit":"USD"},
    {"id":"UBER", "name":"Uber",       "type":"tech",       "unit":"USD"},
    {"id":"ABNB", "name":"Airbnb",     "type":"consumer",   "unit":"USD"},
]


CORE_ASSETS = [
    {"id":"NVDA",    "name":"NVIDIA",     "ticker":"NVDA",    "currency":"USD"},
    {"id":"BTC-EUR", "name":"Bitcoin",    "ticker":"BTC-EUR", "currency":"EUR"},
    {"id":"IWDA.AS", "name":"MSCI World", "ticker":"IWDA.AS", "currency":"EUR"},
]

# ── HTTP ──────────────────────────────────────────────

def fetch(url, headers=None, timeout=14):
    try:
        req = Request(url, headers=headers or {"User-Agent":"GeoMacroIntel/8.0"})
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

# ── TELEGRAM ──────────────────────────────────────────

def send_telegram(message, level="info"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"  WARN Telegram: token={bool(TELEGRAM_TOKEN)} chat={bool(TELEGRAM_CHAT_ID)}")
        return False
    print(f"  -> Telegram {level}: credenciales configuradas")
    icons = {"critical":"🚨","important":"⚠️","info":"ℹ️"}
    text = f"{icons.get(level,'i')} GeoMacro Intel v8\n{message}\n{DATE_ES}"
    payload = {"chat_id": str(TELEGRAM_CHAT_ID), "text": text}
    r = post_json(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        payload,
        {"Content-Type": "application/json"})
    if r and r.get("ok"):
        print(f"  OK Telegram {level}: mensaje_id={r.get('result',{}).get('message_id')}")
        return True
    else:
        print(f"  ERROR Telegram {level}: {r}")
        return False

def build_telegram_summary(results):
    """Resumen no transaccional. Las entradas/cierres TJL se notifican aparte."""
    alerts = results.get("alerts", [])
    core = results.get("core", {})
    fred = results.get("macro", {}).get("fred", {})

    high_alerts = [x for x in alerts if x.get("severity") == "high"
                   and x.get("type") not in ("BUY_SIGNAL", "SIGNAL_BLOCKED_BY_GATE")]
    if high_alerts:
        lines = [f"• {a['asset']}: {a['message'][:60]}" for a in high_alerts[:8]]
        extra = f"\n(+{len(high_alerts)-8} más)" if len(high_alerts) > 8 else ""
        send_telegram("📊 Avisos técnicos del día:\n" + "\n".join(lines) + extra, "info")
        time.sleep(0.5)

    positions = core.get("positions", [])
    if positions:
        total = core.get("total_eur", 0)
        msg = f"Cartera Core — EUR{total:,.2f}\n"
        for position in positions:
            drift = position.get("drift_pct", 0)
            icon = "🔴" if abs(drift) > 10 else "🟡" if abs(drift) > 5 else "🟢"
            msg += f"{icon} {position['name']}: {position['weight_pct']:.1f}% (drift {drift:+.1f}%)\n"
        send_telegram(msg, "important")
        time.sleep(0.5)

    if fred:
        send_telegram(
            f"Macro | FED:{fred.get('fed_funds_rate','?')}% "
            f"DXY:{fred.get('dxy_index','?')} "
            f"CPI:{fred.get('cpi_yoy','?')}% "
            f"Curve:{fred.get('yield_curve_spread','?')}bps",
            "info")


def send_tjl_notifications(paper_data, runtime, verdict_events):
    """Envía alertas idempotentes después de persistir el estado crítico."""
    changed = False
    by_id = {t.get("id"): t for t in paper_data.get("trades", [])}

    for trade in tjl.pending_close_alerts(paper_data):
        if send_telegram(tjl.build_close_alert_text(paper_data, trade), "critical"):
            tjl.mark_close_alert_sent(trade)
            changed = True

    blocked = sorted(set(runtime.get("blocked_assets", [])))
    if blocked and tjl.should_send_blocked_alert(paper_data, blocked):
        if send_telegram(tjl.build_blocked_signal_text(paper_data, blocked), "important"):
            tjl.record_blocked_alert(paper_data, blocked)
            changed = True

    for trade_id in runtime.get("accepted_trade_ids", []):
        trade = by_id.get(trade_id)
        if not trade or trade.get("entry_alert_sent"):
            continue
        msg = (
            f"🟢 SEÑAL TJL ACEPTADA — {trade['asset']} ({trade['asset_id']})\n"
            f"Entrada: ${trade['entry_price']}\n"
            f"Stop: ${trade['stop_price']} ({trade.get('stop_pct')}%)\n"
            f"Target: ${trade['target_price']} ({trade.get('target_pct')}%)\n"
            f"Ronda de validación: {trade.get('validation_round')}"
        )
        if send_telegram(msg, "critical"):
            trade["entry_alert_sent"] = True
            changed = True

    if verdict_events:
        for message in verdict_events:
            if send_telegram(message, "critical"):
                tjl.mark_verdict_alert_sent(paper_data, message=message)
                changed = True

    if changed:
        save_paper_trades(paper_data)
    return paper_data

# ── DATA FETCHERS ─────────────────────────────────────

def fetch_yahoo(ticker, days=730):
    end = int(time.time()); start = end - days*86400
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
           f"?interval=1d&period1={start}&period2={end}")
    d = fetch(url)
    if not d: return None
    try:
        res    = d["chart"]["result"][0]
        closes = res["indicators"]["quote"][0]["close"]
        times  = res["timestamp"]
        return [(t,c) for t,c in zip(times,closes) if c is not None]
    except Exception as e:
        print(f"  WARN Yahoo {ticker}: {e}"); return None

def fetch_yahoo_ohlc(ticker, days=730):
    """OHLC diario completo — necesario para la estrategia de rupturas
    (niveles de máximos) y para el ATR real. Devuelve dict o None."""
    end = int(time.time()); start = end - days*86400
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
           f"?interval=1d&period1={start}&period2={end}")
    d = fetch(url)
    if not d: return None
    try:
        res = d["chart"]["result"][0]
        q = res["indicators"]["quote"][0]
        rows = [(t,o,h,l,c) for t,o,h,l,c in zip(res["timestamp"], q["open"],
                q["high"], q["low"], q["close"]) if c is not None and h is not None and l is not None]
        if not rows: return None
        return {"times":[r[0] for r in rows], "opens":[r[1] for r in rows],
                "highs":[r[2] for r in rows], "lows":[r[3] for r in rows],
                "closes":[r[4] for r in rows]}
    except Exception as e:
        print(f"  WARN Yahoo OHLC {ticker}: {e}"); return None

def atr_ohlc(highs, lows, closes, n=14):
    """ATR real con máximos/mínimos (más preciso que el proxy de cierres)."""
    if len(closes) < n+1: return None
    trs = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
           for i in range(-n, 0)]
    return round(sum(trs)/n, 4)

def fetch_fred(sid, limit=60):
    if not FRED_KEY: return None
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={sid}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit={limit}")
    d = fetch(url)
    if not d: return None
    try:
        return [(o["date"],float(o["value"])) for o in d["observations"]
                if o["value"] not in (".","")] 
    except: return None

def fetch_fx():
    d = fetch("https://api.frankfurter.app/latest?from=USD"
              "&to=EUR,INR,BRL,PLN,SAR,ARS,IDR,MXN,CNY,VND,GBP")
    return d.get("rates",{}) if d else {}

def fetch_eia():
    if not FRED_KEY: return None
    # WCRSTUS1 = Weekly Ending Stocks of Crude Oil (puede dar 400 si serie retirada)
    # Alternativa: WTESTUS1 o datos EIA directos
    obs = None
    for sid in ["WCRSTUS1", "WTESTUS1", "WTOTUSA1"]:
        obs = fetch_fred(sid)
        if obs and len(obs) >= 2:
            print(f"   EIA OK via serie {sid}: {obs[0][1]:.1f}")
            break
        else:
            print(f"   EIA serie {sid}: sin datos, probando siguiente...")
    if not obs or len(obs)<2: 
        print("   EIA: todas las series fallaron, continuando sin datos EIA")
        return None
    l,p = obs[0][1],obs[1][1]
    avg5y = statistics.mean([o[1] for o in obs[:260]]) if len(obs)>=260 else None
    return {"latest":l,"prev":p,
            "vs_avg":round((l/avg5y-1)*100,1) if avg5y else None}

def fetch_news(query, n=5):
    if not NEWS_KEY: return []
    url = (f"https://newsapi.org/v2/everything"
           f"?q={urllib.parse.quote(query)}"
           f"&language=en&sortBy=publishedAt&pageSize={n}&apiKey={NEWS_KEY}")
    d = fetch(url)
    if not d or d.get("status")!="ok": return []
    return [{"title":a.get("title","")[:100],"source":a.get("source",{}).get("name","")}
            for a in d.get("articles",[])[:n]]

# ── INDICADORES TÉCNICOS ──────────────────────────────

def sma(p,n): return sum(p[-n:])/n if len(p)>=n else None

def ema(p,n):
    if len(p)<n: return None
    k=2/(n+1); e=sum(p[:n])/n
    for x in p[n:]: e=x*k+e*(1-k)
    return e

def rsi(p,n=14):
    if len(p)<n+1: return None
    g,l=[],[]
    for i in range(1,len(p)):
        d=p[i]-p[i-1]; g.append(max(d,0)); l.append(max(-d,0))
    ag=sum(g[-n:])/n; al=sum(l[-n:])/n
    if al==0: return 100.0
    return round(100-100/(1+ag/al),2)

def macd(p,fast=12,slow=26,signal=9):
    if len(p)<slow+signal: return None,None
    mv=[]
    for i in range(slow,len(p)+1):
        ef=ema(p[:i],fast); es=ema(p[:i],slow)
        if ef and es: mv.append(ef-es)
    ef=ema(p,fast); es=ema(p,slow)
    if not ef or not es: return None,None
    ml=ef-es; sig=ema(mv,signal) if len(mv)>=signal else None
    return round(ml,4), round(ml-sig,4) if sig else None

def bollinger(p,n=20,m=2):
    if len(p)<n: return None,None,None
    w=p[-n:]; mid=sum(w)/n; std=statistics.stdev(w)
    return round(mid-m*std,4),round(mid,4),round(mid+m*std,4)

def atr_calc(closes,n=14):
    trs=[max(closes[i]-closes[i-1],abs(closes[i]-closes[i-1]))
         for i in range(1,len(closes))]
    return round(sum(trs[-n:])/n,4) if len(trs)>=n else None

def ann_vol(closes,w=126):
    c=closes[-w:] if len(closes)>=w else closes
    rets=[math.log(c[i]/c[i-1]) for i in range(1,len(c)) if c[i-1]>0]
    return round(statistics.stdev(rets)*math.sqrt(252)*100,2) if len(rets)>5 else None

def max_dd(closes,w=126):
    c=closes[-w:] if len(closes)>=w else closes
    peak=c[0]; mdd=0
    for x in c:
        if x>peak: peak=x
        dd=(peak-x)/peak
        if dd>mdd: mdd=dd
    return round(mdd*100,2)

def var95(closes):
    rets=[math.log(closes[i]/closes[i-1])
          for i in range(1,len(closes)) if closes[i-1]>0]
    if len(rets)<30: return None
    mu=statistics.mean(rets); sig=statistics.stdev(rets)
    return round(abs(mu+1.645*sig)*100,3)

def sharpe_ratio(closes,rf=0.0364,w=252):
    c=closes[-w:] if len(closes)>=w else closes
    rets=[math.log(c[i]/c[i-1]) for i in range(1,len(c)) if c[i-1]>0]
    if len(rets)<20: return None
    mu=statistics.mean(rets)*252; sig=statistics.stdev(rets)*math.sqrt(252)
    return round((mu-rf)/sig,3) if sig>0 else None

def calmar_ratio(closes,w=252):
    if len(closes)<2: return None
    c=closes[-w:] if len(closes)>=w else closes
    ret_ann=(c[-1]/c[0])**(252/len(c))-1
    mdd=max_dd(c,len(c))/100
    return round(ret_ann/mdd,3) if mdd>0 else None

def correlation(x,y):
    n=min(len(x),len(y))
    if n<10: return None
    x,y=x[-n:],y[-n:]
    mx,my=sum(x)/n,sum(y)/n
    num=sum((x[i]-mx)*(y[i]-my) for i in range(n))
    dx=math.sqrt(sum((v-mx)**2 for v in x))
    dy=math.sqrt(sum((v-my)**2 for v in y))
    return round(num/(dx*dy),3) if dx*dy>0 else None

def kelly_frac(win_p,win_r,loss_r,fraction=0.50):
    if loss_r==0: return 0
    b=abs(win_r/loss_r); k=(b*win_p-(1-win_p))/b
    return round(max(0,min(k*fraction,0.25)),4)

def tech_score(closes,rsi_v,macd_h,bb_low,bb_high):
    if not closes or len(closes)<50: return 50
    p=closes[-1]; score=50
    s20=sma(closes,20); s50=sma(closes,50)
    s200=sma(closes,200) if len(closes)>=200 else None
    if s20 and s50: score+=8 if s20>s50 else -8
    if s200:        score+=7 if p>s200  else -7
    if rsi_v:
        if rsi_v<30:   score+=15
        elif rsi_v<45: score+=8
        elif rsi_v>70: score-=15
        elif rsi_v>55: score-=5
    if macd_h: score+=10 if macd_h>0 else -10
    if bb_low and bb_high:
        rng=bb_high-bb_low
        if rng>0:
            pos=(p-bb_low)/rng
            if pos<0.2: score+=8
            elif pos>0.8: score-=8
    if len(closes)>=63:
        mom=(closes[-1]/closes[-63]-1)*100
        if mom>5: score+=5
        elif mom<-5: score-=5
    return max(0,min(100,round(score)))

def fund_score(gdp_vals,fx_rates,eia,asset_type):
    score=50
    if gdp_vals:
        avg=statistics.mean(gdp_vals)
        if avg>5: score+=12
        elif avg>3: score+=6
        elif avg<1: score-=8
    eur_raw=fx_rates.get("EUR")  # USD/EUR: cuántos EUR vale 1 USD
    eur = round(1/eur_raw, 4) if eur_raw else None  # convertir a EUR/USD convencional
    if eur:
        if eur>1.08: score+=8   # EUR fuerte vs USD → favorable commodities
        elif eur<1.00: score-=8  # EUR débil vs USD → desfavorable
    if asset_type=="energy" and eia and eia.get("vs_avg") is not None:
        if eia["vs_avg"]<-5: score+=12
        elif eia["vs_avg"]>5: score-=10
    if asset_type=="critical": score+=10
    return max(0,min(100,round(score)))

def comp_score(tech,fund): return round(0.40*tech+0.60*fund)

def entry_levels(closes,atr_v,direction="long"):
    if not closes or not atr_v: return None
    p=closes[-1]
    if direction=="short":
        stop=round(p+2.0*atr_v,4); target=round(p-4.0*atr_v,4)
        return {"entry":round(p,4),"stop":stop,"target":target,"rr":2.0,
                "stop_pct":round((stop-p)/p*100,2),
                "target_pct":round((p-target)/p*100,2)}
    stop=round(p-2.0*atr_v,4); target=round(p+4.0*atr_v,4)
    return {"entry":round(p,4),"stop":stop,"target":target,"rr":2.0,
            "stop_pct":round((p-stop)/p*100,2),
            "target_pct":round((target-p)/p*100,2)}

# ── AUTO-AUDITORÍA ────────────────────────────────────

def auto_audit(name,closes,qdata):
    checks=[]; warnings=[]; passed=True

    checks.append({"check":"datos_frescos","ok":bool(closes)})
    if not closes: warnings.append(f"{name}: sin datos"); passed=False

    ok=len(closes)>=200
    checks.append({"check":"historial_200d","ok":ok})
    if not ok: warnings.append(f"{name}: historial {len(closes)}d < 200d")

    ok=qdata.get("rsi") is not None
    checks.append({"check":"rsi_calculado","ok":ok})
    if not ok: warnings.append(f"{name}: RSI no calculado")

    ok=qdata.get("atr") is not None
    checks.append({"check":"atr_calculado","ok":ok})
    if not ok: warnings.append(f"{name}: ATR no calculado"); passed=False

    vol=qdata.get("volatility")
    ok=vol is not None and vol<200
    checks.append({"check":"volatilidad_operable","ok":ok})
    if not ok: warnings.append(f"{name}: vol extrema ({vol}%)")

    tech=qdata.get("technical_score",50); fund=qdata.get("fundamental_score",50)
    ok=abs(tech-fund)<30
    checks.append({"check":"scores_consistentes","ok":ok})
    if not ok: warnings.append(f"{name}: divergencia tech/fund ({tech} vs {fund})")

    ok=qdata.get("price",0)>0
    checks.append({"check":"precio_valido","ok":ok})
    if not ok: warnings.append(f"{name}: precio invalido"); passed=False

    ok=qdata.get("levels") is not None
    checks.append({"check":"niveles_calculados","ok":ok})
    if not ok: warnings.append(f"{name}: niveles no calculados")

    failed=[c for c in checks if not c["ok"]]
    if len(failed)>=3: passed=False

    return {"passed":passed,"checks":checks,"warnings":warnings,
            "score":round((len(checks)-len(failed))/len(checks)*100)}

# ── BACKTESTING ───────────────────────────────────────

def run_backtest_asset(name,closes_full):
    if len(closes_full)<500: return None
    results_bt=[]; window=200; step=20

    for i in range(window,len(closes_full)-step,step):
        seg=[c for _,c in closes_full[:i]] if isinstance(closes_full[0],tuple) else closes_full[:i]
        if len(seg)<window: continue
        rsi_v=rsi(seg); _,macd_h=macd(seg); bb_low,_,bb_h=bollinger(seg)
        t=tech_score(seg,rsi_v,macd_h,bb_low,bb_h)
        signal="buy" if t>60 else ("sell" if t<40 else "hold")
        future=closes_full[i:i+step]
        fc=[c for _,c in future] if isinstance(future[0],tuple) else future
        if len(fc)<step: continue
        ret=(fc[-1]/fc[0]-1)*100
        results_bt.append({
            "signal":signal,"return_pct":round(ret,3),
            "correct":(signal=="buy" and ret>0) or
                      (signal=="sell" and ret<0) or signal=="hold"})

    if not results_bt: return None
    sigs=[r for r in results_bt if r["signal"]!="hold"]
    buys=[r for r in results_bt if r["signal"]=="buy"]
    wr=round(sum(1 for r in sigs if r["correct"])/len(sigs)*100,1) if sigs else 0
    avg_r=round(statistics.mean([r["return_pct"] for r in sigs]),3) if sigs else 0
    closes_list=[c for _,c in closes_full] if isinstance(closes_full[0],tuple) else closes_full
    yrs=len(closes_list)/252
    bh_ann=round((closes_list[-1]/closes_list[0])**(1/yrs)*100-100,2) if yrs>0 else 0
    return {"asset":name,"win_rate_pct":wr,"avg_return_pct":avg_r,
            "total_signals":len(sigs),"bh_annual_pct":bh_ann,
            "period_days":len(closes_list)}

# ── CLAUDE / KAHNEMAN v8 ──────────────────────────────

SYSTEM_PROMPT = """Eres un analista cuantitativo institucional senior.
REGLAS: (1) Separa hechos de inferencias siempre. (2) Toda probabilidad con intervalo p=X% +/-Y%. 
(3) Pre-mortem siempre 3 riesgos en formato estandarizado. (4) JSON final sin markdown ni backticks.
(5) Sin "podria" ni "quizas" sin cuantificar. (6) Datos insuficientes = reducir conviction explicitamente.
Respondes en español. Conciso y riguroso."""

def claude(prompt, max_tokens=800):
    if not ANTHROPIC_KEY:
        print("  WARN claude: ANTHROPIC_KEY no disponible")
        return None
    print(f"    [Claude] llamando {CLAUDE_MODEL} max_tokens={max_tokens}...")
    # Usar SDK oficial si está disponible
    if ANTHROPIC_SDK:
        try:
            client = anthropic_sdk.Anthropic(api_key=ANTHROPIC_KEY)
            message = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=max_tokens,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}]
            )
            text = message.content[0].text
            print(f"    [Claude] OK (SDK): {len(text)} chars")
            return text
        except Exception as e:
            import traceback
            print(f"    [Claude] ERROR SDK tipo={type(e).__name__}: {e}")
            print(f"    [Claude] ERROR SDK detalle: {traceback.format_exc()[:300]}")
            return None
    # Fallback: urllib manual
    payload = {
        "model": CLAUDE_MODEL,
        "max_tokens": max_tokens,
        "system": SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": prompt}]
    }
    headers = {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01"
    }
    r = post_json("https://api.anthropic.com/v1/messages", payload, headers)
    if not r:
        print("    [Claude] ERROR: respuesta nula del servidor")
        return None
    if r.get("error"):
        err = r["error"]
        print(f"    [Claude] ERROR API: type={err.get('type')} message={err.get('message')}")
        return None
    cnt = r.get("content", [])
    if not cnt:
        print(f"    [Claude] ERROR: content vacío")
        return None
    text = cnt[0].get("text", "")
    print(f"    [Claude] OK (urllib): {len(text)} chars")
    return text

def parse_cal(raw):
    """Parse calibration JSON robustly."""
    if not raw: return {}
    clean = raw
    clean = clean.replace("```json","").replace("```","")
    s = clean.find("{")
    e = clean.rfind("}") + 1
    if s == -1 or e <= s:
        return {"signal":"ESPERAR","prob":50,"prob_interval":20,
                "summary": raw[:120].replace("\n"," ").strip()}
    json_str = clean[s:e]
    try:
        return json.loads(json_str)
    except:
        pass
    try:
        import re
        json_str2 = re.sub(r'(?<!\)"([^"]*)"(?=\s*[,}])', 
                           lambda m: json.dumps(m.group(1)), json_str)
        return json.loads(json_str2)
    except:
        pass
    import re
    result = {"signal":"ESPERAR","prob":50,"prob_interval":20}
    for field, pattern in [
        ("signal",       r'"signal"\s*:\s*"([^"]+)"'),
        ("prob",         r'"prob"\s*:\s*(\d+)'),
        ("prob_interval",r'"prob_interval"\s*:\s*(\d+)'),
        ("horizon",      r'"horizon"\s*:\s*"([^"]+)"'),
        ("summary",      r'"summary"\s*:\s*"([^"]+)"'),
        ("conviction",   r'"conviction"\s*:\s*"([^"]+)"'),
        ("invalidation", r'"invalidation"\s*:\s*"([^"]+)"'),
        ("top_risk",     r'"top_risk"\s*:\s*"([^"]+)"'),
    ]:
        m = re.search(pattern, json_str)
        if m:
            val = m.group(1)
            result[field] = int(val) if field in ("prob","prob_interval") else val
    if not result.get("summary"):
        result["summary"] = raw[:120].replace("\n"," ").strip()
    return result

def kahneman_trading(asset,qdata,macro_ctx,audit):
    if not audit.get("passed",False):
        return {"thesis":"Auditoria fallida — señal no emitida","premortem":"",
                "calibration":{"signal":"ESPERAR","prob":50,"prob_interval":25,
                               "summary":"Auditoria previa fallida","conviction":"nula"},
                "audit":audit}

    name=asset["name"]; score=qdata["composite_score"]
    lv=qdata.get("levels",{}) or {}
    kf=qdata.get("kelly",0)

    ctx=(f"ACTIVO: {name} ({asset['type']}) | {DATE_ES} | {QUARTER}\n"
         f"Score: {score}/100 | tech:{qdata['technical_score']} fund:{qdata['fundamental_score']}\n"
         f"RSI14:{qdata.get('rsi','N/A')} | Vol:{qdata.get('volatility','N/A')}% | "
         
         f"Entrada:${lv.get('entry','N/A')} Stop:${lv.get('stop','N/A')} "
         f"(-{lv.get('stop_pct','N/A')}%) Target:${lv.get('target','N/A')} "
         f"(+{lv.get('target_pct','N/A')}%) R:R 2:1\n"
         f"Warnings: {'; '.join(audit.get('warnings',[])) or 'ninguno'}\n"
         f"MACRO: {macro_ctx}")

    print(f"     -> Kahneman paso 1/3 (tesis)...")
    t1=claude(f"""{ctx}

TESIS DE TRADING — formato exacto:

DATOS TECNICOS:
[RSI, MACD hist, posicion Bollinger, SMA cross — numeros reales]

DATOS FUNDAMENTALES:
[DXY, FED, yield curve, EUR/USD — numeros reales]

SINTESIS:
[2-3 frases: que dicen los datos combinados, sin opinion]

TESIS:
[1 frase: direccion, precio objetivo, condicion necesaria]

PROBABILIDAD INICIAL: p=X% +/-Y%
RAZON: [una frase]""",500)
    time.sleep(0.5)

    print(f"     -> Kahneman paso 2/3 (pre-mortem)...")
    t2=claude(f"""Tesis {name}: {t1}

PRE-MORTEM — exactamente 3 riesgos:

RIESGO 1: [nombre]
Probabilidad: X%
Mecanismo: [como invalida la tesis]
Señal de alerta: [dato concreto observable]

RIESGO 2: [nombre]
Probabilidad: X%
Mecanismo: [como invalida la tesis]
Señal de alerta: [dato concreto observable]

RIESGO 3: [nombre]
Probabilidad: X%
Mecanismo: [como invalida la tesis]
Señal de alerta: [dato concreto observable]

PROBABILIDAD AJUSTADA: p=X% +/-Y%""",400)
    time.sleep(0.5)

    print(f"     -> Kahneman paso 3/3 (calibracion JSON)...")
    t3=claude(f"""Tesis:{t1}
Pre-mortem:{t2}
Score:{score}/100 Audit:{audit.get('score',0)}/100

Responde SOLO con este JSON, sin texto adicional, sin markdown, sin backticks:
{{"signal":"COMPRAR|VENDER|ESPERAR","prob":65,"prob_interval":12,"horizon":"1-3 meses","invalidation":"condicion concreta","summary":"una frase dashboard","conviction":"alta|media|baja","data_quality":"{audit.get('score',0)}/100"}}""",220)
    time.sleep(0.5)

    cal = parse_cal(t3)
    print(f"     -> Calibracion: signal={cal.get('signal')} prob={cal.get('prob')} summary={cal.get('summary','')[:50]}")

    return {"thesis":t1 or "","premortem":t2 or "","calibration":cal,"audit":audit}

# ── MODULO 3: CORE RIGUROSO ───────────────────────────

def get_asset_reputation(asset_id, paper_data):
    """
    Calcula el track record de un activo basado en paper_trades.json.
    Returns: {wr, consecutive_losses, total_closed, silenced, silence_reason}
    
    Lógica Bayesiana: el umbral de señal se ajusta según el historial real.
    - WR < 35%: umbral +15 (mucho más difícil señalizar)
    - WR 35-45%: umbral +10
    - WR 45-55%: umbral +5  
    - WR > 65%: umbral -5 (ligera ventaja comprobada)
    - 3 stops consecutivos: silencio 14 días
    - 5+ stops consecutivos: silencio 30 días
    """
    import datetime
    trades = [t for t in paper_data.get('trades', [])
              if t.get('asset_id') == asset_id
              and t.get('status') in ('stopped', 'target_hit')
              and t.get('result') in ('win', 'loss')]

    total_closed = len(trades)
    if total_closed < 3:
        return {'wr': None, 'consecutive_losses': 0,
                'total_closed': total_closed, 'silenced': False,
                'silence_reason': None, 'threshold_adj': 0}

    # WR últimos 10 trades cerrados
    recent = sorted(trades, key=lambda x: x.get('exit_date', ''))[-10:]
    wins = sum(1 for t in recent if t['result'] == 'win')
    wr = wins / len(recent)

    # Stops consecutivos desde el más reciente
    sorted_all = sorted(trades, key=lambda x: x.get('exit_date', ''))
    consecutive_losses = 0
    for t in reversed(sorted_all):
        if t['result'] == 'loss':
            consecutive_losses += 1
        else:
            break

    # Período de silencio
    silenced = False
    silence_reason = None
    if consecutive_losses >= 3:
        last_trade = sorted_all[-1]
        last_exit = last_trade.get('exit_date', '')
        try:
            dt = datetime.datetime.strptime(last_exit[:16], '%d/%m/%Y %H:%M')
            days_since = (datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - dt).days
            silence_days = 30 if consecutive_losses >= 5 else 14
            if days_since < silence_days:
                silenced = True
                silence_reason = (f"{consecutive_losses} stops consecutivos. "
                                  f"Silencio {silence_days}d. "
                                  f"{silence_days - days_since}d restantes.")
        except Exception:
            pass

    # Ajuste de umbral basado en WR
    if wr < 0.35:   threshold_adj = +15
    elif wr < 0.45: threshold_adj = +10
    elif wr < 0.55: threshold_adj = +5
    elif wr > 0.65: threshold_adj = -5
    else:           threshold_adj = 0

    return {
        'wr': round(wr, 3),
        'consecutive_losses': consecutive_losses,
        'total_closed': total_closed,
        'recent_n': len(recent),
        'silenced': silenced,
        'silence_reason': silence_reason,
        'threshold_adj': threshold_adj
    }

def get_effective_threshold(base_threshold, reputation):
    """Umbral efectivo = base + ajuste por track record."""
    if reputation['wr'] is None:
        return base_threshold  # Sin historial, umbral base
    adj = reputation['threshold_adj']
    effective = base_threshold + adj
    # Límites: no bajar de 55 ni subir de 85
    return max(55, min(85, effective))

def format_reputation_log(asset_name, reputation, base_threshold, effective_threshold):
    """Log claro del estado autodidacta para el pipeline."""
    if reputation['wr'] is None:
        return f"     [{asset_name}] Sin historial — umbral base: {base_threshold}"
    wr_pct = round(reputation['wr'] * 100, 1)
    adj = reputation['threshold_adj']
    sign = '+' if adj > 0 else ''
    status = '🔴 SILENCIADO' if reputation['silenced'] else ('🟡' if wr_pct < 50 else '🟢')
    return (f"     [{asset_name}] {status} WR={wr_pct}% ({reputation['recent_n']} trades) | "
            f"Stops consec={reputation['consecutive_losses']} | "
            f"Umbral: {base_threshold}{sign}{adj}={effective_threshold}")

# ── ESTRATEGIA VALIDADA: MOMENTUM PULLBACK ────────────
# Comprar retrocesos (RSI 35-55) dentro de tendencia alcista fuerte
# (SMA50 > SMA200 * 1.03). Stop 1.5 ATR, target 3.0 ATR.
# Validada out-of-sample: expectancy +2.24%/trade, WR 57%, ~35 señales/año.
# Esta es la ÚNICA lógica de entrada del sistema. Reemplaza el score roto.

# ── ESTRATEGIA v2: TREND JOINED LONG ───────────────────────────────
# El gate, las rondas y el protocolo canónico viven en tjl_validation.py y
# en el estado persistente de paper_trades.json. No se duplican aquí para
# evitar que código y estado diverjan.

def trend_joined_long_trigger(highs):
    """Nivel de ruptura calculable ANTES de que empiece el día:
    máximo entre el high de ayer y el high de los 20 días previos."""
    if len(highs) < 22: return None
    return round(max(highs[-1], max(highs[-21:-1])), 4)

def trend_joined_long_signal(highs, lows, closes, current_price):
    """
    Señal de COMPRA si:
      1. El cierre de AYER está por encima de la SMA200 (tendencia de fondo)
      2. El precio actual ha superado el trigger (ruptura de máximos)
      3. El precio no se ha extendido demasiado (< trigger + 0.5 ATR) —
         evita perseguir un precio ya disparado; en la ruptura, no en el techo.
    Devuelve (señal_bool, trigger, atr) para construir niveles honestos.
    """
    if len(closes) < 210: return False, None, None
    s200 = sma(closes[:-1], 200) if len(closes) > 200 else None
    if not s200 or closes[-2] <= s200:      # cierre de ayer bajo SMA200
        return False, None, None
    trigger = trend_joined_long_trigger(highs[:-1])  # niveles hasta ayer
    if trigger is None: return False, None, None
    atr_v = atr_ohlc(highs[:-1], lows[:-1], closes[:-1])
    if not atr_v: return False, None, None
    if current_price <= trigger:            # no ha roto
        return False, trigger, atr_v
    if current_price > trigger + 0.5*atr_v: # ya se escapó — no perseguir
        return False, trigger, atr_v
    return True, trigger, atr_v

EXPECTED_TJL_ENGINE_HASH = "323fae029218"

def _tjl_engine_rule_sources():
    """Código que define de forma efectiva entradas, niveles y salidas TJL."""
    return [
        inspect.getsource(trend_joined_long_signal),
        inspect.getsource(trend_joined_long_trigger),
        inspect.getsource(trend_joined_long_levels),
        inspect.getsource(sma),
        inspect.getsource(atr_ohlc),
        inspect.getsource(tjl.resolve_tjl_exit),
        inspect.getsource(tjl.apply_exit),
    ]

def _tjl_engine_rules_hash():
    return tjl.compute_rules_spec_hash(_tjl_engine_rule_sources())

def _assert_tjl_integrity():
    tjl.assert_protocol_hash()
    return tjl.assert_rules_spec_hash(
        extra_sources=_tjl_engine_rule_sources(),
        expected=EXPECTED_TJL_ENGINE_HASH,
    )

def trend_joined_long_levels(entry_price, atr_v):
    """Niveles de la estrategia: stop 1.5 ATR, target 3.0 ATR desde la entrada real."""
    if not atr_v: return None
    return {
        "entry": round(entry_price, 4),
        "stop": round(entry_price - 1.5*atr_v, 4),
        "target": round(entry_price + 3.0*atr_v, 4),
        "stop_pct": round(1.5*atr_v/entry_price*100, 2),
        "target_pct": round(3.0*atr_v/entry_price*100, 2),
        "rr": 2.0,
    }

# ── DECISION CUANTITATIVA (sin API) ───────────────────
def quant_signal(qdata, effective_threshold):
    """Decide COMPRAR / ESPERAR con matemáticas puras: score + momentum + RSI."""
    comp = qdata.get("composite_score", 0)
    rsi_v = qdata.get("rsi")
    mom1 = qdata.get("momentum_1m")
    mom3 = qdata.get("momentum_3m")
    prob = int(max(40, min(85, comp)))
    if comp >= effective_threshold:
        mom_ok = (mom1 is None or mom1 > -5) and (mom3 is None or mom3 > -10)
        rsi_ok = (rsi_v is None or rsi_v < 75)
        if mom_ok and rsi_ok:
            return {"signal": "COMPRAR", "prob": prob, "prob_interval": 12,
                    "horizon": "1-3 meses",
                    "summary": f"Score {comp} sobre umbral {effective_threshold}, momentum confirma",
                    "conviction": "alta" if comp >= effective_threshold + 8 else "media",
                    "source": "quant"}
        return {"signal": "ESPERAR", "prob": 50, "prob_interval": 15,
                "summary": f"Score {comp} OK pero momentum/RSI no confirman (m1={mom1} rsi={rsi_v})",
                "conviction": "baja", "source": "quant"}
    return {"signal": "ESPERAR", "prob": 50, "prob_interval": 15,
            "summary": f"Score {comp} por debajo de umbral {effective_threshold}",
            "conviction": "baja", "source": "quant"}


# ── MODULO 1: TRADING ─────────────────────────────────

RETURNS_CACHE = {}  # closes recientes por activo (correlaciones del gestor de cartera)
OHLC_TODAY = {}     # última barra diaria por activo
OHLC_BARS = {}      # barras diarias recientes por activo, para no perder stops entre ejecuciones
VOL_FACTOR_CURRENT = {}  # factor de vol vigente (overlay TJL)

def _corr(a_id, b_id):
    """Correlación de retornos diarios 90d entre dos activos (0 si faltan datos)."""
    ca, cb = RETURNS_CACHE.get(a_id), RETURNS_CACHE.get(b_id)
    if not ca or not cb: return 0.0
    n = min(len(ca), len(cb))
    if n < 30: return 0.0
    ra = [(ca[i]/ca[i-1]-1) for i in range(len(ca)-n+1, len(ca))]
    rb = [(cb[i]/cb[i-1]-1) for i in range(len(cb)-n+1, len(cb))]
    ma, mb = sum(ra)/len(ra), sum(rb)/len(rb)
    cov = sum((x-ma)*(y-mb) for x,y in zip(ra,rb))
    va = sum((x-ma)**2 for x in ra); vb = sum((y-mb)**2 for y in rb)
    if va <= 0 or vb <= 0: return 0.0
    return cov / (va**0.5 * vb**0.5)

def run_trading_module(fx_rates,eia,macro_ctx):
    print("\n--- MODULO 1: TRADING ---------------------------")
    results=[]; gdp_ind=None  # GDP retirado (no aportaba al score de commodities)

    for asset in TRADING_ASSETS:
        print(f"   {asset['name']}...")
        log={"module":"trading","asset":asset["name"],
             "ticker":asset["id"],"timestamp":DATE_ES}

        ohlc = fetch_yahoo_ohlc(asset["id"])
        if not ohlc or len(ohlc["closes"])<50:
            log["status"]="skipped_no_data"
            results.append({"meta":asset,"quant":None,"analysis":{},"log":log})
            continue

        closes = ohlc["closes"]; highs = ohlc["highs"]; lows = ohlc["lows"]
        RETURNS_CACHE[asset["id"]] = closes[-91:]  # para correlaciones del gestor de cartera
        bars = []
        for ts, op, hi, lo, cl in zip(ohlc["times"], ohlc["opens"], highs, lows, closes):
            if op is None or hi is None or lo is None or cl is None:
                continue
            bar_date = datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
            bars.append({"date": bar_date, "open": op, "high": hi, "low": lo, "close": cl})
        OHLC_BARS[asset["id"]] = bars[-90:]
        if bars:
            OHLC_TODAY[asset["id"]] = bars[-1]
        rsi_v=rsi(closes); _,macd_h=macd(closes)
        bb_low,_,bb_h=bollinger(closes); atr_v=atr_calc(closes)
        vol=ann_vol(closes); mdd=max_dd(closes); var=var95(closes)
        shr=sharpe_ratio(closes)
        tech=tech_score(closes,rsi_v,macd_h,bb_low,bb_h)
        fund=fund_score(gdp_ind,fx_rates,eia,asset["type"])
        comp=comp_score(tech,fund)
        lv=entry_levels(closes,atr_v)
        kf = None  # Kelly ELIMINADO: composite_score no es una probabilidad calibrada

        qdata={"price":round(closes[-1],4),
               "momentum_1m":round((closes[-1]/closes[-21]-1)*100,2) if len(closes)>=21 else None,
               "momentum_3m":round((closes[-1]/closes[-63]-1)*100,2) if len(closes)>=63 else None,
               "rsi":rsi_v,"macd_histogram":macd_h,
               "sma20":round(sma(closes,20),4) if sma(closes,20) else None,
               "sma50":round(sma(closes,50),4) if sma(closes,50) else None,
               "sma200":round(sma(closes,200),4) if len(closes)>=200 and sma(closes,200) else None,
               "bollinger_low":bb_low,"bollinger_high":bb_h,
               "atr":atr_v,"volatility":vol,"sharpe":shr,
               "max_drawdown_6m":mdd,"var_95_daily":var,
               "technical_score":tech,"fundamental_score":fund,"composite_score":comp,
               "levels":lv,"kelly":kf}

        audit=auto_audit(asset["name"],closes,qdata)
        log["audit"]=audit

        # ── AUTODIDACTA: leer track record antes de señalizar ──
        paper_data_current = load_paper_trades()
        reputation = get_asset_reputation(asset["id"], paper_data_current)
        BASE_THRESHOLD = 65  # umbral base composite_score
        effective_threshold = get_effective_threshold(BASE_THRESHOLD, reputation)
        print(format_reputation_log(asset["name"], reputation, BASE_THRESHOLD, effective_threshold))

        analysis={"thesis":"","premortem":"","calibration":{},"audit":audit,
                  "reputation":reputation,"effective_threshold":effective_threshold}

        # Reputación solo descriptiva: TJL permanece pura y no adapta sus señales.
        analysis["reputation"]["used_for_decision"] = False

        # ── ESTRATEGIA v2: TREND JOINED LONG (ruptura en tendencia) ──
        # Comprar la ruptura de máximos en valores en tendencia (cierre>SMA200).
        # El autodidacta sigue activo: silencia activos con mal track record.
        current_price = closes[-1]
        has_signal, tjl_trigger, tjl_atr = trend_joined_long_signal(highs, lows, closes, current_price)

        # AUTODIDACTA DESACTIVADO sobre TJL durante la validación forward:
        # una estrategia que cambia su conducta según resultados NO está congelada,
        # y la reputación mezclaba pérdidas de la estrategia antigua. TJL corre PURA.
        veto_by_reputation = False

        if has_signal and not veto_by_reputation:
            lv = trend_joined_long_levels(current_price, tjl_atr)
            cal = {
                "signal": "COMPRAR",
                # Evidencia honesta en vez de "probabilidad" heurística:
                "evidence": {"hist_wr_pct": 44, "interval_95_pct": [36, 53],
                             "n_oos": 124, "quality": "provisional (backtest OOS)"},
                "prob": None, "prob_interval": None,
                "horizon": "días-semanas",
                "summary": f"Ruptura de máximos en tendencia (trigger {tjl_trigger}). Entry={lv['entry']}",
                "conviction": "alta", "source": "trend_joined_long",
                "strategy": "trend_joined_long_v2",
                "trigger": tjl_trigger,
            }
            qdata["levels"] = lv
        else:
            if veto_by_reputation:
                reason = "vetado por autodidacta (mal track record, ruptura no limpia)"
            elif tjl_trigger and current_price <= tjl_trigger:
                reason = f"sin ruptura (precio {round(current_price,2)} bajo trigger {tjl_trigger})"
            elif tjl_trigger:
                reason = "precio demasiado extendido sobre el trigger (no perseguir)"
            else:
                reason = "sin tendencia de fondo (cierre bajo SMA200) o datos insuficientes"
            cal = {
                "signal": "ESPERAR", "prob": 50, "prob_interval": 15,
                "summary": f"Sin señal: {reason}",
                "conviction": "baja", "source": "trend_joined_long",
            }
        analysis["calibration"] = cal
        log["claude_calls"] = 0

        # Kahneman OPCIONAL: solo capa narrativa si está activado
        if KAHNEMAN_ENABLED and ANTHROPIC_KEY and cal["signal"] == "COMPRAR":
            try:
                k = kahneman_trading(asset, qdata, macro_ctx, audit)
                analysis["thesis"] = k.get("thesis","")
                analysis["premortem"] = k.get("premortem","")
                log["claude_calls"] = 3 if audit["passed"] else 1
            except Exception as e:
                print(f"     -> Kahneman falló (ignorado): {e}")

        print(f"     -> SEÑAL: {cal['signal']} ({cal.get('summary','')[:50]})")

        log["scores"]={"tech":tech,"fund":fund,"composite":comp}
        log["signal"]=cal["signal"]
        log["audit_passed"]=audit["passed"]
        log["autodidacta"]={"wr":reputation["wr"],"consecutive_losses":reputation["consecutive_losses"],
                              "effective_threshold":effective_threshold,"silenced":False}
        log["status"]="complete"
        results.append({"meta":asset,"quant":qdata,"analysis":analysis,"log":log})
        time.sleep(1)

    print(f"   {len(results)} activos analizados")
    return results

# ── MODULO 2: GEOESTRATEGICO ──────────────────────────

# ── MODULO 4: BACKTESTING ─────────────────────────────

def run_backtest_module():
    print("\n--- MODULO 4: BACKTESTING -----------------------")
    if not IS_WEEKLY:
        print("   Omitido (solo lunes)"); return []
    results=[]
    for asset in TRADING_ASSETS:
        print(f"   Backtesting {asset['name']}...")
        raw=fetch_yahoo(asset["id"],days=1825)
        if not raw or len(raw)<500:
            print(f"   SKIP {asset['name']}"); continue
        bt=run_backtest_asset(asset["name"],raw)
        if bt:
            results.append(bt)
            print(f"   {asset['name']}: WR={bt['win_rate_pct']}% B&H={bt['bh_annual_pct']}%/año")
        time.sleep(0.5)
    print(f"   Backtesting commodities: {len(results)} activos")

    print(f"   Backtesting: {len(results)} activos")
    return results

# ══════════════════════════════════════════════════════
# MÓDULO 5: PAPER TRADING
# ══════════════════════════════════════════════════════

PAPER_FILE = os.path.join(os.path.dirname(__file__), "paper_trades.json")

def load_paper_trades():
    if not os.path.exists(PAPER_FILE):
        return {"trades": [], "stats": {}}
    return tjl.load_paper_trades(PAPER_FILE)

def save_paper_trades(data):
    tjl.save_paper_trades(PAPER_FILE, data)

def migrate_trade_strategies(paper_data):
    """Migración estructural (idempotente): etiqueta trades históricos sin campo
    'strategy'. TJL se reconoce por el summary de la estrategia; el resto es legacy.
    Tras esto, 'strategy' es campo obligatorio y ningún trade queda 'unknown'."""
    TJL_PREFIX = "Ruptura de máximos en tendencia"
    n_tjl = n_leg = 0
    for t in paper_data.get("trades", []):
        if t.get("strategy"):
            continue
        if (t.get("summary") or "").startswith(TJL_PREFIX):
            t["strategy"] = "trend_joined_long_v2"; n_tjl += 1
        else:
            t["strategy"] = "legacy_momentum_pullback"; n_leg += 1
    if n_tjl or n_leg:
        print(f"   Migración de estrategias: {n_tjl} TJL + {n_leg} legacy etiquetados")
    return paper_data

def _parse_trade_day(value):
    """Convierte fechas del libro a YYYY-MM-DD; devuelve None si no son válidas."""
    if not value:
        return None
    text = str(value).strip()
    for fmt in ("%d/%m/%Y %H:%M UTC", "%d/%m/%Y %H:%M", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(text[:len(datetime.datetime.now().strftime(fmt))], fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    try:
        return datetime.datetime.fromisoformat(text.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except Exception:
        return None


def _compute_paper_stats(paper_data, new_count=0):
    trades = paper_data.get("trades", [])
    closed = [t for t in trades if t.get("status") in ("stopped", "target_hit")]
    open_t = [t for t in trades if t.get("status") == "open"]
    wins = [t for t in closed if t.get("result") == "win"]
    losses = [t for t in closed if t.get("result") == "loss"]

    def avg(items, field):
        vals = [t.get(field) for t in items if isinstance(t.get(field), (int, float))]
        return round(statistics.mean(vals), 2) if vals else None

    win_rate = round(len(wins) / len(closed) * 100, 1) if closed else None
    avg_win = avg(wins, "pnl_pct")
    avg_loss = avg(losses, "pnl_pct")
    expectancy = round(statistics.mean(t["pnl_pct"] for t in closed
                                       if isinstance(t.get("pnl_pct"), (int, float))), 2) if closed else None

    tjl_closed = [t for t in closed if t.get("strategy") == tjl.STRATEGY_NAME]
    tjl_wins = [t for t in tjl_closed if t.get("result") == "win"]
    tjl_losses = [t for t in tjl_closed if t.get("result") == "loss"]
    for trade in tjl_closed:
        tjl.ensure_pnl_net_base(trade)
    tjl_net = [t["pnl_net_base"] for t in tjl_closed
               if isinstance(t.get("pnl_net_base"), (int, float))]
    tjl_gross = [t["pnl_pct"] for t in tjl_closed
                 if isinstance(t.get("pnl_pct"), (int, float))]

    paper_data["stats"] = {
        "total_signals": len(trades),
        "open": len(open_t),
        "closed": len(closed),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": win_rate,
        "avg_win_pct": avg_win,
        "avg_loss_pct": avg_loss,
        "expectancy_pct": expectancy,
        "new_this_run": new_count,
        "last_updated": DATE_ES,
        "strategy_breakdown": {
            tjl.STRATEGY_NAME: len(tjl_closed),
            "legacy_momentum_pullback": sum(
                1 for t in closed if t.get("strategy") == "legacy_momentum_pullback"),
            "unknown": sum(1 for t in closed if not t.get("strategy")),
        },
        "strategy_validated": {
            "name": "trend_joined_long_v2",
            "closed": len(tjl_closed),
            "wins": len(tjl_wins),
            "losses": len(tjl_losses),
            "win_rate_pct": round(len(tjl_wins) / len(tjl_closed) * 100, 1) if tjl_closed else None,
            "avg_win_pct": avg(tjl_wins, "pnl_pct"),
            "avg_loss_pct": avg(tjl_losses, "pnl_pct"),
            "expectancy_pct": round(statistics.mean(tjl_gross), 3) if tjl_gross else None,
            "expectancy_net_base_direct": round(statistics.mean(tjl_net), 4) if tjl_net else None,
            "expectancy_net_scenarios": ({
                "optimista_0.10": round(statistics.mean(tjl_gross) - 0.10, 3),
                "base_0.20": round(statistics.mean(tjl_gross) - 0.20, 3),
                "conservador_0.40": round(statistics.mean(tjl_gross) - 0.40, 3),
                "estres_0.75": round(statistics.mean(tjl_gross) - 0.75, 3),
            } if tjl_gross else None),
            "note": "Métricas directas de trades TJL; la decisión usa la muestra congelada por ronda.",
        },
    }
    return paper_data


def _build_trade_from_signal(a):
    cal = a["analysis"].get("calibration", {})
    q = a["quant"]
    lv = q.get("levels") or {}
    trade_id = f"{a['meta']['id']}_{DATE_ES[:10]}"
    return {
        "id": trade_id,
        "asset": a["meta"]["name"],
        "asset_id": a["meta"]["id"],
        "source": cal.get("source", "trend_joined_long"),
        "trigger": cal.get("trigger"),
        "signal": "COMPRAR",
        "entry_date": DATE_ES,
        "entry_bar_date": (OHLC_TODAY.get(a["meta"]["id"]) or {}).get("date"),
        "entry_price": lv["entry"],
        "stop_price": lv["stop"],
        "target_price": lv["target"],
        "stop_pct": lv.get("stop_pct"),
        "target_pct": lv.get("target_pct"),
        "rr": lv.get("rr", 2.0),
        "prob": None,
        "evidence": cal.get("evidence"),
        "conviction": cal.get("conviction", ""),
        "score": q.get("composite_score"),
        "vol_factor_at_entry": VOL_FACTOR_CURRENT.get("f"),
        "engine_rules_hash": EXPECTED_TJL_ENGINE_HASH,
        "current_price": lv["entry"],
        "current_pnl_pct": 0.0,
        "status": "open",
        "exit_price": None,
        "exit_date": None,
        "pnl_pct": None,
        "pnl_net_base": None,
        "result": None,
        "summary": cal.get("summary", ""),
        "entry_alert_sent": False,
    }


def update_paper_trades(trading_results, paper_data):
    """Actualiza cierres, aplica el gate persistente y registra entradas de ronda 2."""
    paper_data = migrate_trade_strategies(paper_data)
    paper_data = tjl.initialize_tjl_validation_state(paper_data)
    tv = paper_data["tjl_validation"]
    persisted_engine_hash = tv.get("engine_rules_hash")
    if persisted_engine_hash and persisted_engine_hash != EXPECTED_TJL_ENGINE_HASH:
        raise tjl.TJLIntegrityError(
            f"Hash del motor persistido {persisted_engine_hash} != {EXPECTED_TJL_ENGINE_HASH}")
    tv["engine_rules_hash"] = EXPECTED_TJL_ENGINE_HASH
    for rnd in tv.get("rounds", {}).values():
        rnd.setdefault("engine_rules_hash", EXPECTED_TJL_ENGINE_HASH)

    runtime = {
        "blocked_assets": [],
        "accepted_trade_ids": [],
        "rejected_signals": [],
        "closed_trade_ids": [],
    }
    trades = paper_data.get("trades", [])
    result_by_asset = {
        a["meta"]["id"]: a for a in trading_results
        if a.get("quant") and a.get("meta")
    }

    # Cierres: TJL usa todas las barras posteriores a la fecha de entrada.
    for trade in trades:
        if trade.get("status") != "open":
            continue
        asset_id = trade.get("asset_id")
        result = result_by_asset.get(asset_id)
        current_price = result.get("quant", {}).get("price") if result else None
        if current_price:
            trade["current_price"] = round(current_price, 4)
            if trade.get("signal") == "VENDER":
                trade["current_pnl_pct"] = round((trade["entry_price"] / current_price - 1) * 100, 2)
            else:
                trade["current_pnl_pct"] = round((current_price / trade["entry_price"] - 1) * 100, 2)

        if trade.get("strategy") == tjl.STRATEGY_NAME:
            entry_day = trade.get("entry_bar_date") or _parse_trade_day(trade.get("entry_date"))
            last_day = trade.get("last_evaluated_bar_date")
            for bar in OHLC_BARS.get(asset_id, []):
                bar_day = bar.get("date")
                if not bar_day or (entry_day and bar_day <= entry_day) or (last_day and bar_day <= last_day):
                    continue
                exit_info = tjl.resolve_tjl_exit(trade, bar)
                trade["last_evaluated_bar_date"] = bar_day
                if exit_info:
                    exit_dt = datetime.datetime.strptime(bar_day, "%Y-%m-%d").replace(
                        tzinfo=datetime.timezone.utc)
                    tjl.apply_exit(trade, exit_info, exit_dt)
                    trade["exit_bar_date"] = bar_day
                    runtime["closed_trade_ids"].append(trade["id"])
                    break
        elif current_price:
            # Compatibilidad con legacy: no influye en la validación TJL.
            if trade.get("signal") == "COMPRAR":
                if current_price <= trade["stop_price"]:
                    trade.update(status="stopped", exit_price=trade["stop_price"],
                                 exit_date=DATE_ES, result="loss",
                                 pnl_pct=round((trade["stop_price"] / trade["entry_price"] - 1) * 100, 2))
                elif current_price >= trade["target_price"]:
                    trade.update(status="target_hit", exit_price=trade["target_price"],
                                 exit_date=DATE_ES, result="win",
                                 pnl_pct=round((trade["target_price"] / trade["entry_price"] - 1) * 100, 2))

    open_trades = [t for t in trades if t.get("status") == "open"]
    open_asset_ids = {t.get("asset_id") for t in open_trades}
    existing_ids = {t.get("id") for t in trades}
    gate = tjl.get_effective_tjl_gate(paper_data)

    signals = []
    for a in trading_results:
        if not a.get("quant"):
            continue
        cal = a.get("analysis", {}).get("calibration", {})
        lv = a["quant"].get("levels") or {}
        if cal.get("signal") == "COMPRAR" and cal.get("strategy") == tjl.STRATEGY_NAME and lv.get("entry"):
            signals.append(a)

    if gate["status"] != "open":
        runtime["blocked_assets"] = sorted(a["meta"]["name"] for a in signals)
    else:
        MAX_OPEN = 8
        MAX_PER_SECTOR = 2
        sector_of = {x["id"]: x.get("type", "?") for x in TRADING_ASSETS}
        sector_counts = {}
        for trade in open_trades:
            sector = sector_of.get(trade.get("asset_id"), "?")
            sector_counts[sector] = sector_counts.get(sector, 0) + 1

        candidates = []
        for a in signals:
            asset_id = a["meta"]["id"]
            trade_id = f"{asset_id}_{DATE_ES[:10]}"
            if asset_id in open_asset_ids or trade_id in existing_ids:
                runtime["rejected_signals"].append({"asset": a["meta"]["name"], "reason": "posición o ID existente"})
                continue
            correlations = [abs(_corr(asset_id, t.get("asset_id"))) for t in open_trades]
            avg_corr = sum(correlations) / len(correlations) if correlations else 0.0
            candidates.append((avg_corr, a))
        candidates.sort(key=lambda item: item[0])

        slots = max(0, MAX_OPEN - len(open_trades))
        selected = []
        for _, a in candidates:
            if len(selected) >= slots:
                runtime["rejected_signals"].append({"asset": a["meta"]["name"], "reason": "máximo 8 abiertas"})
                continue
            sector = sector_of.get(a["meta"]["id"], "?")
            if sector_counts.get(sector, 0) >= MAX_PER_SECTOR:
                runtime["rejected_signals"].append({"asset": a["meta"]["name"], "reason": f"límite sector {sector}"})
                continue
            selected.append(a)
            sector_counts[sector] = sector_counts.get(sector, 0) + 1

        for a in selected:
            trade = _build_trade_from_signal(a)
            outcome = tjl.register_new_tjl_trade(paper_data, trade)
            if outcome["accepted"]:
                paper_data["trades"][-1]["engine_rules_hash"] = EXPECTED_TJL_ENGINE_HASH
                runtime["accepted_trade_ids"].append(trade["id"])
                existing_ids.add(trade["id"])
            else:
                runtime["rejected_signals"].append({"asset": a["meta"]["name"], "reason": outcome["reason"]})

    _compute_paper_stats(paper_data, len(runtime["accepted_trade_ids"]))
    tjl.verify_tjl_validation_state(paper_data)
    return paper_data, runtime


# ── MODULO DE RECUPERACIÓN DE DÍAS PERDIDOS ──────────────────────────
# Reconstruye cronológicamente las señales que el sistema HABRÍA dado
# durante el periodo en que estuvo caído, usando precios reales históricos.
# Simula stop/target con el movimiento real posterior. NO inventa resultados.
# Se ejecuta UNA vez (controlado por flag RECOVER_LOST_DAYS).

def recover_lost_days(paper_data, days_back=30):
    print("   recover_lost_days DESACTIVADO: las reconstrucciones retrospectivas")
    print("   contaminan la validación forward (miran resultados ya conocidos).")
    return paper_data
    # ── código original inerte debajo ──
    """
    Reconstrucción cronológica honesta del aprendizaje perdido.
    Para cada día del periodo:
      1. Reconstruye indicadores con precios hasta ese día (sin mirar el futuro)
      2. Calcula umbral efectivo con el historial acumulado HASTA ese día
      3. Genera señal con quant_signal
      4. Simula resultado con el movimiento real posterior (stop/target por ATR)
    Mantiene la evolución secuencial del autodidacta.
    """
    from datetime import datetime, timedelta
    print(f"\n=== RECUPERACIÓN DE {days_back} DÍAS PERDIDOS (reconstrucción cronológica) ===")

    existing_ids = {t["id"] for t in paper_data.get("trades", [])}
    recovered = 0
    cutoff = NOW - timedelta(days=days_back)

    for asset in TRADING_ASSETS:
        prices_raw = fetch_yahoo(asset["id"], days=730)
        if not prices_raw or len(prices_raw) < 250:
            continue

        # prices_raw: lista de (date, close). Localizar el tramo del periodo perdido.
        dated = []
        for d, c in prices_raw:
            try:
                dd = datetime.strptime(d[:10], "%Y-%m-%d") if isinstance(d, str) else d
                dated.append((dd, c))
            except Exception:
                continue
        if len(dated) < 250:
            continue

        # Índices del periodo a recuperar (días dentro de la ventana)
        for i in range(len(dated)):
            day_date, _ = dated[i]
            if day_date.tzinfo is not None:
                day_date = day_date.replace(tzinfo=None)
            if day_date < cutoff.replace(tzinfo=None):
                continue
            if i < 200:  # necesita histórico para indicadores
                continue

            # Solo evaluar señales cada ~3 días (como haría el sistema, no cada día)
            if (i - 200) % 3 != 0:
                continue

            seg = [c for _, c in dated[:i+1]]   # precios HASTA ese día (sin futuro)
            if len(seg) < 200:
                continue

            # Reconstruir indicadores
            rsi_v = rsi(seg); _, macd_h = macd(seg)
            bb_low, _, bb_h = bollinger(seg); atr_v = atr_calc(seg)
            tech = tech_score(seg, rsi_v, macd_h, bb_low, bb_h)
            fund = fund_score(None, {}, None, asset["type"])  # sin macro histórico, neutro
            comp = comp_score(tech, fund)

            # Estrategia validada (misma que en vivo): momentum pullback
            rep = get_asset_reputation(asset["id"], paper_data)
            if rep.get("silenced"):
                continue
            # Estrategia v2 (aprox. con cierres — la recuperación es solo orientativa)
            s200_r = sma(seg[:-1], 200) if len(seg) > 200 else None
            if not s200_r or seg[-2] <= s200_r:
                continue
            trig_r = max(seg[-21:-1])  # proxy de trigger con cierres
            if seg[-1] <= trig_r:
                continue
            lv = trend_joined_long_levels(seg[-1], atr_v)
            if not lv:
                continue
            entry = lv["entry"]; stop = lv["stop"]; target = lv["target"]

            tid = f"{asset['id']}_{day_date.strftime('%Y-%m-%d')}_REC"
            if tid in existing_ids:
                continue

            # Simular con el MOVIMIENTO REAL posterior (los días que vinieron después)
            future = [c for _, c in dated[i+1:]]
            result = None; exit_price = None; exit_idx = None
            for j, fp in enumerate(future):
                if fp <= stop:
                    result = "loss"; exit_price = stop; exit_idx = j; break
                if fp >= target:
                    result = "win"; exit_price = target; exit_idx = j; break
            # Si no tocó ni stop ni target, sigue abierto -> no lo cerramos (honesto)
            if result is None:
                continue

            pnl = round((exit_price/entry-1)*100, 2)
            exit_date = dated[i+1+exit_idx][0]
            trade = {
                "id": tid, "asset": asset["name"], "asset_id": asset["id"],
                "signal": "COMPRAR",
                "entry_date": day_date.strftime("%d/%m/%Y %H:%M"),
                "entry_price": round(entry,4), "stop_price": stop, "target_price": target,
                "prob": cal["prob"], "score": comp,
                "status": "stopped" if result=="loss" else "target_hit",
                "exit_price": exit_price,
                "exit_date": exit_date.strftime("%d/%m/%Y %H:%M") if hasattr(exit_date,'strftime') else str(exit_date),
                "pnl_pct": pnl, "result": result,
                "summary": f"RECUPERADO: señal reconstruida del {day_date.strftime('%d/%m')}",
                "recovered": True,
            }
            paper_data["trades"].append(trade)
            existing_ids.add(tid)
            recovered += 1
            print(f"   {asset['name']} {day_date.strftime('%d/%m')}: COMPRAR @ {entry:.2f} -> {result} ({pnl:+.1f}%)")

    print(f"=== {recovered} señales recuperadas e integradas en el aprendizaje ===")
    return paper_data

# ═══ SHADOW BOOKS — investigación paralela SIN tocar TJL (congelada) ═══
# Reglas PREDECLARADAS (2026-07-20), contabilidad separada, coste 0.20% ida+vuelta.
SHADOW_TREND_UNIVERSE = ["SPY","QQQ","EFA","EEM","TLT","IEF","GLD","DBC","XLE","UUP"]
SHADOW_COST_RT = 0.20  # % ida+vuelta

def run_shadow_books(paper_data):
    """Trend multiactivo (SMA200 + momentum 126d, pesos 1/vol), benchmarks
    (SPY, IWDA) y overlay de volatilidad. Todo virtual, todo separado."""
    sh = paper_data.setdefault("shadow", {})
    trend = sh.setdefault("multi_asset_trend", {"positions": {}, "closed": [], "equity": 100.0,
                                                 "rules": "long si close>SMA200 y ret126d>0; peso 1/vol20; coste 0.20% rt"})
    bench = sh.setdefault("benchmarks", {})
    today = DATE_ES[:10]

    # ── Libro A: trend multiactivo — CAPITAL VIRTUAL con unidades y pesos 1/vol ──
    trend.setdefault("capital", 100000.0)   # capital virtual inicial
    trend.setdefault("cash", trend["capital"])
    data = {}
    vols = {}
    for etf in SHADOW_TREND_UNIVERSE:
        o = fetch_yahoo_ohlc(etf, days=400)
        if o and len(o["closes"]) > 210:
            c = o["closes"]; data[etf] = c
            rets = [(c[i]/c[i-1]-1) for i in range(-20, 0)]
            mu = sum(rets)/len(rets)
            vols[etf] = max(1e-6, (sum((r-mu)**2 for r in rets)/len(rets))**0.5)
        time.sleep(0.2)

    # señales deseadas hoy
    want = {}
    for etf, closes in data.items():
        s200 = sma(closes[:-1], 200)
        ret126 = closes[-1]/closes[-127] - 1 if len(closes) > 127 else None
        want[etf] = bool(s200 and closes[-2] > s200 and ret126 is not None and ret126 > 0)

    # salidas primero (liberan cash) — coste aplicado a la salida
    for etf in list(trend["positions"].keys()):
        pos = trend["positions"][etf]
        if etf in data and not want.get(etf, False):
            px = data[etf][-1]
            proceeds = pos["units"] * px * (1 - SHADOW_COST_RT/100)
            pnl = (px/pos["entry"] - 1)*100 - SHADOW_COST_RT
            trend["cash"] += proceeds
            trend["closed"].append({"etf": etf, "entry": pos["entry"], "exit": px,
                                    "units": pos["units"], "pnl_pct": round(pnl, 2),
                                    "opened": pos["date"], "closed": today})
            del trend["positions"][etf]
            print(f"   [shadow trend] EXIT {etf} {pnl:+.1f}%")

    # entradas: presupuesto = cash disponible, pesos 1/vol normalizados entre las NUEVAS
    new_longs = [e for e in want if want[e] and e not in trend["positions"] and e in vols]
    if new_longs and trend["cash"] > 1000:
        inv = {e: 1.0/vols[e] for e in new_longs}
        s = sum(inv.values())
        budget = trend["cash"] * 0.98
        for e in new_longs:
            alloc = budget * inv[e]/s
            px = data[e][-1]
            units = alloc / px
            trend["cash"] -= alloc
            trend["positions"][e] = {"entry": px, "units": round(units, 4),
                                     "weight_at_entry": round(inv[e]/s, 3), "date": today}
            print(f"   [shadow trend] LONG {e} @ {round(px,2)} (peso {inv[e]/s:.0%}, {round(units,2)} uds)")

    # equity = cash + valor de mercado de posiciones (interpretable económicamente)
    mv = sum(pos["units"]*data.get(etf, [pos["entry"]])[-1] for etf, pos in trend["positions"].items())
    trend["equity"] = round(trend["cash"] + mv, 2)
    trend["return_pct"] = round((trend["equity"]/trend["capital"] - 1)*100, 2)
    trend["n_open"] = len(trend["positions"]); trend["n_closed"] = len(trend["closed"])

    # ── Libro B: benchmarks virtuales ──
    for tick, label in [("SPY", "spy"), ("IWDA.AS", "iwda")]:
        o = fetch_yahoo_ohlc(tick, days=30)
        time.sleep(0.2)
        if not o: continue
        px = o["closes"][-1]
        b = bench.setdefault(label, {"start_price": px, "start_date": today})
        b["last_price"] = px
        b["return_pct"] = round((px/b["start_price"] - 1)*100, 2)

    # ── Libro C: overlay de volatilidad sobre TJL (bookkeeping) ──
    spy = data.get("SPY") or []
    if len(spy) > 21:
        rets = [(spy[i]/spy[i-1]-1) for i in range(-20, 0)]
        mu = sum(rets)/len(rets)
        vol20 = (sum((r-mu)**2 for r in rets)/len(rets))**0.5 * (252**0.5) * 100
        target = 15.0  # vol objetivo anualizada %
        factor = round(min(1.5, max(0.25, target/max(vol20, 1e-6))), 2)
        vm = sh.setdefault("vol_managed_tjl", {"note": "pnl TJL x factor vol grabado A LA ENTRADA", "applied": [], "cum_pnl_pct": 0.0})
        vm["current_factor"] = factor
        vm["current_spy_vol"] = round(vol20, 1)
        VOL_FACTOR_CURRENT["f"] = factor
        # aplicar a cierres TJL nuevos (una sola vez por trade, con el factor DE SU ENTRADA)
        done = {a.get("trade_id") for a in vm["applied"]}
        for t in paper_data.get("trades", []):
            if (t.get("strategy") == "trend_joined_long_v2"
                    and t.get("status") in ("stopped", "target_hit")
                    and t.get("id") not in done):
                f_e = t.get("vol_factor_at_entry") or 1.0
                scaled = round((t.get("pnl_pct") or 0) * f_e, 2)
                vm["applied"].append({"trade_id": t.get("id"), "factor": f_e,
                                      "pnl_raw": t.get("pnl_pct"), "pnl_scaled": scaled})
                vm["cum_pnl_pct"] = round(vm.get("cum_pnl_pct", 0) + scaled, 2)
    return paper_data

def run_paper_trading_module(trading_results):
    print("\n--- MODULO 5: PAPER TRADING ---------------------")
    paper_data = load_paper_trades()
    paper_data, runtime = update_paper_trades(trading_results, paper_data)
    save_paper_trades(paper_data)
    stats = paper_data["stats"]
    print(f"   Señales: {stats['total_signals']} total | {stats['open']} abiertas | "
          f"{stats['closed']} cerradas | WR: {stats['win_rate_pct']}% | "
          f"Nuevas: {stats['new_this_run']}")
    return paper_data, runtime

# ══════════════════════════════════════════════════════
# MÓDULO 6: INTELIGENCIA GEOPOLÍTICA EN TIEMPO REAL
# ══════════════════════════════════════════════════════
#
# FUNDAMENTO ACADÉMICO:
# - Caldara & Iacoviello (2022, AER): GPR index — eventos geopolíticos
#   adversos tienen impacto estadísticamente significativo en commodities
# - IMF GFSR (2025): escaladas militares y despliegues tienen mayor impacto
#   que amenazas verbales (hallazgo contraintuitivo clave)
# - Evidencia Russia-Ucrania: gas natural +7.5%/evento, trigo +16% en 9 semanas
# - Hallazgo crítico: mercados sobrerreaccionan al shock inicial y revierten
#   → el módulo informa, NO señaliza compra/venta automática
#
# DISEÑO KAHNEMAN:
# - Umbral alto de activación (evita sesgo de acción por ruido)
# - Separación explícita información/señal
# - Contexto histórico obligatorio en cada alerta
# - Circuit breaker: máximo 2 alertas alta importancia por ciclo
# - Régimen de mercado como condicionante (no alertar igual en calma que en crisis)

# ── MAPA EVENTO → ACTIVOS (base académica Caldara-Iacoviello + IMF 2025) ──────
#
# Estructura: keyword_groups → {assets, direction, channel, magnitude_hist, duration_hist}
# channel: supply_disruption | uncertainty | safe_haven | trade_diversion

# ── VOLATILIDAD ACTUAL DEL ACTIVO (condicionante de régimen) ──────────────────
def get_asset_current_vol(asset_id, trading_results):
    """Obtiene volatilidad actual del activo desde los resultados del módulo trading."""
    for a in trading_results:
        if a["meta"]["id"] == asset_id and a.get("quant"):
            return a["quant"].get("volatility")
    return None

# ── FETCH GDELT DOC API (gratuito, sin key) ───────────────────────────────────
def run():
    print(f"\n{'='*58}\nGeoMacro Intel — Trading Engine — {DATE_ES}\n{'='*58}")
    print(f"Kahneman (IA): {'ON' if KAHNEMAN_ENABLED else 'OFF'} | Backtest semanal: {'SI' if IS_WEEKLY else 'NO'}\n")

    LIVE_MODE = os.environ.get("LIVE_MODE", "false").lower() == "true"
    results = {
        "generated_at": DATE_ES, "version": "9.0-trading",
        "is_weekly": IS_WEEKLY, "live_mode": LIVE_MODE,
        "trading": [], "backtesting": [], "macro": {},
        "ranking": [], "alerts": [], "audit_log": [],
    }

    # Integridad y migración persistente ANTES de cualquier llamada de red.
    try:
        engine_hash = _assert_tjl_integrity()
        initial_paper = load_paper_trades()
        initial_paper = migrate_trade_strategies(initial_paper)
        initial_paper = tjl.initialize_tjl_validation_state(initial_paper)
        persisted = initial_paper["tjl_validation"].get("engine_rules_hash")
        if persisted and persisted != engine_hash:
            raise tjl.TJLIntegrityError(
                f"Hash de motor persistido {persisted} != hash actual {engine_hash}")
        initial_paper["tjl_validation"]["engine_rules_hash"] = engine_hash
        for rnd in initial_paper["tjl_validation"].get("rounds", {}).values():
            rnd.setdefault("engine_rules_hash", engine_hash)
        save_paper_trades(initial_paper)
    except Exception as exc:
        message = f"🔴 INTEGRIDAD TJL: {type(exc).__name__}: {exc}"
        print(message)
        send_telegram(message, "critical")
        raise SystemExit(1)

    results["strategy_freeze"] = {
        "frozen": True,
        "version": tjl.STRATEGY_NAME,
        "engine_rules_hash": engine_hash,
        "rules_spec_hash": tjl.EXPECTED_TJL_SPEC_HASH,
        "protocol_hash": tjl.EXPECTED_PROTOCOL_HASH,
    }

    print("-> Datos macro globales...")
    fx_rates = fetch_fx(); eia = fetch_eia(); fred_d = {}
    if FRED_KEY:
        for sid, lbl in [("FEDFUNDS", "fed_funds_rate"),
                         ("T10Y2Y", "yield_curve_spread"),
                         ("DTWEXM", "dxy_index")]:
            obs = fetch_fred(sid)
            if obs:
                fred_d[lbl] = round(obs[0][1], 3); print(f"   FRED {lbl}: {obs[0][1]:.3f}")
        cpi_obs = fetch_fred("CPIAUCSL")
        if cpi_obs and len(cpi_obs) >= 13:
            cpi_yoy = round((cpi_obs[0][1]/cpi_obs[12][1]-1)*100, 2)
            fred_d["cpi_yoy"] = cpi_yoy; print(f"   FRED cpi_yoy: {cpi_yoy}%")

    # Salud FRED explícita y auditable (las 4 series obligatorias)
    REQUIRED_FRED = ["fed_funds_rate", "yield_curve_spread", "dxy_index", "cpi_yoy"]
    missing_fred = sorted(set(REQUIRED_FRED) - set(fred_d.keys()))
    results["fred_health"] = {
        "status": "OK" if not missing_fred else "ERROR",
        "last_attempt": DATE_ES,
        "missing_series": missing_fred,
        "note": "" if FRED_KEY else "FRED_API_KEY no configurada",
    }
    # geo_intelligence: módulo RETIRADO deliberadamente (jun 2026).
    results["geo_intelligence"] = {
        "active": False, "status": "REMOVED",
        "note": "Módulo retirado deliberadamente: fuentes gratuitas llegaban tarde y sin alfa. No es un fallo.",
    }
    results["macro"] = {"fx_rates": fx_rates, "fred": fred_d, "eia_crude": eia, "timestamp": DATE_ES}
    macro_ctx = (f"FED:{fred_d.get('fed_funds_rate','N/A')}% "
                 f"CPI YoY:{fred_d.get('cpi_yoy','N/A')}% "
                 f"Yield10Y-2Y:{fred_d.get('yield_curve_spread','N/A')}bps "
                 f"DXY:{fred_d.get('dxy_index','N/A')}")

    if KAHNEMAN_ENABLED and ANTHROPIC_KEY:
        print("-> Test API Anthropic...")
        t = claude("Di solo: OK", max_tokens=10)
        print(f"   API {'OK' if t else 'FALLO'}")

    # Factor de vol vigente ANTES de abrir trades (se graba en cada entrada)
    try:
        _spy = fetch_yahoo_ohlc("SPY", days=40)
        if _spy and len(_spy["closes"]) > 21:
            _c = _spy["closes"]
            _r = [(_c[i]/_c[i-1]-1) for i in range(-20, 0)]
            _m = sum(_r)/len(_r)
            _v = (sum((x-_m)**2 for x in _r)/len(_r))**0.5 * (252**0.5) * 100
            VOL_FACTOR_CURRENT["f"] = round(min(1.5, max(0.25, 15.0/max(_v, 1e-6))), 2)
    except Exception:
        pass

    trading_results = run_trading_module(fx_rates, eia, macro_ctx)
    results["trading"] = trading_results

    # ── RECUPERACIÓN DE DÍAS PERDIDOS (una sola vez, con flag) ──
    if os.environ.get("RECOVER_LOST_DAYS", "false").lower() == "true":
        pdata = load_paper_trades()
        pdata = recover_lost_days(pdata, days_back=int(os.environ.get("RECOVER_DAYS", "30")))
        save_paper_trades(pdata)
        print("   Recuperación completada. Quita RECOVER_LOST_DAYS para no repetir.")

    paper_data, tjl_runtime = run_paper_trading_module(trading_results)
    paper_data, verdict_events = tjl.evaluate_tjl_verdict(paper_data)
    save_paper_trades(paper_data)  # estado crítico persistido antes de módulos frágiles
    paper_data = send_tjl_notifications(paper_data, tjl_runtime, verdict_events)

    print("   Shadow books (investigación paralela)...")
    shadow_err = None
    try:
        paper_data = run_shadow_books(paper_data)
        save_paper_trades(paper_data)
    except Exception as e:
        shadow_err = str(e)[:200]
        print(f"   ERROR shadow books: {shadow_err}")
    results["shadow_health"] = {"status": "OK" if not shadow_err else "ERROR",
                                "error": shadow_err, "last_attempt": DATE_ES}
    results["paper_trading"] = paper_data
    results.update(tjl.build_results_snapshot(paper_data))
    results["tjl_verdict_protocol"] = tjl.TJL_VERDICT_PROTOCOL

    # ── INVARIANTES FUNCIONALES (coherencia de resultados, no solo disponibilidad) ──
    integrity_problems = []
    try:
        st = paper_data.get("stats", {})
        sv = st.get("strategy_validated", {})
        bd = st.get("strategy_breakdown", {})
        trades_all = paper_data.get("trades", [])
        TJL_PREFIX = "Ruptura de máximos en tendencia"
        tjl_closed_by_summary = sum(1 for t in trades_all
                                    if t.get("status") not in ("open", None)
                                    and (t.get("summary") or "").startswith(TJL_PREFIX))
        # 1. Si hay cierres TJL reales pero las métricas dicen 0 -> las métricas mienten
        if tjl_closed_by_summary > 0 and (sv.get("closed") or 0) == 0:
            integrity_problems.append("TJL: existen cierres reales pero strategy_validated=0 (métricas rotas)")
        # 2. Cerradas = ganadas + perdidas
        if (st.get("closed") or 0) != (st.get("wins") or 0) + (st.get("losses") or 0):
            integrity_problems.append("Incoherencia: closed != wins + losses")
        # 3. Ningún trade sin estrategia etiquetada
        if (bd.get("unknown") or 0) > 0:
            integrity_problems.append(f"unknown_strategy = {bd.get('unknown')} (debe ser 0)")
        # 4. Series FRED obligatorias
        mf = results.get("fred_health", {}).get("missing_series", [])
        if mf:
            integrity_problems.append(f"FRED series ausentes: {', '.join(mf)}")
        if results.get("shadow_health", {}).get("status") == "ERROR":
            integrity_problems.append(f"Shadow books ERROR: {results['shadow_health'].get('error')}")
    except Exception as e:
        integrity_problems.append(f"Invariantes: excepción {e}")

    results["integrity"] = {"ok": not integrity_problems, "problems": integrity_problems}
    if integrity_problems:
        send_telegram("🔴 INTEGRIDAD DEL SISTEMA\n" + "\n".join(f"• {p}" for p in integrity_problems)
                      + "\nEl sistema detectó esto solo.", "critical")
        for p in integrity_problems:
            print(f"   ⚠️ INTEGRIDAD: {p}")

    backtest_results = run_backtest_module()
    results["backtesting"] = backtest_results

    all_items = []
    for a in trading_results:
        if not a.get("quant"): continue
        cal = a["analysis"].get("calibration", {})
        all_items.append({
            "name": a["meta"]["name"], "type": a["meta"]["type"], "category": "commodity",
            "score": a["quant"]["composite_score"], "signal": cal.get("signal", "—"),
            "prob": cal.get("prob"), "prob_interval": cal.get("prob_interval"),
            "kelly": a["quant"].get("kelly"), "vol": a["quant"].get("volatility"),
            "sharpe": a["quant"].get("sharpe"), "price": a["quant"].get("price"),
            "summary": cal.get("summary", ""), "conviction": cal.get("conviction", ""),
            "audit_passed": a["log"].get("audit_passed", True)})
    all_items.sort(key=lambda x: x["score"], reverse=True)
    results["ranking"] = [{"rank": i+1, **item} for i, item in enumerate(all_items)]

    for a in trading_results:
        q = a.get("quant", {}) or {}; cal = a["analysis"].get("calibration", {}); nm = a["meta"]["name"]
        if q.get("rsi") and q["rsi"] < 30:
            results["alerts"].append({"type": "RSI_OVERSOLD", "module": "trading", "asset": nm,
                "value": q["rsi"], "message": f"{nm}: RSI={q['rsi']} sobreventa extrema", "severity": "high"})
        if q.get("composite_score", 0) >= 70 and cal.get("signal") == "COMPRAR":
            gate = tjl.get_effective_tjl_gate(paper_data)
            if cal.get("strategy") == tjl.STRATEGY_NAME and gate["status"] != "open":
                blocked = tjl.build_blocked_alert_entry(paper_data, a["meta"]["id"])
                blocked.update({
                    "module": "trading",
                    "value": q["composite_score"],
                    "message": f"{nm}: señal TJL bloqueada — NO COMPRAR",
                    "severity": "high",
                })
                results["alerts"].append(blocked)
            else:
                results["alerts"].append({"type": "BUY_SIGNAL", "module": "trading", "asset": nm,
                    "value": q["composite_score"],
                    "message": f"{nm}: score {q['composite_score']}/100 COMPRAR",
                    "severity": "high", "actionable": True})
        if q.get("max_drawdown_6m", 0) > 25:
            results["alerts"].append({"type": "HIGH_DRAWDOWN", "module": "trading", "asset": nm,
                "value": q["max_drawdown_6m"],
                "message": f"{nm}: drawdown 6M={q['max_drawdown_6m']}%", "severity": "medium"})

    results["audit_log"].extend([a["log"] for a in trading_results if a.get("log")])
    total_claude = sum(l.get("claude_calls", 0) for l in results["audit_log"])
    audits_ok = sum(1 for a in trading_results if a.get("log", {}).get("audit_passed", True))
    print(f"\nTrading:{len(trading_results)} ({audits_ok} OK) | "
          f"Backtest:{len(backtest_results)} | Alertas:{len(results['alerts'])} | Claude:{total_claude}")

    out = os.path.join(os.path.dirname(__file__), "results.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"Guardado: {out}")

    print("\n-> Alertas Telegram...")
    build_telegram_summary(results)
    print(f"Pipeline trading completado — {DATE_ES}\n")

def run_self_tests():
    """Ejecuta las pruebas aisladas sin red ni Telegram real."""
    import subprocess
    import sys

    base = os.path.dirname(__file__)
    test_files = [
        os.path.join(base, "test_tjl_validation.py"),
        os.path.join(base, "test_analysis_engine_integration.py"),
    ]
    missing = [path for path in test_files if not os.path.exists(path)]
    if missing:
        raise RuntimeError("Faltan archivos de test: " + ", ".join(missing))
    env = os.environ.copy()
    for secret in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "FRED_API_KEY",
                   "NEWS_API_KEY", "ANTHROPIC_API_KEY"):
        env.pop(secret, None)
    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", *test_files],
        cwd=base, env=env, check=False)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)
    print("SELF-TESTS TJL: PASS")

if __name__=="__main__":
    if os.environ.get("TJL_SELF_TEST") == "1":
        run_self_tests()
        raise SystemExit(0)
    run()
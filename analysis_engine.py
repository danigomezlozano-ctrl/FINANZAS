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

import os, json, math, time, datetime, statistics, urllib.request, urllib.parse
from urllib.request import urlopen, Request
try:
    import anthropic as anthropic_sdk
    ANTHROPIC_SDK = True
except ImportError:
    ANTHROPIC_SDK = False

ANTHROPIC_KEY    = os.environ.get("ANTHROPIC_API_KEY", "")
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

NOW      = datetime.datetime.utcnow()
TODAY    = NOW.strftime("%Y-%m-%d")
QUARTER  = f"Q{math.ceil(NOW.month/3)} {NOW.year}"
DATE_ES  = NOW.strftime("%d/%m/%Y %H:%M UTC")
IS_QUARTERLY = (NOW.weekday() == 0 and NOW.day <= 7 and NOW.month in [1,4,7,10])
IS_WEEKLY    = (NOW.weekday() == 0)
if os.environ.get("FORCE_CORE_REVIEW"): IS_QUARTERLY = True
if os.environ.get("FORCE_BACKTEST"):    IS_WEEKLY    = True

# ── MODELO CLAUDE ACTUALIZADO ─────────────────────────
CLAUDE_MODEL = "claude-sonnet-4-6"  # modelo actual abril 2026

TRADING_ASSETS = [
    {"id":"GC=F",  "name":"Oro",        "type":"precious",   "unit":"USD/oz"},
    {"id":"SI=F",  "name":"Plata",       "type":"precious",   "unit":"USD/oz"},
    {"id":"CL=F",  "name":"Crudo WTI",   "type":"energy",     "unit":"USD/bbl"},
    {"id":"NG=F",  "name":"Gas Natural", "type":"energy",     "unit":"USD/MMBtu"},
    {"id":"HG=F",  "name":"Cobre",       "type":"industrial", "unit":"USD/lb"},
    {"id":"ALI=F", "name":"Aluminio",    "type":"industrial", "unit":"USD/t"},
    {"id":"LIT",   "name":"ETF Litio",   "type":"critical",   "unit":"USD/share"},
    {"id":"COPX",  "name":"ETF Cobre",   "type":"industrial", "unit":"USD/share"},
]

REGIONS = [
    {"id":"VNM","name":"Vietnam",    "region":"SE Asia",    "currency":"VND","sector":"manufactura tech",    "etf":"VNM"},
    {"id":"IND","name":"India",      "region":"South Asia", "currency":"INR","sector":"servicios digitales", "etf":"INDA"},
    {"id":"POL","name":"Polonia",    "region":"CEE Europa", "currency":"PLN","sector":"logistica UE hub",    "etf":"EPOL"},
    {"id":"BRA","name":"Brasil",     "region":"LATAM",      "currency":"BRL","sector":"agro recursos",       "etf":"EWZ"},
    {"id":"SAU","name":"Arabia S.",  "region":"MENA",       "currency":"SAR","sector":"energia diversif",    "etf":"KSA"},
    {"id":"ARG","name":"Argentina",  "region":"LATAM Sur",  "currency":"ARS","sector":"reforma estructural", "etf":"ARGT"},
    {"id":"IDN","name":"Indonesia",  "region":"SE Asia",    "currency":"IDR","sector":"recursos demografía", "etf":"EIDO"},
    {"id":"MEX","name":"México",     "region":"LATAM Norte","currency":"MXN","sector":"nearshoring",         "etf":"EWW"},
    {"id":"CHN","name":"China",      "region":"East Asia",  "currency":"CNY","sector":"tech manufactura exp","etf":"MCHI"},
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
    print(f"  -> Telegram {level}: token={TELEGRAM_TOKEN[:20]}... chat={TELEGRAM_CHAT_ID}")
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
    alerts  = results.get("alerts", [])
    core    = results.get("core", {})
    ranking = results.get("ranking", [])
    fred    = results.get("macro", {}).get("fred", {})

    for a in [x for x in alerts if x.get("severity") == "high"]:
        send_telegram(f"{a['asset']}\n{a['message']}", "critical")
        time.sleep(0.5)

    buys = [r for r in ranking
            if r.get("signal","").upper() in ("COMPRAR","BUY")
            and r.get("prob",0) >= 60]
    if buys:
        msg = "Señales COMPRAR:\n" + "".join(
            f"• {s['name']}: {s['score']}/100, p={s.get('prob','?')}%\n" for s in buys)
        send_telegram(msg, "important")
        time.sleep(0.5)

    positions = core.get("positions", [])
    if positions:
        total = core.get("total_eur", 0)
        msg   = f"Cartera Core — EUR{total:,.2f}\n"
        for p in positions:
            d = p.get("drift_pct", 0)
            e = "🔴" if abs(d)>10 else "🟡" if abs(d)>5 else "🟢"
            msg += f"{e} {p['name']}: {p['weight_pct']:.1f}% (drift {d:+.1f}%)\n"
        send_telegram(msg, "important")
        time.sleep(0.5)

    if fred:
        send_telegram(
            f"Macro | FED:{fred.get('fed_funds_rate','?')}% "
            f"DXY:{fred.get('dxy_index','?')} "
            f"CPI:{fred.get('cpi_yoy','?')}% "
            f"Curve:{fred.get('yield_curve_spread','?')}bps",
            "info")

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

def fetch_wb_gdp(iso):
    url = (f"https://api.worldbank.org/v2/country/{iso}/indicator/"
           f"NY.GDP.MKTP.KD.ZG?format=json&mrv=4&per_page=4")
    d = fetch(url)
    if not d or not d[1]: return None
    return [o["value"] for o in d[1] if o["value"] is not None]

def fetch_fx():
    d = fetch("https://api.frankfurter.app/latest?from=USD"
              "&to=EUR,INR,BRL,PLN,SAR,ARS,IDR,MXN,CNY,VND,GBP")
    return d.get("rates",{}) if d else {}

def fetch_eia():
    if not FRED_KEY: return None
    obs = fetch_fred("WCRSTUS1")
    if not obs or len(obs)<2: return None
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
            print(f"    [Claude] ERROR SDK: {e}")
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
        print("    [Claude] ERROR: respuesta nula")
        return None
    if r.get("error"):
        print(f"    [Claude] ERROR API: {r['error']}")
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
         f"Kelly:{round((kf or 0)*100,1)}%\n"
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

def kahneman_geo(region,gdp_vals,fx_rates,news,macro_ctx):
    name=region["name"]; sec=region["sector"]
    gdp=gdp_vals[0] if gdp_vals else None
    fx=fx_rates.get(region["currency"])
    ns=" | ".join([n["title"] for n in news[:3]]) if news else "sin noticias"

    ctx=(f"REGION: {name} ({region['region']}) | {DATE_ES} | {QUARTER}\n"
         f"Sector:{sec} | PIB:{gdp}% | FX:1 USD={fx} {region['currency']}\n"
         f"Noticias:{ns}\nMACRO:{macro_ctx}")

    print(f"     -> Kahneman geo paso 1/3...")
    t1=claude(f"""{ctx}

ANALISIS GEOESTRATEGICO — formato exacto:

SITUACION MACRO:
[datos cuantificados: PIB, FX, contexto global]

CONTEXTO GEOPOLITICO:
[riesgos y oportunidades especificos del pais cuantificados]

OPORTUNIDAD en {sec}:
[tamaño estimado, drivers, timeline]

PROBABILIDAD INICIAL: p=X% +/-Y%""",450)
    time.sleep(0.5)

    print(f"     -> Kahneman geo paso 2/3...")
    t2=claude(f"""Tesis {name}:{t1}

PRE-MORTEM GEOESTRATEGICO — 3 factores de bloqueo:

FACTOR 1: [nombre]
Probabilidad: X%
Mecanismo de bloqueo: [especifico]
Señal de alerta: [observable]

FACTOR 2: [nombre]
Probabilidad: X%
Mecanismo de bloqueo: [especifico]
Señal de alerta: [observable]

FACTOR 3: [nombre]
Probabilidad: X%
Mecanismo de bloqueo: [especifico]
Señal de alerta: [observable]

PROBABILIDAD AJUSTADA: p=X% +/-Y%""",380)
    time.sleep(0.5)

    print(f"     -> Kahneman geo paso 3/3 (JSON)...")
    t3=claude(f"""Tesis:{t1} Pre-mortem:{t2}
Responde SOLO con este JSON sin texto ni markdown:
{{"signal":"OPORTUNIDAD_ALTA|OPORTUNIDAD_MEDIA|ESPERAR|EVITAR","prob":60,"prob_interval":15,"horizon":"6-18 meses","invalidation":"condicion concreta","summary":"una frase","conviction":"alta|media|baja","top_risk":"riesgo principal"}}""",220)
    time.sleep(0.5)

    cal = parse_cal(t3)
    print(f"     -> Geo calibracion: signal={cal.get('signal')} prob={cal.get('prob')}")

    return {"thesis":t1 or "","premortem":t2 or "","calibration":cal}

def kahneman_core(positions,macro_ctx,perf,news_nvda,news_btc):
    pos_str="\n".join([
        f"- {p['name']}: {p['weight_pct']:.1f}% | target {p['target_pct']*100:.0f}% "
        f"| drift {p['drift_pct']:+.1f}% | Sharpe {p.get('sharpe','N/A')} "
        f"| MDD {p.get('max_drawdown','N/A')}% | Return1Y {p.get('return_1y','N/A')}%"
        for p in positions])
    perf_str=json.dumps(perf.get("portfolio",{}))
    corr=perf.get("correlations",{})
    news_str=""
    if news_nvda: news_str+=f"NVIDIA:{' | '.join([n['title'] for n in news_nvda[:2]])}\n"
    if news_btc:  news_str+=f"Bitcoin:{' | '.join([n['title'] for n in news_btc[:2]])}\n"

    prompt=f"""REVISION TRIMESTRAL CARTERA CORE — {DATE_ES} — {QUARTER}

POSICIONES:
{pos_str}

METRICAS CARTERA:
Sharpe portfolio: {perf_str}
Correlaciones: MSCI-NVDA={corr.get('msci_nvda','N/A')} | MSCI-BTC={corr.get('msci_btc','N/A')} | NVDA-BTC={corr.get('nvda_btc','N/A')}

MACRO: {macro_ctx}

NOTICIAS:
{news_str}

CHECKLIST DE CONVICTION — responde cada punto:

1. MSCI World (target 70%): ¿Tesis de diversificacion global sigue valida? [SI/NO/DEBILITADA + razon]
2. NVIDIA (target 10%): ¿Moat en infraestructura IA ha cambiado? [SI/NO/DEBILITADA + razon]
3. Bitcoin (target 5%): ¿Tesis reserva de valor sigue valida? [SI/NO/DEBILITADA + razon]
4. CONCENTRACION: Drift NVDA={[p['drift_pct'] for p in positions if p['id']=='NVDA'][0] if positions else 'N/A'}%. ¿Requiere accion? [SI/NO + razon]
5. CORRELACIONES: MSCI-NVDA={corr.get('msci_nvda','N/A')}. ¿Diversificacion reducida? [SI/NO]
6. MACRO: ¿Entorno FED/DXY favorece cartera 3-5 años? [FAVORECE/NEUTRAL/PERJUDICA]
7. RIESGO SISTEMICO: ¿Riesgo nuevo en 3 meses? [SI/NO + descripcion]
8. REBALANCEO: ¿Rebalancear ahora? [SI/NO/ESPERAR + razon]
9. HORIZONTE: ¿Evento en 12 meses que invalide tesis? [especificar]
10. VEREDICTO: ¿Cartera bien posicionada 3-10 años? [SI/NO/PARCIALMENTE + 2 frases]

JSON final sin markdown:
{{"msci_conviction":"intacta|debilitada|invalidada","nvda_conviction":"intacta|debilitada|invalidada","btc_conviction":"intacta|debilitada|invalidada","rebalance_needed":true,"rebalance_urgency":"inmediato|proximo_mes|proximo_trimestre","rebalance_action":"descripcion o null","summary":"2 frases","overall_conviction":"alta|media|baja"}}"""

    raw=claude(prompt,1200)
    return {"review":raw or "","calibration":parse_cal(raw)}

def kahneman_risks(assets_data,regions_data,macro_ctx):
    asset_sum="; ".join([f"{a['meta']['name']}:{a['quant']['composite_score']}/100"
                         for a in assets_data if a.get('quant')])
    region_sum="; ".join([f"{r['name']}:PIB{r.get('gdp_latest','?')}%"
                          for r in regions_data])
    raw=claude(f"""FECHA:{DATE_ES} MACRO:{macro_ctx}
COMMODITIES:{asset_sum}
REGIONES:{region_sum}

5 riesgos geopoliticos y macro actuales. Formato:
RIESGO N: [nombre]
Nivel: X/100
Descripcion: [impacto concreto]
Activos: [lista]
Señal: [observable]

JSON final sin markdown:
[{{"name":"str","level":65,"desc":"str","assets_affected":["str"],"signal":"str"}}]""",650)
    if not raw: return []
    try:
        s=raw.find("["); e=raw.rfind("]")+1
        if s!=-1 and e>s: return json.loads(raw[s:e])
    except: pass
    return []

# ── MODULO 3: CORE RIGUROSO ───────────────────────────

def compute_portfolio_metrics(prices_raw):
    metrics={}
    for pid,raw in [("MSCI",prices_raw.get("IWDA.AS",[])),
                    ("NVDA",prices_raw.get("NVDA",[])),
                    ("BTC", prices_raw.get("BTC-EUR",[]))]:
        if not raw or len(raw)<20: continue
        closes=[c for _,c in raw] if isinstance(raw[0],tuple) else raw
        metrics[pid]={
            "sharpe":       sharpe_ratio(closes),
            "calmar":       calmar_ratio(closes),
            "max_drawdown": max_dd(closes,len(closes)),
            "volatility":   ann_vol(closes),
            "return_1y":    round((closes[-1]/closes[-252]-1)*100,2) if len(closes)>=252 else None,
            "var_95":       var95(closes),
        }

    def get_rets(raw):
        c=[x for _,x in raw] if raw and isinstance(raw[0],tuple) else (raw or [])
        return [math.log(c[i]/c[i-1]) for i in range(1,len(c)) if c[i-1]>0]

    rm=get_rets(prices_raw.get("IWDA.AS",[]))
    rn=get_rets(prices_raw.get("NVDA",[]))
    rb=get_rets(prices_raw.get("BTC-EUR",[]))

    metrics["correlations"]={
        "msci_nvda":correlation(rm,rn),
        "msci_btc": correlation(rm,rb),
        "nvda_btc": correlation(rn,rb),
    }

    n=min(len(rm),len(rn),len(rb))
    if n>20:
        prets=[TARGET_MSCI*rm[-(n-i)]+TARGET_NVDA*rn[-(n-i)]+TARGET_BTC*rb[-(n-i)]
               for i in range(n)]
        mu=statistics.mean(prets); sig=statistics.stdev(prets)
        metrics["portfolio"]={
            "sharpe_ann":  round((mu*252-0.0364)/(sig*math.sqrt(252)),3) if sig>0 else None,
            "vol_ann":     round(sig*math.sqrt(252)*100,2),
            "return_ann":  round(mu*252*100,2),
        }
    return metrics

def run_core_module(fx_rates,macro_ctx):
    print("\n--- MODULO 3: CARTERA CORE (RIGUROSO) ----------")
    prices_raw={}
    for asset in CORE_ASSETS:
        raw=fetch_yahoo(asset["ticker"],days=400)
        if raw: prices_raw[asset["id"]]=raw; print(f"   {asset['name']}: {raw[-1][1]:.2f}")

    eur_usd=fx_rates.get("EUR",0.87)
    nvda_raw=prices_raw.get("NVDA",[])
    btc_raw=prices_raw.get("BTC-EUR",[])
    msci_raw=prices_raw.get("IWDA.AS",[])

    nvda_eur=nvda_raw[-1][1]*eur_usd if nvda_raw else 0
    btc_eur=btc_raw[-1][1]           if btc_raw  else 0
    msci_eur=msci_raw[-1][1]         if msci_raw else 0

    val_nvda=UNITS_NVDA*nvda_eur
    val_btc=UNITS_BTC*btc_eur
    val_msci=UNITS_MSCI*msci_eur if msci_eur>0 else PORTFOLIO_EUR-val_nvda-val_btc
    total=val_msci+val_nvda+val_btc

    perf=compute_portfolio_metrics(prices_raw)

    positions=[]
    for pid,name,val,price_eur,units,target in [
        ("MSCI","MSCI World",val_msci,msci_eur,UNITS_MSCI,TARGET_MSCI),
        ("NVDA","NVIDIA",    val_nvda,nvda_eur, UNITS_NVDA,TARGET_NVDA),
        ("BTC", "Bitcoin",   val_btc, btc_eur,  UNITS_BTC, TARGET_BTC)]:
        m=perf.get(pid,{})
        positions.append({
            "id":pid,"name":name,"units":units,
            "price_eur":round(price_eur,4),"value_eur":round(val,2),
            "weight_pct":round(val/total*100,2) if total>0 else 0,
            "target_pct":target,
            "drift_pct":round((val/total-target)*100,2) if total>0 else 0,
            "sharpe":m.get("sharpe"),"max_drawdown":m.get("max_drawdown"),
            "return_1y":m.get("return_1y"),"volatility":m.get("volatility"),
            "var_95":m.get("var_95"),
        })
        print(f"   {name}: {round(val/total*100,1) if total>0 else 0}% "
              f"Sharpe:{m.get('sharpe','N/A')} MDD:{m.get('max_drawdown','N/A')}%")

    alerts=[]
    for p in positions:
        if abs(p["drift_pct"])>10:
            alerts.append({"type":"DRIFT_ALERT","asset":p["name"],
                "message":f"{p['name']}: {p['weight_pct']:.1f}% vs target "
                          f"{p['target_pct']*100:.0f}% (drift {p['drift_pct']:+.1f}%)",
                "severity":"high" if abs(p["drift_pct"])>15 else "medium"})
        if p["id"]=="NVDA" and p["weight_pct"]>20:
            alerts.append({"type":"CONCENTRATION_ALERT","asset":"NVIDIA",
                "message":f"NVIDIA al {p['weight_pct']:.1f}% — supera limite 20%",
                "severity":"high"})
        if p.get("sharpe") and p["sharpe"]<0:
            alerts.append({"type":"NEGATIVE_SHARPE","asset":p["name"],
                "message":f"{p['name']}: Sharpe negativo ({p['sharpe']})",
                "severity":"important"})

    review_data={"review":"","calibration":{},"performed":False}
    if IS_QUARTERLY and ANTHROPIC_KEY:
        print("   -> Revision trimestral Kahneman v8 (checklist 10 puntos)...")
        news_nvda=fetch_news("NVIDIA earnings AI revenue",3)
        news_btc=fetch_news("Bitcoin institutional ETF",2)
        review_data=kahneman_core(positions,macro_ctx,perf,news_nvda,news_btc)
        review_data["performed"]=True; review_data["date"]=DATE_ES
    else:
        review_data["next_review"]="Primer lunes de trimestre"

    print(f"   Total: ~EUR{total:.0f}")
    return {"positions":positions,"total_eur":round(total,2),"performance":perf,
            "alerts":alerts,"review":review_data,"is_quarterly":IS_QUARTERLY,
            "timestamp":DATE_ES}

# ── MODULO 1: TRADING ─────────────────────────────────

def run_trading_module(fx_rates,eia,macro_ctx):
    print("\n--- MODULO 1: TRADING ---------------------------")
    results=[]; gdp_ind=fetch_wb_gdp("IND")

    for asset in TRADING_ASSETS:
        print(f"   {asset['name']}...")
        log={"module":"trading","asset":asset["name"],
             "ticker":asset["id"],"timestamp":DATE_ES}

        prices_raw=fetch_yahoo(asset["id"])
        if not prices_raw or len(prices_raw)<50:
            log["status"]="skipped_no_data"
            results.append({"meta":asset,"quant":None,"analysis":{},"log":log})
            continue

        closes=[p[1] for p in prices_raw]
        rsi_v=rsi(closes); _,macd_h=macd(closes)
        bb_low,_,bb_h=bollinger(closes); atr_v=atr_calc(closes)
        vol=ann_vol(closes); mdd=max_dd(closes); var=var95(closes)
        shr=sharpe_ratio(closes)
        tech=tech_score(closes,rsi_v,macd_h,bb_low,bb_h)
        fund=fund_score(gdp_ind,fx_rates,eia,asset["type"])
        comp=comp_score(tech,fund)
        lv=entry_levels(closes,atr_v)
        kf=kelly_frac(comp/100,(lv["target_pct"]/100) if lv else 0.08,
                      (lv["stop_pct"]/100) if lv else 0.04,fraction=0.50)

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

        analysis={"thesis":"","premortem":"","calibration":{},"audit":audit}
        if ANTHROPIC_KEY:
            print(f"     -> Kahneman v8 (audit:{audit['score']}/100)...")
            analysis=kahneman_trading(asset,qdata,macro_ctx,audit)
            log["claude_calls"]=3 if audit["passed"] else 1

        log["scores"]={"tech":tech,"fund":fund,"composite":comp}
        log["signal"]=analysis["calibration"].get("signal","ESPERAR")
        log["audit_passed"]=audit["passed"]
        log["status"]="complete"
        results.append({"meta":asset,"quant":qdata,"analysis":analysis,"log":log})
        time.sleep(1)

    print(f"   {len(results)} activos analizados")
    return results

# ── MODULO 2: GEOESTRATEGICO ──────────────────────────

def run_geo_module(fx_rates,macro_ctx):
    print("\n--- MODULO 2: GEOESTRATEGICO --------------------")
    results=[]
    for reg in REGIONS:
        print(f"   {reg['name']}...")
        gdp_vals=fetch_wb_gdp(reg["id"])
        fx_val=fx_rates.get(reg["currency"])
        news=fetch_news(f"{reg['name']} economy investment {NOW.year}",5)
        gdp_lat=round(gdp_vals[0],2) if gdp_vals else None
        gdp_trend=("acelerando" if gdp_vals and len(gdp_vals)>=2 and gdp_vals[0]>gdp_vals[1]
                   else "desacelerando" if gdp_vals and len(gdp_vals)>=2 and gdp_vals[0]<gdp_vals[1]
                   else "estable" if gdp_vals else "sin datos")
        score=50
        if gdp_lat:
            if gdp_lat>5: score+=15
            elif gdp_lat>3: score+=8
            elif gdp_lat<1: score-=10
            if gdp_trend=="acelerando": score+=5
        score=max(0,min(100,score))
        analysis={"thesis":"","premortem":"","calibration":{}}
        if ANTHROPIC_KEY:
            print(f"     -> Kahneman geo v8...")
            analysis=kahneman_geo(reg,gdp_vals,fx_rates,news,macro_ctx)
        results.append({
            "id":reg["id"],"name":reg["name"],"region":reg["region"],
            "sector":reg["sector"],"currency":reg["currency"],
            "fx_usd":round(fx_val,4) if fx_val else None,
            "gdp_latest":gdp_lat,"gdp_trend":gdp_trend,
            "score":score,"news":news[:3],"analysis":analysis})
        time.sleep(1)
    print(f"   {len(results)} regiones analizadas")
    return results

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

    # ── Backtesting geográfico con ETFs proxy ──
    print("   Backtesting geografico (ETFs proxy)...")
    for reg in REGIONS:
        etf = reg.get("etf")
        if not etf:
            continue
        print(f"   Backtesting {reg['name']} ({etf})...")
        raw = fetch_yahoo(etf, days=1825)
        if not raw or len(raw) < 500:
            print(f"   SKIP {reg['name']}"); continue
        bt = run_backtest_asset(f"{reg['name']} ({etf})", raw)
        if bt:
            bt["category"] = "region"
            bt["region"]   = reg["region"]
            results.append(bt)
            print(f"   {reg['name']}: WR={bt['win_rate_pct']}% B&H={bt['bh_annual_pct']}%/año")
        time.sleep(0.3)

    print(f"   Backtesting: {len(results)} activos+regiones")
    return results

# ══════════════════════════════════════════════════════
# MÓDULO 5: PAPER TRADING
# ══════════════════════════════════════════════════════

PAPER_FILE = os.path.join(os.path.dirname(__file__), "paper_trades.json")

def load_paper_trades():
    try:
        if os.path.exists(PAPER_FILE):
            with open(PAPER_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except: pass
    return {"trades": [], "stats": {}}

def save_paper_trades(data):
    try:
        with open(PAPER_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  WARN paper_trades: {e}")

def update_paper_trades(trading_results, paper_data):
    trades = paper_data.get("trades", [])
    existing_ids = {t["id"] for t in trades}

    for trade in trades:
        if trade["status"] != "open": continue
        asset_id = trade["asset_id"]
        current_price = None
        for a in trading_results:
            if a["meta"]["id"] == asset_id and a.get("quant"):
                current_price = a["quant"].get("price")
                break
        if not current_price: continue

        trade["current_price"] = round(current_price, 4)
        if trade["signal"] == "VENDER":
            trade["current_pnl_pct"] = round((trade["entry_price"]/current_price-1)*100, 2)
        else:
            trade["current_pnl_pct"] = round((current_price/trade["entry_price"]-1)*100, 2)

        if trade["signal"] == "COMPRAR":
            if current_price <= trade["stop_price"]:
                trade["status"] = "stopped"
                trade["exit_price"] = trade["stop_price"]
                trade["exit_date"] = DATE_ES
                trade["pnl_pct"] = round((trade["stop_price"]/trade["entry_price"]-1)*100, 2)
                trade["result"] = "loss"
            elif current_price >= trade["target_price"]:
                trade["status"] = "target_hit"
                trade["exit_price"] = trade["target_price"]
                trade["exit_date"] = DATE_ES
                trade["pnl_pct"] = round((trade["target_price"]/trade["entry_price"]-1)*100, 2)
                trade["result"] = "win"
        elif trade["signal"] == "VENDER":
            if current_price >= trade["stop_price"]:
                trade["status"] = "stopped"
                trade["exit_price"] = trade["stop_price"]
                trade["exit_date"] = DATE_ES
                trade["pnl_pct"] = round((trade["entry_price"]/trade["stop_price"]-1)*100, 2)
                trade["result"] = "loss"
            elif current_price <= trade["target_price"]:
                trade["status"] = "target_hit"
                trade["exit_price"] = trade["target_price"]
                trade["exit_date"] = DATE_ES
                trade["pnl_pct"] = round((trade["entry_price"]/trade["target_price"]-1)*100, 2)
                trade["result"] = "win"

    new_count = 0
    for a in trading_results:
        if not a.get("quant"): continue
        cal  = a["analysis"].get("calibration", {})
        sig  = cal.get("signal", "")
        prob = cal.get("prob", 0)
        if sig not in ("COMPRAR", "VENDER"): continue
        if prob < 40: continue

        trade_id = f"{a['meta']['id']}_{DATE_ES[:10]}"
        if trade_id in existing_ids: continue

        q  = a["quant"]
        lv = q.get("levels", {}) or {}
        if not lv.get("entry"): continue

        # Niveles correctos según dirección: VENDER invierte stop y target
        atr_v = q.get("atr")
        if sig == "VENDER" and atr_v:
            ep = lv["entry"]
            sp = round(ep + 2.0 * atr_v, 4)
            tp = round(ep - 4.0 * atr_v, 4)
            spc = round((sp - ep) / ep * 100, 2)
            tpc = round((ep - tp) / ep * 100, 2)
        else:
            ep, sp, tp = lv["entry"], lv["stop"], lv["target"]
            spc, tpc = lv.get("stop_pct"), lv.get("target_pct")

        trade = {
            "id":            trade_id,
            "asset":         a["meta"]["name"],
            "asset_id":      a["meta"]["id"],
            "signal":        sig,
            "entry_date":    DATE_ES,
            "entry_price":   ep,
            "stop_price":    sp,
            "target_price":  tp,
            "stop_pct":      spc,
            "target_pct":    tpc,
            "rr":            lv.get("rr", 2.0),
            "prob":          prob,
            "prob_interval": cal.get("prob_interval"),
            "conviction":    cal.get("conviction", ""),
            "score":         q.get("composite_score"),
            "kelly":         q.get("kelly"),
            "current_price": lv["entry"],
            "current_pnl_pct": 0.0,
            "status":        "open",
            "exit_price":    None,
            "exit_date":     None,
            "pnl_pct":       None,
            "result":        None,
            "summary":       cal.get("summary", ""),
        }
        trades.append(trade)
        existing_ids.add(trade_id)
        new_count += 1
        print(f"   Paper trade registrado: {a['meta']['name']} {sig} @ ${ep}")

    closed = [t for t in trades if t["status"] in ("stopped","target_hit")]
    open_t = [t for t in trades if t["status"] == "open"]
    wins   = [t for t in closed if t["result"] == "win"]
    losses = [t for t in closed if t["result"] == "loss"]

    win_rate = round(len(wins)/len(closed)*100, 1) if closed else None
    avg_win  = round(sum(t["pnl_pct"] for t in wins)/len(wins), 2) if wins else None
    avg_loss = round(sum(t["pnl_pct"] for t in losses)/len(losses), 2) if losses else None
    expectancy = None
    if win_rate and avg_win and avg_loss:
        wr = win_rate/100
        expectancy = round(wr*avg_win + (1-wr)*avg_loss, 2)

    stats = {
        "total_signals":  len(trades),
        "open":           len(open_t),
        "closed":         len(closed),
        "wins":           len(wins),
        "losses":         len(losses),
        "win_rate_pct":   win_rate,
        "avg_win_pct":    avg_win,
        "avg_loss_pct":   avg_loss,
        "expectancy_pct": expectancy,
        "new_this_run":   new_count,
        "last_updated":   DATE_ES,
    }

    paper_data["trades"] = trades
    paper_data["stats"]  = stats
    return paper_data

def run_paper_trading_module(trading_results):
    print("\n--- MODULO 5: PAPER TRADING ---------------------")
    paper_data = load_paper_trades()
    paper_data = update_paper_trades(trading_results, paper_data)
    save_paper_trades(paper_data)
    s = paper_data["stats"]
    print(f"   Señales: {s['total_signals']} total | {s['open']} abiertas | "
          f"{s['closed']} cerradas | WR: {s['win_rate_pct']}% | "
          f"Nuevas: {s['new_this_run']}")
    return paper_data

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
# magnitude_hist: magnitud histórica documentada en papers (no inventada)
# duration_hist: días antes de reversión histórica media

GEO_EVENT_MAP = [
    {
        "event_type": "MIDDLE_EAST_ESCALATION",
        "keywords": [
            "iran", "hormuz", "strait of hormuz", "persian gulf", "tehran",
            "israel iran", "us iran", "attack iran", "strike iran",
            "saudi oil", "aramco attack", "houthi", "red sea attack",
            "hamas", "hezbollah escalation", "middle east war"
        ],
        "assets_affected": ["CL=F", "NG=F", "GC=F", "SI=F"],
        "etfs_affected":   ["KSA", "INDA"],
        "direction":       "up",
        "channel":         "supply_disruption",
        "magnitude_hist":  "WTI +8-20% (Gulf crisis 2019), Oro +5-8%",
        "duration_hist":   "Reversión media 15-30 días si no hay disrupción física confirmada",
        "cameo_codes":     ["14", "15", "19", "20"],  # Military action codes
        "confidence_floor": 0.70  # Umbral mínimo de confianza Claude para alertar
    },
    {
        "event_type": "RUSSIA_ENERGY_DISRUPTION",
        "keywords": [
            "russia sanctions", "russia gas", "nord stream", "gazprom",
            "russia ukraine escalation", "russia nato", "pipeline sabotage",
            "russian oil ban", "russian energy", "europe gas supply"
        ],
        "assets_affected": ["NG=F", "CL=F", "ALI=F"],
        "etfs_affected":   ["EPOL"],
        "direction":       "up",
        "channel":         "supply_disruption",
        "magnitude_hist":  "Gas natural +7.5%/evento (Palomba 2025), Aluminio +15% (LME 2022)",
        "duration_hist":   "Efectos persistentes si disrupción física confirmada (>30 días)",
        "cameo_codes":     ["16", "17"],
        "confidence_floor": 0.68
    },
    {
        "event_type": "CHINA_TAIWAN_TENSION",
        "keywords": [
            "taiwan strait", "china taiwan", "pla military", "taiwan blockade",
            "china semiconductor", "tsmc risk", "taiwan invasion", "china military exercise"
        ],
        "assets_affected": ["GC=F", "CL=F"],
        "etfs_affected":   ["MCHI", "VNM", "INDA"],
        "direction":       "mixed",  # Oro sube, ETF China baja, Vietnam puede subir (nearshoring)
        "channel":         "uncertainty",
        "magnitude_hist":  "S&P -2.5% media conflictos socios comerciales (IMF 2025)",
        "duration_hist":   "Recuperación media en 30 días si no hay escalada física",
        "cameo_codes":     ["14", "15"],
        "confidence_floor": 0.72
    },
    {
        "event_type": "OPEC_SUPPLY_SHOCK",
        "keywords": [
            "opec cut", "opec production", "saudi production cut", "opec plus",
            "oil supply reduction", "opec surprise", "production quota",
            "opec meeting", "oil embargo"
        ],
        "assets_affected": ["CL=F", "NG=F"],
        "etfs_affected":   ["KSA"],
        "direction":       "up",
        "channel":         "supply_disruption",
        "magnitude_hist":  "WTI +5-15% sorpresa OPEC (IMF, Känzig 2021 AER)",
        "duration_hist":   "Efecto persiste si recorte real; reversión si solo verbal",
        "cameo_codes":     ["08", "09"],
        "confidence_floor": 0.65
    },
    {
        "event_type": "US_TRADE_WAR",
        "keywords": [
            "tariff", "trade war", "import duty", "trade sanctions",
            "us china tariff", "trump tariff", "trade restrictions",
            "export controls", "technology ban", "chip ban"
        ],
        "assets_affected": ["CL=F", "HG=F", "ALI=F"],
        "etfs_affected":   ["MCHI", "EWW", "EWZ", "INDA"],
        "direction":       "mixed",  # China baja, México puede subir (desvío comercial)
        "channel":         "trade_diversion",
        "magnitude_hist":  "S&P -9% Liberation Day 2025; México +3-5% desvío comercial",
        "duration_hist":   "Efectos persistentes en cadenas de suministro (>60 días)",
        "cameo_codes":     ["16"],
        "confidence_floor": 0.65
    },
    {
        "event_type": "GLOBAL_RISK_OFF",
        "keywords": [
            "nuclear threat", "war declaration", "military invasion",
            "global recession fears", "financial crisis", "bank failure",
            "systemic risk", "market crash", "flight to safety"
        ],
        "assets_affected": ["GC=F", "SI=F", "CL=F"],
        "etfs_affected":   ["ARGT", "EWZ", "EIDO"],  # EM más vulnerables
        "direction":       "safe_haven_up",  # Oro/plata suben, EM bajan
        "channel":         "safe_haven",
        "magnitude_hist":  "Oro +5-10% eventos war (SSRN 2024), EM -8-12% (IMF 2025)",
        "duration_hist":   "Oro mantiene efecto; EM recuperan en 30 días media",
        "cameo_codes":     ["19", "20"],
        "confidence_floor": 0.75
    },
    {
        "event_type": "LATAM_POLITICAL_SHOCK",
        "keywords": [
            "argentina crisis", "brazil political", "mexico security",
            "latam election", "populism latin america", "debt default",
            "currency crisis", "capital controls", "latam unrest"
        ],
        "assets_affected": ["CL=F"],
        "etfs_affected":   ["ARGT", "EWZ", "EWW"],
        "direction":       "down",
        "channel":         "uncertainty",
        "magnitude_hist":  "ETFs regionales -5-15% según magnitud crisis",
        "duration_hist":   "Variable: puede persistir meses si es crisis sistémica",
        "cameo_codes":     ["14", "17"],
        "confidence_floor": 0.65
    },
    {
        "event_type": "FED_SURPRISE",
        "keywords": [
            "fed rate hike surprise", "powell hawkish", "emergency rate cut",
            "fed pivot", "inflation surprise", "cpi shock", "fed emergency meeting"
        ],
        "assets_affected": ["GC=F", "CL=F", "HG=F"],
        "etfs_affected":   ["ARGT", "EWZ", "INDA", "EIDO"],  # EM sensibles a USD
        "direction":       "down",  # Subida Fed = oro baja, EM bajan
        "channel":         "monetary",
        "magnitude_hist":  "~1-2% FX por 25bp sorpresa; EM -3-5%",
        "duration_hist":   "Efecto inmediato, reversión parcial en 5-10 días",
        "cameo_codes":     ["03", "04"],
        "confidence_floor": 0.70
    },
]

# ── VOLATILIDAD ACTUAL DEL ACTIVO (condicionante de régimen) ──────────────────
def get_asset_current_vol(asset_id, trading_results):
    """Obtiene volatilidad actual del activo desde los resultados del módulo trading."""
    for a in trading_results:
        if a["meta"]["id"] == asset_id and a.get("quant"):
            return a["quant"].get("volatility")
    return None

# ── FETCH GDELT DOC API (gratuito, sin key) ───────────────────────────────────
def fetch_geo_news(keywords, max_records=8):
    """
    Fetch de noticias geopolíticas usando NewsAPI (principal) + GDELT (fallback).
    NewsAPI: funciona de forma fiable, key ya disponible en el sistema.
    GDELT: fallback si NewsAPI no tiene resultados para la query.
    """
    # ── NewsAPI (fuente principal) ──
    if NEWS_KEY:
        query = " OR ".join(keywords[:4])
        url = (f"https://newsapi.org/v2/everything"
               f"?q={urllib.parse.quote(query)}"
               f"&language=en&sortBy=publishedAt"
               f"&pageSize={max_records}&apiKey={NEWS_KEY}")
        try:
            d = fetch(url)
            if d and d.get("status") == "ok":
                articles = d.get("articles", [])
                if articles:
                    return [
                        {
                            "title":  a.get("title", "")[:120],
                            "url":    a.get("url", ""),
                            "source": a.get("source", {}).get("name", ""),
                            "date":   a.get("publishedAt", ""),
                            "tone":   -1.0,  # NewsAPI no tiene tono — neutro-negativo
                            "lang":   "en",
                        }
                        for a in articles if a.get("title")
                    ]
        except Exception as e:
            print(f"  WARN NewsAPI geo ({query[:30]}): {e}")

    # ── GDELT fallback ──
    base = "https://api.gdeltproject.org/api/v2/doc/doc"
    gdelt_query = " OR ".join(keywords[:3])
    for timespan in ["24h", "48h", "1week"]:
        try:
            params = {
                "query":      gdelt_query,
                "mode":       "artlist",
                "maxrecords": str(max_records),
                "format":     "json",
                "timespan":   timespan,
                "sort":       "DateDesc",
            }
            url = base + "?" + urllib.parse.urlencode(params)
            req = Request(url, headers={"User-Agent": "GeoMacroIntel/8.0"})
            with urlopen(req, timeout=12) as r:
                raw = r.read().decode()
            if not raw or not raw.strip() or raw.strip()[0] != "{":
                continue
            data = json.loads(raw)
            articles = data.get("articles", [])
            if articles:
                return [
                    {
                        "title":  a.get("title", "")[:120],
                        "url":    a.get("url", ""),
                        "source": a.get("domain", ""),
                        "date":   a.get("seendate", ""),
                        "tone":   float(a.get("tone", 0)),
                        "lang":   a.get("language", ""),
                    }
                    for a in articles if a.get("title")
                ]
        except Exception:
            continue

    return []

# ── PRE-FILTRO DE KEYWORDS (evita llamadas Claude innecesarias) ───────────────
def prefilter_news(articles, event_map_entry):
    """
    Filtra artículos por keywords del evento.
    Retorna los que contienen al menos 1 keyword relevante.
    Coste: $0 (sin API).
    """
    keywords = event_map_entry["keywords"]
    matched = []
    for art in articles:
        text = (art.get("title", "") + " " + art.get("url", "")).lower()
        hits = [kw for kw in keywords if kw in text]
        if hits:
            matched.append({**art, "keyword_hits": hits})
    return matched

# ── CLASIFICACIÓN CON CLAUDE (solo artículos pre-filtrados) ──────────────────
def classify_geo_event(articles, event_entry, macro_ctx, trading_results):
    """
    Claude clasifica si el conjunto de artículos constituye un evento
    geopolítico real con impacto en activos específicos.

    Incluye:
    - Tipo de evento (CAMEO)
    - Activos afectados y dirección
    - Magnitud esperada con precedente histórico
    - Duración histórica del efecto
    - Régimen de mercado actual como condicionante
    - Separación explícita información/señal (anti-sesgo-acción)
    """
    if not articles or not ANTHROPIC_KEY:
        return None

    # Obtener volatilidad actual de activos afectados (condicionante de régimen)
    asset_vols = {}
    for aid in event_entry["assets_affected"]:
        v = get_asset_current_vol(aid, trading_results)
        if v: asset_vols[aid] = round(v, 1)

    vol_ctx = ", ".join([f"{k}:{v}%" for k, v in asset_vols.items()]) or "sin datos"

    headlines = "\n".join([
        f"- [{a['source']}] {a['title']} (tono:{a['tone']:.1f}, keywords:{a['keyword_hits']})"
        for a in articles[:5]
    ])

    prompt = f"""Eres un analista de riesgos geopolíticos con acceso a literatura académica (Caldara-Iacoviello 2022, IMF GFSR 2025).

EVENTO POTENCIAL: {event_entry['event_type']}
NOTICIAS DETECTADAS (últimas 6h):
{headlines}

ACTIVOS MONITORIZADOS: {', '.join(event_entry['assets_affected'] + event_entry['etfs_affected'])}
VOLATILIDAD ACTUAL DE ACTIVOS: {vol_ctx}
CONTEXTO MACRO ACTUAL: {macro_ctx}
PRECEDENTE HISTÓRICO: {event_entry['magnitude_hist']}
DURACIÓN HISTÓRICA: {event_entry['duration_hist']}

REGLAS CRÍTICAS DE EVALUACIÓN:
1. Escaladas militares reales > amenazas verbales (IMF 2025)
2. Disrupción física confirmada > especulación mediática
3. Alta volatilidad actual = umbral de alerta MÁS ALTO (ya está priced in)
4. Este módulo INFORMA, NO señaliza compra/venta. Incluir esto explícitamente.
5. Si las noticias son ruido sin sustancia real: impact=none

Responde SOLO con este JSON sin texto ni markdown:
{{"event_type":"{event_entry['event_type']}","is_real_event":true,"impact":"high|medium|low|none","assets_up":[],"assets_down":[],"magnitude_estimate":"X-Y%","confidence":0.75,"is_escalation":true,"historical_parallel":"evento similar + año + magnitud","duration_estimate":"X-Y días","regime_adjustment":"alto_vol_ya_priced_in|normal","alert_text":"1 frase informativa sin sesgo acción","warning":"INFORMATIVO: evaluar contexto completo antes de cualquier acción"}}"""

    raw = claude(prompt, max_tokens=300)
    if not raw:
        return None
    try:
        s = raw.find("{"); e = raw.rfind("}") + 1
        if s != -1 and e > s:
            return json.loads(raw[s:e])
    except Exception as ex:
        print(f"  WARN geo_intel parse: {ex}")
    return None

# ── MÓDULO PRINCIPAL ──────────────────────────────────────────────────────────
def run_geo_intelligence_module(macro_ctx, trading_results):
    """
    Módulo 6: Inteligencia Geopolítica en Tiempo Real.

    Arquitectura de 3 capas:
    1. GDELT + NewsAPI → ingesta multi-fuente (coste $0)
    2. Pre-filtro keywords → elimina ~90% ruido (coste $0)
    3. Claude clasificación → solo artículos relevantes (~$0.50-1/mes)

    Circuit breaker: máximo 2 alertas alta importancia por ciclo.
    Kahneman: informa, no señaliza. Contexto histórico obligatorio.
    """
    print("\n--- MODULO 6: INTELIGENCIA GEOPOLITICA -----------")

    events_detected = []
    high_alerts_count = 0
    MAX_HIGH_ALERTS = 2  # Circuit breaker conductual

    for event_entry in GEO_EVENT_MAP:
        etype = event_entry["event_type"]

        # ── Capa 1: Fetch GDELT ──
        # Usamos las primeras 3 keywords más específicas para la query GDELT
        query_terms = " OR ".join(event_entry["keywords"][:4])
        time.sleep(1)  # Pausa mínima entre queries
        all_articles = fetch_geo_news(event_entry["keywords"], max_records=8)

        if not all_articles:
            continue

        # ── Capa 2: Pre-filtro keywords ──
        filtered = prefilter_news(all_articles, event_entry)

        if not filtered:
            continue

        print(f"   {etype}: {len(filtered)} artículos relevantes detectados")

        # ── Capa 3: Clasificación Claude ──
        classification = classify_geo_event(
            filtered, event_entry, macro_ctx, trading_results
        )

        if not classification:
            continue

        impact = classification.get("impact", "none")
        confidence = classification.get("confidence", 0)
        confidence_floor = event_entry["confidence_floor"]

        # Ignorar si impacto nulo o confianza insuficiente
        if impact == "none" or confidence < confidence_floor:
            print(f"   {etype}: descartado (impact={impact}, conf={confidence:.2f})")
            continue

        # Marcar el nivel de régimen de mercado
        classification["event_type"] = etype
        classification["assets_monitored"] = (
            event_entry["assets_affected"] + event_entry["etfs_affected"]
        )
        classification["channel"] = event_entry["channel"]
        classification["articles_analyzed"] = len(filtered)
        classification["timestamp"] = DATE_ES

        events_detected.append(classification)
        print(f"   {etype}: DETECTADO impact={impact} conf={confidence:.2f}")

    # ── Circuit breaker: limitar alertas Telegram de alta importancia ──
    high_events = [e for e in events_detected if e.get("impact") == "high"]
    medium_events = [e for e in events_detected if e.get("impact") == "medium"]

    # Ordenar por confianza descendente
    high_events.sort(key=lambda x: x.get("confidence", 0), reverse=True)

    # Enviar Telegram solo para los top-2 de alta importancia
    for event in high_events[:MAX_HIGH_ALERTS]:
        assets_up   = ", ".join(event.get("assets_up", [])) or "—"
        assets_down = ", ".join(event.get("assets_down", [])) or "—"
        parallel    = event.get("historical_parallel", "sin precedente documentado")
        duration    = event.get("duration_estimate", "desconocida")
        regime      = event.get("regime_adjustment", "normal")
        alert_text  = event.get("alert_text", "")
        warning     = event.get("warning", "")

        regime_note = (
            "⚠️ VOLATILIDAD YA ELEVADA — efecto puede estar priced in"
            if regime == "alto_vol_ya_priced_in" else ""
        )

        msg = (
            f"🌍 <b>EVENTO GEOPOLÍTICO — {event['event_type']}</b>\n"
            f"Impacto: <b>{event['impact'].upper()}</b> | Confianza: {event['confidence']:.0%}\n"
            f"Canal: {event['channel']}\n\n"
            f"📈 Activos al alza: {assets_up}\n"
            f"📉 Activos a la baja: {assets_down}\n"
            f"Magnitud estimada: {event.get('magnitude_estimate','?')}\n"
            f"Duración histórica: {duration}\n\n"
            f"📚 Precedente: {parallel}\n"
            f"{regime_note}\n\n"
            f"ℹ️ {alert_text}\n"
            f"<i>{warning}</i>"
        )
        send_telegram(msg, "important")
        time.sleep(0.5)
        high_alerts_count += 1

    # Eventos de media importancia: resumen consolidado (1 solo mensaje)
    if medium_events:
        lines = ["🔍 <b>RADAR GEOPOLÍTICO — Eventos medios</b>\n"]
        for ev in medium_events[:3]:
            lines.append(
                f"• {ev['event_type']}: {ev.get('alert_text','')[:80]} "
                f"(conf:{ev.get('confidence',0):.0%})"
            )
        lines.append("\n<i>INFORMATIVO. No implica señal de trading.</i>")
        send_telegram("\n".join(lines), "info")

    total = len(events_detected)
    print(f"   Geo Intel: {total} eventos detectados | "
          f"{high_alerts_count} alertas high enviadas (max {MAX_HIGH_ALERTS})")

    return {
        "events":          events_detected,
        "high_count":      len(high_events),
        "medium_count":    len(medium_events),
        "circuit_breaker": high_alerts_count >= MAX_HIGH_ALERTS,
        "timestamp":       DATE_ES,
    }



# ── AUDITORÍA PÚBLICA VÍA GIST PRIVADO ───────────────────────────────────────

def publish_audit_gist(results):
    """
    Publica un resumen mínimo de auditoría en un Gist secreto de GitHub.
    Solo expone métricas agregadas — sin datos personales, sin valores de cartera,
    sin API keys ni tokens.
    Crea el Gist la primera vez; actualiza en ejecuciones posteriores.
    """
    if not GIST_TOKEN:
        print("  WARN Gist: GIST_TOKEN no disponible")
        return None

    fred = results.get("macro", {}).get("fred", {})
    paper = results.get("paper_trading", {}).get("stats", {})
    geo = results.get("geo_intelligence", {})
    alerts = results.get("alerts", [])
    high_alerts = [a for a in alerts if a.get("severity") == "high"]

    # Solo métricas agregadas — sin datos personales
    audit_data = {
        "timestamp":         results.get("generated_at", ""),
        "pipeline_ok":       True,
        "fred": {
            "fed_funds_rate":    fred.get("fed_funds_rate"),
            "yield_curve":       fred.get("yield_curve_spread"),
            "dxy_index":         fred.get("dxy_index"),
            "cpi_yoy":           fred.get("cpi_yoy"),
            "complete":          all([
                fred.get("fed_funds_rate"),
                fred.get("yield_curve_spread"),
                fred.get("dxy_index"),
                fred.get("cpi_yoy"),
            ]),
        },
        "geo_intelligence": {
            "executed":      True,
            "events_found":  len(geo.get("events", [])),
            "high_alerts":   geo.get("high_count", 0),
        },
        "paper_trading": {
            "total":         paper.get("total_signals", 0),
            "open":          paper.get("open", 0),
            "closed":        paper.get("closed", 0),
            "win_rate_pct":  paper.get("win_rate_pct"),
        },
        "alerts_high_count": len(high_alerts),
        "alerts_high":       [a.get("message", "")[:80] for a in high_alerts[:5]],
        "modules": {
            "trading":    len(results.get("trading", [])),
            "regions":    len(results.get("regions", [])),
            "backtesting": len(results.get("backtesting", [])),
        }
    }

    content = json.dumps(audit_data, ensure_ascii=False, indent=2)
    headers = {
        "Authorization": f"token {GIST_TOKEN}",
        "Content-Type":  "application/json",
        "User-Agent":    "GeoMacroIntel/8.0",
    }

    gist_id = GIST_ID

    # Si ya existe el Gist, actualizarlo (PATCH)
    if gist_id:
        payload = {"files": {"geomacro_audit.json": {"content": content}}}
        url = f"https://api.github.com/gists/{gist_id}"
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode(),
            headers=headers,
            method="PATCH"
        )
        try:
            with urllib.request.urlopen(req, timeout=14) as r:
                resp = json.loads(r.read().decode())
            print(f"  OK Gist actualizado: {resp.get('html_url','')}")
            return resp.get("id")
        except Exception as e:
            print(f"  WARN Gist PATCH: {e}")
            return None

    # Primera vez: crear el Gist
    payload = {
        "description": "GeoMacro Intel — Auditoría del sistema (datos agregados)",
        "public": False,
        "files": {"geomacro_audit.json": {"content": content}}
    }
    req = urllib.request.Request(
        "https://api.github.com/gists",
        data=json.dumps(payload).encode(),
        headers=headers,
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=14) as r:
            resp = json.loads(r.read().decode())
        gist_id = resp.get("id", "")
        url = resp.get("html_url", "")
        print(f"  OK Gist creado: {url}")
        print(f"  IMPORTANTE: añade GIST_ID={gist_id} como secret en GitHub Actions")
        # Intentar guardar el ID en un archivo local para el siguiente commit
        try:
            id_file = os.path.join(os.path.dirname(__file__), ".gist_id")
            with open(id_file, "w") as f:
                f.write(gist_id)
        except Exception:
            pass
        return gist_id
    except Exception as e:
        print(f"  WARN Gist POST: {e}")
        return None

# ── PIPELINE PRINCIPAL ────────────────────────────────

def run():
    print(f"\n{'='*58}\nGeoMacro Intel v8 — {DATE_ES}\n{'='*58}")
    print(f"Modelo Claude: {CLAUDE_MODEL}")
    print(f"Core trimestral: {'SI' if IS_QUARTERLY else 'NO'} | Backtest: {'SI' if IS_WEEKLY else 'NO'}\n")

    results={
        "generated_at":DATE_ES,"quarter":QUARTER,"version":"8.0",
        "is_quarterly":IS_QUARTERLY,"is_weekly":IS_WEEKLY,
        "trading":[],"regions":[],"global_risks":[],"core":{},"geo_intelligence":{},
        "backtesting":[],"macro":{},"ranking":[],"alerts":[],"audit_log":[],
    }

    print("\n-> Datos macro globales...")
    fx_rates=fetch_fx(); eia=fetch_eia(); fred_d={}
    if FRED_KEY:
        for sid,lbl in [("FEDFUNDS","fed_funds_rate"),
                        ("T10Y2Y","yield_curve_spread"),
                        ("DTWEXM","dxy_index")]:
            obs=fetch_fred(sid)
            if obs: fred_d[lbl]=round(obs[0][1],3); print(f"   FRED {lbl}: {obs[0][1]:.3f}")
        cpi_obs=fetch_fred("CPIAUCSL")
        if cpi_obs and len(cpi_obs)>=13:
            cpi_yoy=round((cpi_obs[0][1]/cpi_obs[12][1]-1)*100,2)
            fred_d["cpi_yoy"]=cpi_yoy; print(f"   FRED cpi_yoy: {cpi_yoy}%")

    results["macro"]={"fx_rates":fx_rates,"fred":fred_d,"eia_crude":eia,"timestamp":DATE_ES}
    macro_ctx=(f"FED:{fred_d.get('fed_funds_rate','N/A')}% "
               f"CPI YoY:{fred_d.get('cpi_yoy','N/A')}% "
               f"Yield10Y-2Y:{fred_d.get('yield_curve_spread','N/A')}bps "
               f"DXY:{fred_d.get('dxy_index','N/A')} "
               f"EUR/USD:{round(1/fx_rates['EUR'],4) if fx_rates.get('EUR') else 'N/A'} "
               f"Inv.crudos:{eia.get('vs_avg','N/A') if eia else 'N/A'}%")

    trading_results=run_trading_module(fx_rates,eia,macro_ctx)
    geo_results=run_geo_module(fx_rates,macro_ctx)
    core_result=run_core_module(fx_rates,macro_ctx)
    backtest_results=run_backtest_module()

    results["trading"]=trading_results; results["regions"]=geo_results
    results["core"]=core_result; results["backtesting"]=backtest_results

    paper_data=run_paper_trading_module(trading_results)
    results["paper_trading"]=paper_data

    geo_intel=run_geo_intelligence_module(macro_ctx, trading_results)
    results["geo_intelligence"]=geo_intel

    print("\n-> Riesgos globales...")
    if ANTHROPIC_KEY:
        results["global_risks"]=kahneman_risks(trading_results,geo_results,macro_ctx)

    # Ranking unificado
    all_items=[]
    for a in trading_results:
        if not a.get("quant"): continue
        cal=a["analysis"].get("calibration",{})
        all_items.append({
            "name":a["meta"]["name"],"type":a["meta"]["type"],"category":"commodity",
            "score":a["quant"]["composite_score"],"signal":cal.get("signal","—"),
            "prob":cal.get("prob"),"prob_interval":cal.get("prob_interval"),
            "kelly":a["quant"].get("kelly"),"vol":a["quant"].get("volatility"),
            "sharpe":a["quant"].get("sharpe"),"price":a["quant"].get("price"),
            "summary":cal.get("summary",""),"conviction":cal.get("conviction",""),
            "audit_passed":a["log"].get("audit_passed",True)})
    for r in geo_results:
        cal=r["analysis"].get("calibration",{})
        all_items.append({
            "name":r["name"],"type":r["region"],"category":"region",
            "score":r["score"],"signal":cal.get("signal","—"),
            "prob":cal.get("prob"),"prob_interval":cal.get("prob_interval"),
            "kelly":None,"vol":None,"sharpe":None,"price":None,
            "summary":cal.get("summary",""),"conviction":cal.get("conviction",""),
            "gdp":r.get("gdp_latest"),"sector":r.get("sector"),"audit_passed":True})
    all_items.sort(key=lambda x:x["score"],reverse=True)
    results["ranking"]=[{"rank":i+1,**item} for i,item in enumerate(all_items)]

    # Alertas
    for a in trading_results:
        q=a.get("quant",{}) or {}; cal=a["analysis"].get("calibration",{}); nm=a["meta"]["name"]
        if q.get("rsi") and q["rsi"]<30:
            results["alerts"].append({"type":"RSI_OVERSOLD","module":"trading","asset":nm,
                "value":q["rsi"],"message":f"{nm}: RSI={q['rsi']} sobreventa extrema","severity":"high"})
        if q.get("composite_score",0)>=70 and cal.get("signal")=="COMPRAR":
            results["alerts"].append({"type":"BUY_SIGNAL","module":"trading","asset":nm,
                "value":q["composite_score"],
                "message":f"{nm}: score {q['composite_score']}/100 COMPRAR p={cal.get('prob')}%+/-{cal.get('prob_interval','?')}%",
                "severity":"high"})
        if q.get("max_drawdown_6m",0)>25:
            results["alerts"].append({"type":"HIGH_DRAWDOWN","module":"trading","asset":nm,
                "value":q["max_drawdown_6m"],
                "message":f"{nm}: drawdown 6M={q['max_drawdown_6m']}%","severity":"medium"})
    for r in geo_results:
        cal=r["analysis"].get("calibration",{})
        if cal.get("signal")=="OPORTUNIDAD_ALTA" and cal.get("prob",0)>=65:
            results["alerts"].append({"type":"GEO_OPPORTUNITY","module":"geo","asset":r["name"],
                "value":r["score"],"message":f"{r['name']}: oportunidad alta p={cal.get('prob')}%","severity":"high"})
    results["alerts"].extend(core_result.get("alerts",[]))

    # Audit log
    results["audit_log"].extend([a["log"] for a in trading_results if a.get("log")])
    for r in geo_results:
        results["audit_log"].append({"module":"geo","asset":r["name"],"timestamp":DATE_ES,
            "scores":{"composite":r["score"]},"signal":r["analysis"].get("calibration",{}).get("signal","—"),
            "claude_calls":3 if ANTHROPIC_KEY else 0,"status":"complete"})
    results["audit_log"].append({"module":"core","asset":"Cartera Core","timestamp":DATE_ES,
        "scores":{"composite":None},"signal":"LARGO_PLAZO",
        "claude_calls":1 if (IS_QUARTERLY and ANTHROPIC_KEY) else 0,
        "status":"quarterly_review" if IS_QUARTERLY else "monitoring_only"})

    total_claude=sum(l.get("claude_calls",0) for l in results["audit_log"])
    audits_ok=sum(1 for a in trading_results if a.get("log",{}).get("audit_passed",True))
    geo_intel_count = len(geo_intel.get("events", []))
    print(f"\nTrading:{len(trading_results)} ({audits_ok} OK) | Geo:{len(geo_results)} | GeoIntel:{geo_intel_count} | "
          f"Backtest:{len(backtest_results)} | Alertas:{len(results['alerts'])} | Claude:{total_claude}")

    out=os.path.join(os.path.dirname(__file__),"results.json")
    with open(out,"w",encoding="utf-8") as f:
        json.dump(results,f,ensure_ascii=False,indent=2)
    print(f"Guardado: {out}")

    print("\n-> Publicando auditoría en Gist...")
    publish_audit_gist(results)

    print("\n-> Alertas Telegram...")
    build_telegram_summary(results)
    print(f"Pipeline v8 completado — {DATE_ES}\n")

if __name__=="__main__":
    run()

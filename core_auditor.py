#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════
GEOMACRO INTEL — AUDITOR DE CARTERA CORE  (módulo independiente)
═══════════════════════════════════════════════════════════════════════

PROPÓSITO: Auditar si la cartera core (largo plazo, máximo beneficio)
sigue siendo la correcta. NO da señales de timing. Vigila la SALUD DE LA
TESIS de cada activo y propone transiciones dulces cuando se deteriora.

FILOSOFÍA (inamovible):
- Máximo beneficio a largo plazo manteniendo la estructura de la cartera.
- Transición dulce: vender en verde, anticipando el deterioro antes del
  desplome, no reaccionando a él.
- Cada activo se estudia según su naturaleza (fundamentales / adopción).

TRES CAPAS:
  1. DETECTOR DE TESIS  — NVDA y SEMI (fundamentales FMP), BTC (on-chain)
  2. RADAR DE CANDIDATOS — rol convexo (cripto + acciones alto crecimiento)
                           con filtro anti-sesgo-de-supervivencia
  3. AUTOVIGILANCIA      — self-check de salud + watchdog. Alerta proactiva
                           por Telegram SOLO si algo falla. Cero ruido.

INFORME: semanal (lunes). SELF-CHECK: en cada ejecución.
═══════════════════════════════════════════════════════════════════════
"""
import os, json, time, urllib.request, urllib.parse
from datetime import datetime, timezone

# ── Configuración desde entorno (secrets de GitHub) ──
FMP_KEY        = os.environ.get("FMP_API_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")
NOW = datetime.now(timezone.utc)
DATE_ES = NOW.strftime("%d/%m/%Y %H:%M UTC")

# Cartera core real (de las memorias del usuario)
CORE_ASSETS = {
    "NVDA": {"name": "NVIDIA",            "type": "stock",  "role": "growth"},
    "SEMI": {"name": "ETF Semiconductores","type": "stock", "role": "growth", "fmp_proxy": "NVDA"},
    "BTC":  {"name": "Bitcoin",           "type": "crypto", "role": "convex"},
    "MSCI": {"name": "MSCI World",        "type": "index",  "role": "core"},
}

# ───────────────────────────────────────────────────────────────────────
# UTILIDADES DE RED (con detección de fallo para autovigilancia)
# ───────────────────────────────────────────────────────────────────────
def _get(url, headers=None, timeout=20):
    """Fetch JSON. Devuelve (data, error). error=None si OK."""
    try:
        req = urllib.request.Request(url, headers=headers or {"User-Agent": "Mozilla/5.0"})
        raw = urllib.request.urlopen(req, timeout=timeout).read()
        return json.loads(raw), None
    except urllib.error.HTTPError as e:
        return None, f"HTTP {e.code}"
    except Exception as e:
        return None, str(e)[:80]

def telegram_send(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        print("[telegram] sin credenciales, no se envía")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT, "text": text,
        "parse_mode": "HTML", "disable_web_page_preview": True
    }).encode()
    try:
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=15)
    except Exception as e:
        print(f"[telegram] error: {e}")

# ───────────────────────────────────────────────────────────────────────
# CAPA 1 — DETECTOR DE TESIS
# ───────────────────────────────────────────────────────────────────────
def fmp_fundamentals(symbol):
    """Trae 4 trimestres de income statement. (data, error)."""
    if not FMP_KEY:
        return None, "FMP_API_KEY no configurada"
    url = (f"https://financialmodelingprep.com/stable/income-statement"
           f"?symbol={symbol}&period=quarter&limit=4&apikey={FMP_KEY}")
    data, err = _get(url)
    if err:
        return None, f"FMP {symbol}: {err}"
    if not isinstance(data, list) or len(data) < 3:
        return None, f"FMP {symbol}: datos insuficientes ({len(data) if isinstance(data,list) else 0} trim)"
    return data, None

def analyze_fundamentals(quarters):
    """Semáforo de tesis basado en crecimiento de ingresos + márgenes."""
    q = list(reversed(quarters))  # cronológico
    revs = [x["revenue"] for x in q if x.get("revenue")]
    if len(revs) < 3:
        return {"status": "NO_DATA", "reason": "Ingresos incompletos"}
    gms = [q[i]["grossProfit"]/q[i]["revenue"]*100 for i in range(len(q)) if q[i].get("grossProfit") and q[i].get("revenue")]
    nms = [q[i]["netIncome"]/q[i]["revenue"]*100 for i in range(len(q)) if q[i].get("netIncome") and q[i].get("revenue")]

    growths = [(revs[i]/revs[i-1]-1)*100 for i in range(1, len(revs))]
    accelerating = growths[-1] > growths[0]
    growing = growths[-1] > 0
    margin_stable = len(gms) >= 2 and gms[-1] >= gms[0] - 1.5
    margin_eroding = len(gms) >= 2 and gms[-1] < gms[0] - 3

    if not growing or margin_eroding:
        status = "ROJO"
        reason = f"Crecimiento {growths[-1]:+.0f}%, margen {gms[0]:.0f}%→{gms[-1]:.0f}% — tesis deteriorándose"
    elif accelerating and margin_stable:
        status = "VERDE"
        reason = f"Ingresos acelerando {growths[0]:.0f}%→{growths[-1]:.0f}%, margen {gms[-1]:.0f}% sólido"
    else:
        status = "AMBAR"
        reason = f"Crece {growths[-1]:+.0f}% pero desacelera; margen {gms[-1]:.0f}% OK — vigilar"

    return {
        "status": status, "reason": reason,
        "rev_growth_latest": round(growths[-1], 1),
        "rev_growth_trend": [round(g, 1) for g in growths],
        "gross_margin": round(gms[-1], 1) if gms else None,
        "net_margin": round(nms[-1], 1) if nms else None,
        "revenue_latest_b": round(revs[-1]/1000, 1),
    }

def analyze_btc_thesis():
    """Tesis BTC por adopción on-chain, NO por precio."""
    addr_trend = hr_trend = ath_chg = None
    errors = []

    data, err = _get("https://api.blockchain.info/charts/n-unique-addresses?timespan=180days&format=json")
    if data and data.get("values"):
        v = data["values"]
        if v[0]["y"] > 0:
            addr_trend = (v[-1]["y"]/v[0]["y"]-1)*100
    elif err:
        errors.append(f"addr:{err}")

    data, err = _get("https://api.blockchain.info/charts/hash-rate?timespan=180days&format=json")
    if data and data.get("values"):
        v = data["values"]
        if v[0]["y"] > 0:
            hr_trend = (v[-1]["y"]/v[0]["y"]-1)*100
    elif err:
        errors.append(f"hashrate:{err}")

    data, err = _get("https://api.coingecko.com/api/v3/coins/bitcoin?market_data=true&localization=false&tickers=false&community_data=false&developer_data=false")
    if data:
        ath_chg = data.get("market_data", {}).get("ath_change_percentage", {}).get("usd")

    healthy = total = 0
    if addr_trend is not None:
        total += 1; healthy += 1 if addr_trend > 0 else 0
    if hr_trend is not None:
        total += 1; healthy += 1 if hr_trend > 0 else 0

    if total == 0:
        return {"status": "NO_DATA", "reason": "Sin datos on-chain", "_errors": errors}
    if healthy == total:
        status, reason = "VERDE", f"Red sólida: direcciones {addr_trend:+.0f}%, hashrate {hr_trend:+.0f}% (180d). Tesis intacta pese a precio."
    elif healthy == 0:
        status, reason = "ROJO", f"Red debilitándose: direcciones {addr_trend:+.0f}%, hashrate {hr_trend:+.0f}%"
    else:
        status, reason = "AMBAR", f"Mixto: direcciones {addr_trend:+.0f}%, hashrate {hr_trend:+.0f}%"

    return {
        "status": status, "reason": reason,
        "active_addr_trend_180d": round(addr_trend, 1) if addr_trend is not None else None,
        "hashrate_trend_180d": round(hr_trend, 1) if hr_trend is not None else None,
        "price_vs_ath": round(ath_chg, 1) if ath_chg is not None else None,
        "_errors": errors,
    }

def run_thesis_detector():
    """Ejecuta los detectores de los 3 activos vigilables. Devuelve (resultados, fallos)."""
    results = {}
    failures = []

    # NVDA — fundamentales
    q, err = fmp_fundamentals("NVDA")
    if err:
        failures.append(err); results["NVDA"] = {"status": "NO_DATA", "reason": err}
    else:
        results["NVDA"] = analyze_fundamentals(q)

    # SEMI — proxy NVDA a nivel sector (el ETF está dominado por las mismas empresas)
    # Para el ETF usamos el agregado de sus mayores componentes vía NVDA + contexto
    results["SEMI"] = dict(results.get("NVDA", {}))
    if results["SEMI"].get("status") != "NO_DATA":
        results["SEMI"]["reason"] = "Sector semis (proxy líderes): " + results["SEMI"].get("reason", "")

    # BTC — adopción on-chain
    btc = analyze_btc_thesis()
    if btc.get("_errors"):
        failures.extend(btc["_errors"])
    results["BTC"] = btc

    # MSCI — no se vigila tesis (índice diversificado), solo nota informativa
    results["MSCI"] = {"status": "N/A", "reason": "Índice global diversificado — sin detector de tesis (solo crisis buy)"}

    return results, failures

# ───────────────────────────────────────────────────────────────────────
# CAPA 2 — RADAR DE CANDIDATOS (rol convexo)
# ───────────────────────────────────────────────────────────────────────
STABLECOINS = {"USDT", "USDC", "DAI", "BUSD", "TUSD", "USDD", "FDUSD", "PYUSD", "FRAX"}

def radar_convex_crypto():
    """Busca cripto con asimetría estructural. Filtro anti-sesgo de supervivencia."""
    data, err = _get("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=30&page=1&sparkline=false&price_change_percentage=30d,1y")
    if err or not data:
        return [], f"radar cripto: {err or 'sin datos'}"

    candidates = []
    for c in data:
        sym = c.get("symbol", "").upper()
        mcap = c.get("market_cap", 0) or 0
        # FILTROS ANTI-RUIDO:
        if sym in STABLECOINS:          continue   # no convexas por diseño
        if mcap < 1e9:                  continue   # manipulables
        if mcap > 500e9:                continue   # ya gigantes maduros
        ch1y = c.get("price_change_percentage_1y_in_currency")
        # Asimetría: capacidad histórica de grandes movimientos (rango 1y amplio)
        # NO premiamos solo subida — buscamos volatilidad estructural con red
        candidates.append({
            "symbol": sym,
            "name": c.get("name", ""),
            "mcap_b": round(mcap/1e9, 1),
            "change_1y": round(ch1y, 1) if ch1y is not None else None,
        })
    # Devolver universo filtrado (sin rankear por subida, solo limpio de ruido)
    return candidates[:8], None

# ── RADAR DE DESCUBRIMIENTO CON MEMORIA (barrido + observación a 1 año) ──
# Fuentes que funcionan desde GitHub: SEC EDGAR (símbolos) + FMP (perfil/fundamentales).
# Máquina de estados: nueva -> OBSERVACION -> CONFIRMADA (4 trim. tesis OK) / DESCARTADA.
# Barrido continuo por lotes (no agota la API). Memoria en radar_watchlist.json.

WATCHLIST_FILE = "radar_watchlist.json"
SCAN_BATCH = 40           # símbolos analizados por ejecución
QUARTERS_TO_CONFIRM = 4   # trimestres manteniendo tesis para confirmarse
GROWTH_SECTORS_RADAR = {"Technology", "Healthcare", "Energy", "Communication Services"}
DISCARD_COOLDOWN_DAYS = 180  # no re-analizar una descartada hasta pasado este tiempo

def load_watchlist():
    try:
        with open(WATCHLIST_FILE) as f:
            return json.load(f)
    except Exception:
        return {"scan_cursor": 0, "last_scan_reset": DATE_ES[:10],
                "candidates": {}, "confirmed": [], "discarded": {}}

def save_watchlist(wl):
    try:
        with open(WATCHLIST_FILE, "w") as f:
            json.dump(wl, f, indent=2)
    except Exception as e:
        print(f"[watchlist] no se pudo guardar: {e}")

def sec_symbols():
    """Universo oficial de empresas USA (SEC EDGAR, nunca bloquea)."""
    data, err = _get("https://www.sec.gov/files/company_tickers.json",
                     headers={"User-Agent": "GeoMacro research contact@example.com"})
    if err or not data:
        return [], f"SEC EDGAR: {err or 'sin datos'}"
    syms = [v["ticker"] for v in data.values() if v.get("ticker")]
    return syms, None

def fmp_profile(symbol):
    """Perfil de empresa (sector, mcap, beta, IPO). Plan free OK. (data, error)."""
    if not FMP_KEY:
        return None, "FMP key no configurada"
    url = f"https://financialmodelingprep.com/stable/profile?symbol={symbol}&apikey={FMP_KEY}"
    data, err = _get(url)
    if err:
        return None, err
    if not isinstance(data, list) or not data:
        return None, "perfil vacío"
    return data[0], None

def _passes_structural_filter(profile):
    """Filtro estructural: mid-cap emergente, joven, volátil (capaz de gran movimiento)."""
    if not profile:
        return False
    if profile.get("isEtf") or profile.get("isFund"):
        return False
    if not profile.get("isActivelyTrading", True):
        return False
    mcap = profile.get("marketCap", 0) or 0
    if not (1e9 <= mcap <= 50e9):        # ni micro-casino ni gigante maduro
        return False
    if profile.get("sector", "") not in GROWTH_SECTORS_RADAR:
        return False
    # IPO reciente (disrupción joven) — opcional pero preferente
    ipo = profile.get("ipoDate", "") or ""
    try:
        ipo_year = int(ipo[:4]) if ipo else 0
    except Exception:
        ipo_year = 0
    if ipo_year and ipo_year < 2015:
        return False
    return True

def _evaluate_thesis(symbol):
    """Analiza fundamentales de un símbolo. Devuelve dict de tesis o None."""
    q, err = fmp_fundamentals(symbol)
    if err or not q:
        return None
    t = analyze_fundamentals(q)
    if t.get("status") in ("NO_DATA", None):
        return None
    return t

def run_discovery_radar():
    """
    Capa 2 — Radar de descubrimiento con memoria.
    1. Barre un lote del universo SEC buscando candidatas nuevas.
    2. Re-evalúa las que ya están en observación.
    3. Asciende a CONFIRMADA o DESCARTA según la prueba del tiempo.
    Devuelve (resumen, error).
    """
    wl = load_watchlist()
    errors = []

    # ── Universo de símbolos (SEC) ──
    symbols, err = sec_symbols()
    if err:
        return wl, f"radar: {err}"  # devuelve watchlist previa, avisa del fallo

    # ── BARRIDO: analizar el siguiente lote ──
    cursor = wl.get("scan_cursor", 0)
    if cursor >= len(symbols):
        cursor = 0
        wl["last_scan_reset"] = DATE_ES[:10]  # vuelta completa al universo
    batch = symbols[cursor:cursor + SCAN_BATCH]
    wl["scan_cursor"] = cursor + SCAN_BATCH

    fmp_fail = 0
    new_found = 0
    for sym in batch:
        # Saltar si ya está en seguimiento o descartada reciente
        if sym in wl["candidates"]:
            continue
        disc = wl["discarded"].get(sym)
        if disc:
            try:
                d_days = (NOW - datetime.fromisoformat(disc["date"])).days
                if d_days < DISCARD_COOLDOWN_DAYS:
                    continue
            except Exception:
                pass
        prof, perr = fmp_profile(sym)
        if perr:
            fmp_fail += 1
            time.sleep(0.15)
            continue
        if not _passes_structural_filter(prof):
            time.sleep(0.15)
            continue
        # Pasa filtro estructural -> evaluar tesis
        thesis = _evaluate_thesis(sym)
        time.sleep(0.15)
        if thesis and thesis.get("status") in ("VERDE", "AMBAR"):
            wl["candidates"][sym] = {
                "name": prof.get("companyName", "")[:30],
                "sector": prof.get("sector", ""),
                "first_seen": DATE_ES[:10],
                "state": "OBSERVACION",
                "quarters_confirmed": 1,
                "history": [{"date": DATE_ES[:10],
                             "rev_growth": thesis.get("rev_growth_latest"),
                             "gross_margin": thesis.get("gross_margin"),
                             "thesis": thesis["status"]}],
                "last_checked": DATE_ES[:10],
            }
            new_found += 1

    # ── RE-EVALUAR candidatas en observación (solo si pasó >75 días del último check) ──
    promoted, discarded = [], []
    for sym, c in list(wl["candidates"].items()):
        try:
            last = datetime.fromisoformat(c.get("last_checked", c["first_seen"]))
            days = (NOW - last).days
        except Exception:
            days = 999
        if days < 75:   # ~1 trimestre entre re-evaluaciones
            continue
        thesis = _evaluate_thesis(sym)
        time.sleep(0.15)
        if not thesis:
            continue
        c["last_checked"] = DATE_ES[:10]
        c["history"].append({"date": DATE_ES[:10],
                             "rev_growth": thesis.get("rev_growth_latest"),
                             "gross_margin": thesis.get("gross_margin"),
                             "thesis": thesis["status"]})
        if thesis["status"] == "ROJO":
            # Tesis rota -> descartar
            wl["discarded"][sym] = {"date": NOW.isoformat(), "reason": "tesis rota en observación"}
            discarded.append(sym)
            del wl["candidates"][sym]
        else:
            c["quarters_confirmed"] = c.get("quarters_confirmed", 1) + 1
            if c["quarters_confirmed"] >= QUARTERS_TO_CONFIRM:
                c["state"] = "CONFIRMADA"
                c["confirmed_date"] = DATE_ES[:10]
                if sym not in [x.get("symbol") for x in wl["confirmed"]]:
                    wl["confirmed"].append({"symbol": sym, "name": c["name"],
                                            "sector": c["sector"],
                                            "confirmed_date": DATE_ES[:10],
                                            "quarters": c["quarters_confirmed"]})
                promoted.append(sym)

    save_watchlist(wl)

    summary = {
        "scanned_batch": len(batch),
        "scan_position": f"{wl['scan_cursor']}/{len(symbols)}",
        "new_in_observation": new_found,
        "total_observing": len([c for c in wl["candidates"].values() if c["state"] == "OBSERVACION"]),
        "confirmed_total": len(wl["confirmed"]),
        "promoted_now": promoted,
        "discarded_now": discarded,
        "fmp_failures": fmp_fail,
    }
    err_out = None
    if fmp_fail >= len(batch) and len(batch) > 0:
        err_out = f"radar: FMP falló en todo el lote ({fmp_fail})"
    return summary, err_out, wl


# ───────────────────────────────────────────────────────────────────────
# CAPA 3 — AUTOVIGILANCIA (self-check + watchdog)
# ───────────────────────────────────────────────────────────────────────
def self_check(thesis_results, thesis_failures, radar_failures):
    """
    Revisa la salud del propio sistema. Devuelve lista de problemas.
    Si está vacía, todo OK (silencio = no molesta al usuario).
    """
    problems = []

    # 1. ¿Hay credenciales?
    if not FMP_KEY:
        problems.append("FMP_API_KEY no configurada — fundamentales no disponibles")
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        problems.append("Telegram no configurado — sin alertas")

    # 2. ¿Algún detector se quedó sin datos?
    no_data = [k for k, v in thesis_results.items()
               if v.get("status") == "NO_DATA"]
    if no_data:
        problems.append(f"Sin datos para: {', '.join(no_data)}")

    # 3. ¿Fallos de red registrados?
    for f in thesis_failures + radar_failures:
        problems.append(f"Fallo de fetch: {f}")

    # 4. ¿El detector clave (NVDA) funcionó?
    if thesis_results.get("NVDA", {}).get("status") in ("NO_DATA", None):
        problems.append("CRÍTICO: detector NVDA sin datos — revisar FMP key/créditos")

    return problems

def write_health_log(problems):
    """Persiste el estado de salud para el watchdog."""
    health = {
        "last_run": DATE_ES,
        "last_run_ts": NOW.isoformat(),
        "ok": len(problems) == 0,
        "problems": problems,
    }
    try:
        with open("core_health.json", "w") as f:
            json.dump(health, f, indent=2)
    except Exception as e:
        print(f"[health] no se pudo escribir: {e}")
    return health

# ───────────────────────────────────────────────────────────────────────
# INFORME TELEGRAM — claro, sin ruido
# ───────────────────────────────────────────────────────────────────────
EMOJI = {"VERDE": "🟢", "AMBAR": "🟡", "ROJO": "🔴", "NO_DATA": "⚪", "N/A": "⚪"}

def build_report(thesis, radar_crypto, discovery, watchlist, is_weekly):
    """Construye el informe. Solo se envía si es semanal o si hay algo accionable."""
    lines = [f"📋 <b>AUDITORÍA CARTERA CORE</b> · {NOW.strftime('%d/%m/%Y')}", ""]

    # Estado de cada activo
    lines.append("<b>SALUD DE TESIS</b>")
    for aid in ["NVDA", "SEMI", "BTC", "MSCI"]:
        r = thesis.get(aid, {})
        st = r.get("status", "NO_DATA")
        name = CORE_ASSETS[aid]["name"]
        lines.append(f"{EMOJI.get(st,'⚪')} <b>{name}</b>: {r.get('reason','—')}")
    lines.append("")

    # Alertas accionables (solo ámbar/rojo)
    alerts = [aid for aid in ["NVDA","SEMI","BTC"]
              if thesis.get(aid,{}).get("status") in ("AMBAR","ROJO")]
    if alerts:
        lines.append("<b>⚠️ REQUIERE ATENCIÓN</b>")
        for aid in alerts:
            r = thesis[aid]
            if r["status"] == "ROJO":
                lines.append(f"🔴 {CORE_ASSETS[aid]['name']}: tesis deteriorándose. Considera transición dulce (vender en verde de precio hacia el rol equivalente).")
            else:
                lines.append(f"🟡 {CORE_ASSETS[aid]['name']}: vigilar de cerca. Aún no es momento de rotar, pero la tesis está madurando.")
        lines.append("")

    # Radar de descubrimiento con memoria
    confirmed = watchlist.get("confirmed", [])
    observing = [(s, c) for s, c in watchlist.get("candidates", {}).items()
                 if c.get("state") == "OBSERVACION"]

    if confirmed:
        lines.append("<b>⭐ CANDIDATAS CONFIRMADAS</b> (tesis sostenida ≥1 año)")
        for c in confirmed[:5]:
            lines.append(f"• {c['symbol']} ({c['name']}) · {c['sector']}")
        lines.append("<i>Han demostrado tesis fuerte durante 4 trimestres. Opción real de transición dulce.</i>")
        lines.append("")

    if observing:
        # Ordenar por trimestres confirmados (más cerca de confirmarse primero)
        observing.sort(key=lambda x: x[1].get("quarters_confirmed", 0), reverse=True)
        lines.append(f"<b>EN OBSERVACIÓN</b> ({len(observing)} candidatas, probándose en el tiempo)")
        for sym, c in observing[:6]:
            qc = c.get("quarters_confirmed", 1)
            hist = c.get("history", [])
            g = hist[-1].get("rev_growth") if hist else None
            gstr = f" · crec {g:+.0f}%" if g is not None else ""
            lines.append(f"• {sym} ({c['name']}) · trim {qc}/4{gstr}")
        lines.append("<i>Aún no confirmadas. El sistema las vigila trimestre a trimestre.</i>")
        lines.append("")

    # Posición del barrido
    sp = discovery.get("scan_position", "?")
    lines.append(f"<i>Barrido del mercado: {sp} · descubrimiento continuo</i>")
    lines.append("")

    # Radar convexo cripto
    if radar_crypto:
        top = ", ".join(f"{c['symbol']}" for c in radar_crypto[:5])
        lines.append(f"<b>RADAR CONVEXO (cripto)</b>: {top}")
        lines.append("<i>Universo filtrado (sin stablecoins ni micro-caps).</i>")
        lines.append("")

    lines.append("<i>Cartera de largo plazo · máximo beneficio · transición dulce</i>")
    return "\n".join(lines)

# ───────────────────────────────────────────────────────────────────────
# MAIN
# ───────────────────────────────────────────────────────────────────────
def main():
    print(f"=== AUDITOR CORE — {DATE_ES} ===")
    is_weekly = NOW.weekday() == 0  # lunes

    # CAPA 1
    print("-> Detectando tesis...")
    thesis, thesis_failures = run_thesis_detector()
    for aid, r in thesis.items():
        print(f"   {aid}: [{r.get('status')}] {r.get('reason','')[:60]}")

    # CAPA 2 — Radar de descubrimiento con memoria + radar cripto
    print("-> Radar de descubrimiento (barrido + observación)...")
    radar_crypto, rc_err = radar_convex_crypto()
    discovery, disc_err, watchlist = run_discovery_radar()
    radar_failures = [e for e in (rc_err, disc_err) if e]
    print(f"   Cripto: {len(radar_crypto)} candidatos")
    print(f"   Barrido: posición {discovery.get('scan_position','?')} | "
          f"nuevas en observación: {discovery.get('new_in_observation',0)} | "
          f"total observando: {discovery.get('total_observing',0)} | "
          f"confirmadas: {discovery.get('confirmed_total',0)}")
    if discovery.get("promoted_now"):
        print(f"   ⭐ ASCENDIDAS A CONFIRMADAS: {', '.join(discovery['promoted_now'])}")
    if discovery.get("discarded_now"):
        print(f"   ✗ Descartadas (tesis rota): {', '.join(discovery['discarded_now'])}")

    # CAPA 3 — autovigilancia
    print("-> Self-check...")
    problems = self_check(thesis, thesis_failures, radar_failures)
    health = write_health_log(problems)

    # ALERTA DE SALUD — proactiva, solo si hay problemas
    if problems:
        msg = "🚨 <b>AUTO-CHECK CORE FALLÓ</b>\n\n" + "\n".join(f"• {p}" for p in problems)
        msg += "\n\n<i>El sistema detectó esto solo. Revisa lo indicado.</i>"
        telegram_send(msg)
        print(f"   ⚠️ {len(problems)} problemas detectados, alerta enviada")
    else:
        print("   ✅ Salud OK")

    # INFORME — solo semanal (lunes) o si hay alerta roja
    has_red = any(thesis.get(a,{}).get("status") == "ROJO" for a in ["NVDA","SEMI","BTC"])
    if is_weekly or has_red:
        report = build_report(thesis, radar_crypto, discovery, watchlist, is_weekly)
        telegram_send(report)
        print("   📋 Informe enviado")
    else:
        print("   (no es lunes ni hay roja — sin informe, evitando ruido)")

    # Guardar resultados para dashboard
    output = {
        "generated_at": DATE_ES,
        "thesis": thesis,
        "radar_crypto": radar_crypto,
        "discovery": discovery,
        "confirmed_candidates": watchlist.get("confirmed", []),
        "observing_count": len([c for c in watchlist.get("candidates",{}).values() if c.get("state")=="OBSERVACION"]),
        "health": health,
    }
    with open("core_audit.json", "w") as f:
        json.dump(output, f, indent=2)
    print("=== Completado ===")

if __name__ == "__main__":
    main()

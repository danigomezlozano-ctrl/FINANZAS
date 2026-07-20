# -*- coding: utf-8 -*-
"""
tjl_validation.py — GeoMacro Intel
==================================
Módulo autocontenido que implementa la OPCIÓN B de validación de TJL:

  * Estado del gate persistido en paper_trades.json (nunca solo en memoria).
  * Ronda 1 congelada con exactamente los 18 trades TJL existentes.
  * Veredicto persistente e idempotente (A_REJECT_TJL_V2 / B_SECOND_SAMPLE).
  * Ronda 2 de exactamente 20 operaciones, solo si la ronda 1 es positiva.
  * Resolutor canónico de salidas OHLC (gap, stop, target, barra ambigua
    con política stop_first).
  * Deduplicación persistente de alertas bloqueadas y de cierres.
  * Hash canónico del protocolo (bloqueante) y hash de la especificación
    de reglas (para combinar con el código fuente del engine).

Este módulo NO llama a red, NO llama a Telegram y NO conoce secretos.
analysis_engine.py debe importarlo y respetar el contrato de integración
documentado al final del archivo.
"""

from __future__ import annotations

import hashlib
import json
import os
import statistics
import tempfile
from collections import Counter
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Constantes de estrategia y protocolo
# ---------------------------------------------------------------------------

STRATEGY_NAME = "trend_joined_long_v2"
COST_BASE_PCT = 0.20  # coste base por operación (ida+vuelta), en puntos de %

TJL_RULE_SPEC = {
    "trend_filter": "previous_close_above_sma200",
    "breakout_window": 20,
    "max_extension_atr": 0.5,
    "stop_atr": 1.5,
    "target_atr": 3.0,
    "cost_base_pct": COST_BASE_PCT,
    "ambiguous_bar_policy": "stop_first",
}

TJL_VERDICT_PROTOCOL = {
    "strategy": STRATEGY_NAME,
    "round_1": {
        "target_n": 18,
        "frozen_sample": True,
        "metric": "expectancy_net_base",
        "reject_if": "expectancy_net_base <= 0",
        "verdict_reject": "A_REJECT_TJL_V2",
        "verdict_pass": "B_SECOND_SAMPLE",
    },
    "round_2": {
        "target_n": 20,
        "metric": "expectancy_net_base",
        "reject_if": "expectancy_net_base <= 0",
        "verdict_reject": "A_REJECT_TJL_V2",
        "verdict_pass": "PAPER_VALIDATION_CONTINUES",
    },
    "real_money": "forbidden_until_100_plus_trades",
    "retroactive_changes": "forbidden",
}


def canonical_json(obj) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _sha256_short(text: str, n: int = 12) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:n]


def compute_protocol_hash() -> str:
    return _sha256_short(canonical_json(TJL_VERDICT_PROTOCOL))


def compute_rules_spec_hash(extra_sources: list[str] | None = None) -> str:
    """Hash de la especificación canónica de reglas TJL.

    `extra_sources` debe contener, en la integración con analysis_engine.py,
    el código fuente (inspect.getsource) de:
      trend_joined_long_signal, trend_joined_long_trigger,
      trend_joined_long_levels, sma, atr_ohlc y la función de salida OHLC.
    Sin el engine presente, el hash cubre únicamente la especificación.
    """
    parts = [canonical_json(TJL_RULE_SPEC)]
    if extra_sources:
        parts.extend(extra_sources)
    return _sha256_short("\n".join(parts))


# Hashes esperados, fijados en código. Se verifican en cada arranque.
EXPECTED_PROTOCOL_HASH = "de855be08c78"
EXPECTED_TJL_SPEC_HASH = "33542b259852"


class TJLIntegrityError(RuntimeError):
    """Fallo bloqueante de integridad (hash, muestra, protocolo)."""


def assert_protocol_hash() -> str:
    h = compute_protocol_hash()
    if h != EXPECTED_PROTOCOL_HASH:
        raise TJLIntegrityError(
            f"Protocolo TJL modificado: hash actual {h} != esperado {EXPECTED_PROTOCOL_HASH}. "
            "Ejecución detenida; ningún archivo modificado."
        )
    return h


def assert_rules_spec_hash(extra_sources: list[str] | None = None,
                           expected: str | None = None) -> str:
    exp = expected or EXPECTED_TJL_SPEC_HASH
    h = compute_rules_spec_hash(extra_sources)
    if h != exp:
        raise TJLIntegrityError(
            f"Reglas TJL modificadas: hash actual {h} != esperado {exp}. "
            "Ejecución detenida; ningún archivo modificado."
        )
    return h


# ---------------------------------------------------------------------------
# Utilidades de tiempo y persistencia
# ---------------------------------------------------------------------------

def now_utc_str(now: datetime | None = None) -> str:
    dt = now or datetime.now(timezone.utc)
    return dt.strftime("%d/%m/%Y %H:%M UTC")


def load_paper_trades(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_paper_trades(path: str, paper_data: dict) -> None:
    """Escritura atómica: tmp + rename, para que un fallo a mitad no corrompa el libro."""
    d = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(prefix=".paper_trades_", suffix=".json", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(paper_data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


# ---------------------------------------------------------------------------
# Estado persistente del gate + migración idempotente (ronda 1)
# ---------------------------------------------------------------------------

def _tjl_trades(paper_data: dict) -> list[dict]:
    return [t for t in paper_data.get("trades", [])
            if t.get("strategy") == STRATEGY_NAME]


def ensure_pnl_net_base(trade: dict) -> None:
    """Trade cerrado con pnl_pct → pnl_net_base = pnl_pct - coste base."""
    if trade.get("status") != "open" and trade.get("pnl_pct") is not None:
        if trade.get("pnl_net_base") is None:
            trade["pnl_net_base"] = round(trade["pnl_pct"] - COST_BASE_PCT, 4)


def initialize_tjl_validation_state(paper_data: dict,
                                    now: datetime | None = None) -> dict:
    """Migración idempotente.

    - Crea el bloque persistente `tjl_validation` si no existe.
    - Congela la ronda 1 con EXACTAMENTE los 18 trades TJL existentes
      (error bloqueante si no son 18, hay duplicados o falta alguno).
    - Calcula pnl_net_base para los TJL cerrados que no lo tengan.
    - Si el estado ya existe, lo usa tal cual: nunca vuelve a `closed`
      ni recongela la muestra por reiniciar Python.
    """
    assert_protocol_hash()
    ts = now_utc_str(now)

    for t in _tjl_trades(paper_data):
        ensure_pnl_net_base(t)

    tv = paper_data.get("tjl_validation")
    if tv is not None:
        verify_tjl_validation_state(paper_data)
        _backfill_round_metadata(paper_data, tv)
        return paper_data

    tjl = _tjl_trades(paper_data)
    ids = [t["id"] for t in tjl]
    if len(ids) != len(set(ids)):
        raise TJLIntegrityError(
            f"IDs TJL duplicados en el libro: no se puede congelar la ronda 1. IDs={ids}"
        )
    if len(ids) != 18:
        raise TJLIntegrityError(
            f"La ronda 1 exige exactamente 18 trades TJL y hay {len(ids)}. "
            "Ejecución detenida; enviar alerta crítica."
        )

    n_open = sum(1 for t in tjl if t["status"] == "open")
    paper_data["tjl_validation"] = {
        "schema_version": 1,
        "strategy": STRATEGY_NAME,
        "current_round": 1,
        "gate_status": "closed",
        "gate_since": ts,
        "gate_reason": "tripwire 0W-6L: validación opción B, muestra congelada",
        "protocol_hash": EXPECTED_PROTOCOL_HASH,
        "rules_hash": EXPECTED_TJL_SPEC_HASH,
        "last_blocked_signal_alert": None,
        "rounds": {
            "1": {
                "status": "PENDING",
                "target_n": 18,
                "sample_ids": sorted(ids),
                "sample_frozen_at": ts,
                "open_at_freeze": n_open,
                "closed_at_freeze": 18 - n_open,
                "protocol_hash": EXPECTED_PROTOCOL_HASH,
                "rules_hash": EXPECTED_TJL_SPEC_HASH,
                "verdict": None,
            }
        },
    }
    _backfill_round_metadata(paper_data, paper_data["tjl_validation"])
    verify_tjl_validation_state(paper_data)
    return paper_data


def _backfill_round_metadata(paper_data: dict, tv: dict) -> None:
    """Añade metadatos de ronda a trades ya existentes sin cambiar resultados."""
    by_id = {t.get("id"): t for t in paper_data.get("trades", [])}
    for round_key, rnd in (tv.get("rounds") or {}).items():
        for trade_id in rnd.get("sample_ids") or []:
            trade = by_id.get(trade_id)
            if not trade:
                continue
            trade.setdefault("validation_round", int(round_key))
            trade.setdefault("eligible_for_validation", True)
            trade.setdefault("strategy_version", STRATEGY_NAME)
            trade.setdefault("protocol_hash", tv.get("protocol_hash", EXPECTED_PROTOCOL_HASH))
            trade.setdefault("rules_hash", tv.get("rules_hash", EXPECTED_TJL_SPEC_HASH))


def verify_tjl_validation_state(paper_data: dict) -> None:
    """Valida el estado persistente completo y rechaza trades TJL huérfanos."""
    assert_protocol_hash()
    tv = paper_data.get("tjl_validation")
    if not isinstance(tv, dict):
        raise TJLIntegrityError("Bloque tjl_validation ausente o inválido.")
    if tv.get("strategy") != STRATEGY_NAME:
        raise TJLIntegrityError("La estrategia persistida no coincide con TJL v2.")
    if tv.get("protocol_hash") != EXPECTED_PROTOCOL_HASH:
        raise TJLIntegrityError("Hash de protocolo persistido distinto del esperado.")
    if tv.get("rules_hash") != EXPECTED_TJL_SPEC_HASH:
        raise TJLIntegrityError("Hash de reglas persistido distinto del esperado.")
    current_round = str(tv.get("current_round"))
    rounds = tv.get("rounds") or {}
    if current_round not in rounds:
        raise TJLIntegrityError(f"Ronda activa {current_round} inexistente.")

    all_trade_ids = [t.get("id") for t in paper_data.get("trades", [])]
    duplicates = sorted(i for i, count in Counter(all_trade_ids).items()
                        if i is not None and count > 1)
    if duplicates:
        raise TJLIntegrityError("IDs de trade duplicados en el libro: " + ", ".join(duplicates))

    all_memberships: dict[str, str] = {}
    for round_key in sorted(rounds):
        _verify_frozen_round(paper_data, tv, round_key)
        for trade_id in rounds[round_key].get("sample_ids") or []:
            previous = all_memberships.get(trade_id)
            if previous is not None:
                raise TJLIntegrityError(
                    f"Trade {trade_id} pertenece a dos rondas: {previous} y {round_key}.")
            all_memberships[trade_id] = round_key

    tjl_ids = {t.get("id") for t in _tjl_trades(paper_data)}
    orphan = sorted(tjl_ids - set(all_memberships))
    if orphan:
        raise TJLIntegrityError(
            "Trades TJL fuera de cualquier muestra de validación: " + ", ".join(orphan))
    unknown = sorted(set(all_memberships) - tjl_ids)
    if unknown:
        raise TJLIntegrityError(
            "IDs congelados que ya no son trades TJL: " + ", ".join(unknown))


def _verify_frozen_round(paper_data: dict, tv: dict, round_key: str) -> None:
    """Comprueba integridad de una ronda congelada: protocolo intacto,
    IDs únicos, existentes y pertenecientes a la estrategia."""
    rnd = tv.get("rounds", {}).get(round_key)
    if rnd is None:
        return
    if rnd.get("protocol_hash") and rnd["protocol_hash"] != compute_protocol_hash():
        raise TJLIntegrityError(
            f"El protocolo cambió después de congelar la ronda {round_key}. "
            "Ejecución detenida; ningún trade modificado."
        )
    ids = rnd.get("sample_ids") or []
    target_n = int(rnd.get("target_n") or 0)
    if target_n <= 0:
        raise TJLIntegrityError(f"Ronda {round_key}: target_n inválido ({target_n}).")
    if len(ids) != len(set(ids)):
        raise TJLIntegrityError(f"Ronda {round_key}: IDs duplicados en la muestra congelada.")
    if round_key == "1" and len(ids) != target_n:
        raise TJLIntegrityError(
            f"Ronda 1 congelada debe tener exactamente {target_n} IDs y tiene {len(ids)}.")
    if len(ids) > target_n:
        raise TJLIntegrityError(
            f"Ronda {round_key}: {len(ids)} IDs exceden target_n={target_n}.")
    if rnd.get("sample_frozen_at") and len(ids) != target_n:
        raise TJLIntegrityError(
            f"Ronda {round_key} marcada como congelada con {len(ids)}/{target_n} IDs.")
    by_id = {t["id"]: t for t in paper_data.get("trades", [])}
    for i in ids:
        t = by_id.get(i)
        if t is None:
            raise TJLIntegrityError(f"Ronda {round_key}: ID congelado ausente del libro: {i}")
        if t.get("strategy") != STRATEGY_NAME:
            raise TJLIntegrityError(
                f"Ronda {round_key}: ID {i} no pertenece a {STRATEGY_NAME}."
            )


def get_effective_tjl_gate(paper_data: dict) -> dict:
    tv = paper_data.get("tjl_validation") or {}
    return {
        "status": tv.get("gate_status", "closed"),
        "round": tv.get("current_round", 1),
        "reason": tv.get("gate_reason", "estado no inicializado"),
        "persistent": "tjl_validation" in paper_data,
    }


def set_tjl_gate_state(paper_data: dict, status: str, reason: str,
                       now: datetime | None = None) -> None:
    tv = paper_data["tjl_validation"]
    if tv.get("gate_status") == "disabled" and status != "disabled":
        raise TJLIntegrityError("El gate está permanentemente disabled: no puede reabrirse.")
    tv["gate_status"] = status
    tv["gate_reason"] = reason
    tv["gate_since"] = now_utc_str(now)


# ---------------------------------------------------------------------------
# Entradas nuevas (solo ronda 2, gate abierto)
# ---------------------------------------------------------------------------

def can_open_tjl_trade(paper_data: dict) -> bool:
    tv = paper_data.get("tjl_validation") or {}
    return tv.get("gate_status") == "open"


def register_new_tjl_trade(paper_data: dict, trade: dict,
                           now: datetime | None = None) -> dict:
    """Registra una entrada TJL nueva. Devuelve {'accepted': bool, 'reason': str}.

    Con gate cerrado/disabled: NO añade el trade y devuelve accepted=False.
    Con gate abierto (solo posible en ronda 2): añade el trade sellado con
    metadatos de validación; al llegar exactamente a target_n cierra el gate
    y congela los IDs. El trade target_n+1 se rechaza.
    """
    tv = paper_data.get("tjl_validation")
    if tv is None:
        raise TJLIntegrityError("Estado TJL no inicializado: llamar initialize_tjl_validation_state.")

    gate = tv.get("gate_status")
    if gate != "open":
        return {"accepted": False,
                "reason": f"gate {gate}: {tv.get('gate_reason', '')}"}

    rk = str(tv.get("current_round"))
    rnd = tv["rounds"].get(rk)
    if rnd is None or rnd.get("status") == "FINAL":
        return {"accepted": False, "reason": f"ronda {rk} no acepta entradas"}

    ids = rnd.setdefault("sample_ids", [])
    if len(ids) >= rnd["target_n"]:
        set_tjl_gate_state(paper_data, "closed",
                           f"ronda {rk} completa ({rnd['target_n']} entradas)", now)
        return {"accepted": False, "reason": f"ronda {rk} ya tiene {rnd['target_n']} entradas"}

    if trade.get("id") in ids or any(t["id"] == trade.get("id")
                                     for t in paper_data["trades"]):
        return {"accepted": False, "reason": f"ID duplicado: {trade.get('id')}"}

    trade["strategy"] = STRATEGY_NAME
    trade["validation_round"] = int(rk)
    trade["eligible_for_validation"] = True
    trade["strategy_version"] = STRATEGY_NAME
    trade["rules_hash"] = tv.get("rules_hash")
    trade["protocol_hash"] = tv.get("protocol_hash")
    paper_data["trades"].append(trade)
    ids.append(trade["id"])

    if len(ids) == rnd["target_n"]:
        rnd["sample_frozen_at"] = now_utc_str(now)
        set_tjl_gate_state(paper_data, "closed",
                           f"ronda {rk} completa ({rnd['target_n']}/{rnd['target_n']}), muestra congelada",
                           now)
    return {"accepted": True, "reason": "ok"}


# ---------------------------------------------------------------------------
# Resolutor canónico de salidas OHLC
# ---------------------------------------------------------------------------

def resolve_tjl_exit(trade: dict, bar: dict) -> dict | None:
    """Decide la salida de un trade TJL long contra una barra OHLC diaria.

    Reglas canónicas (TJL_RULE_SPEC):
      1. Gap por debajo del stop → salida al open real ('stopped', gap=True).
      2. Barra que toca stop Y target → política conservadora stop_first
         ('stopped', ambiguous_bar=True).
      3. Toca solo el stop → 'stopped' al precio del stop.
      4. Toca solo el target → 'target_hit' al precio del target.
      5. Nada → None (sigue abierta).
    """
    if trade.get("status") != "open":
        return None
    stop, target = trade["stop_price"], trade["target_price"]
    o, h, l = bar["open"], bar["high"], bar["low"]

    if o <= stop:
        return {"status": "stopped", "exit_price": o, "gap": True,
                "ambiguous_bar": False}
    hit_stop = l <= stop
    hit_target = h >= target
    if hit_stop and hit_target:
        return {"status": "stopped", "exit_price": stop, "gap": False,
                "ambiguous_bar": True}
    if hit_stop:
        return {"status": "stopped", "exit_price": stop, "gap": False,
                "ambiguous_bar": False}
    if hit_target:
        return {"status": "target_hit", "exit_price": target, "gap": False,
                "ambiguous_bar": False}
    return None


def apply_exit(trade: dict, exit_info: dict, now: datetime | None = None) -> None:
    entry = trade["entry_price"]
    px = exit_info["exit_price"]
    pnl = round((px - entry) / entry * 100.0, 4)
    trade["status"] = exit_info["status"]
    trade["exit_price"] = px
    trade["exit_date"] = now_utc_str(now)
    trade["pnl_pct"] = pnl
    trade["pnl_net_base"] = round(pnl - COST_BASE_PCT, 4)
    trade["result"] = "win" if pnl > 0 else "loss"
    trade["ambiguous_bar"] = exit_info.get("ambiguous_bar", False)
    if exit_info.get("gap"):
        trade["gap_exit"] = True
    trade["close_alert_sent"] = False  # la alerta se marca solo tras envío OK


def pending_close_alerts(paper_data: dict) -> list[dict]:
    return [t for t in _tjl_trades(paper_data)
            if t.get("status") in ("stopped", "target_hit")
            and t.get("close_alert_sent") is False]


def build_close_alert_text(paper_data: dict, trade: dict) -> str:
    tv = paper_data["tjl_validation"]
    rnd_no = trade.get("validation_round", 1)
    rnd = tv["rounds"].get(str(rnd_no), {})
    sample = rnd.get("sample_ids", [])
    by_id = {t["id"]: t for t in paper_data["trades"]}
    remaining = sum(1 for i in sample if by_id.get(i, {}).get("status") == "open")
    icon = "🎯" if trade["status"] == "target_hit" else "🛑"
    resultado = "objetivo alcanzado" if trade["status"] == "target_hit" else "pérdida"
    return (
        f"{icon} CIERRE TJL — {trade['asset']}\n"
        f"Resultado: {resultado}\n"
        f"Salida: {trade['exit_price']}\n"
        f"PnL bruto: {trade['pnl_pct']:+.2f}%\n"
        f"PnL neto base: {trade['pnl_net_base']:+.2f}%\n"
        f"Ronda: {rnd_no}\n"
        f"Restantes en muestra: {remaining}"
    )


def mark_close_alert_sent(trade: dict) -> None:
    trade["close_alert_sent"] = True


# ---------------------------------------------------------------------------
# Veredicto persistente e idempotente
# ---------------------------------------------------------------------------

def evaluate_tjl_verdict(paper_data: dict,
                         now: datetime | None = None) -> tuple[dict, list[str]]:
    """Evalúa la ronda activa. Devuelve (paper_data, eventos_para_telegram).

    Idempotente: una ronda FINAL nunca se recalcula ni genera nuevos eventos.
    """
    tv = paper_data.get("tjl_validation")
    if tv is None:
        raise TJLIntegrityError("Estado TJL no inicializado.")
    events: list[str] = []
    rk = str(tv["current_round"])
    _verify_frozen_round(paper_data, tv, rk)
    rnd = tv["rounds"][rk]

    if rnd.get("status") == "FINAL":
        if not rnd.get("verdict_alert_sent", False) and rnd.get("verdict_message"):
            events.append(rnd["verdict_message"])
        return paper_data, events

    ids = rnd.get("sample_ids") or []
    by_id = {t["id"]: t for t in paper_data["trades"]}
    sample = [by_id[i] for i in ids]

    # Ronda 2 aún incompleta en entradas → sigue PENDING
    if len(sample) < rnd["target_n"]:
        rnd["status"] = "PENDING"
        return paper_data, events

    if any(t["status"] == "open" for t in sample):
        rnd["status"] = "PENDING"
        return paper_data, events

    for t in sample:
        ensure_pnl_net_base(t)
        if t.get("pnl_net_base") is None:
            raise TJLIntegrityError(
                f"Trade cerrado sin pnl_net_base: {t['id']}. Veredicto detenido.")

    expectancy = round(statistics.mean(t["pnl_net_base"] for t in sample), 4)
    ts = now_utc_str(now)

    if expectancy <= 0:
        verdict = "A_REJECT_TJL_V2"
        set_tjl_gate_state(paper_data, "disabled",
                           f"veredicto A ronda {rk}: expectancy neta {expectancy}% ≤ 0. "
                           "TJL v2 descartada.", now)
        events.append(
            f"⚖️ VEREDICTO TJL RONDA {rk}: A_REJECT_TJL_V2\n"
            f"Expectancy neta: {expectancy:+.3f}% (n={len(sample)})\n"
            f"Gate: DISABLED permanente. TJL v2 no volverá a operar."
        )
    else:
        if rk == "1":
            verdict = "B_SECOND_SAMPLE"
            tv["current_round"] = 2
            tv["rounds"]["2"] = {
                "status": "PENDING",
                "target_n": 20,
                "sample_ids": [],
                "sample_frozen_at": None,
                "protocol_hash": EXPECTED_PROTOCOL_HASH,
                "rules_hash": EXPECTED_TJL_SPEC_HASH,
                "verdict": None,
            }
            set_tjl_gate_state(paper_data, "open",
                               f"veredicto B ronda 1: expectancy neta {expectancy}% > 0. "
                               "Ronda 2 (20 trades) abierta.", now)
            events.append(
                f"⚖️ VEREDICTO TJL RONDA 1: B_SECOND_SAMPLE\n"
                f"Expectancy neta: {expectancy:+.3f}% (n={len(sample)})\n"
                f"Gate: OPEN. Ronda 2 de exactamente 20 operaciones."
            )
        else:
            verdict = "PAPER_VALIDATION_CONTINUES"
            set_tjl_gate_state(paper_data, "closed",
                               f"ronda 2 positiva ({expectancy}%): validación en papel continúa. "
                               "Dinero real prohibido hasta 100+ trades.", now)
            events.append(
                f"⚖️ VEREDICTO TJL RONDA 2: PAPER_VALIDATION_CONTINUES\n"
                f"Expectancy neta: {expectancy:+.3f}% (n={len(sample)})\n"
                f"Sigue en papel. Regla de 100+ operaciones intacta."
            )

    rnd.update({
        "status": "FINAL",
        "verdict": verdict,
        "expectancy_net_base": expectancy,
        "n_sample": len(sample),
        "decided_at": ts,
        "protocol_hash": EXPECTED_PROTOCOL_HASH,
        "rules_hash": EXPECTED_TJL_SPEC_HASH,
        "verdict_message": events[-1] if events else None,
        "verdict_alert_sent": False,
    })
    return paper_data, events


def mark_verdict_alert_sent(paper_data: dict, round_no: int | None = None,
                             message: str | None = None) -> None:
    """Marca como enviada la alerta de un veredicto FINAL.

    Cuando la ronda 1 produce B, ``current_round`` pasa a 2 antes de enviar
    Telegram. Por eso no se debe inferir siempre la ronda desde current_round:
    se localiza por mensaje o, en su defecto, la ronda FINAL pendiente más
    reciente.
    """
    tv = paper_data["tjl_validation"]
    rounds = tv.get("rounds", {})
    if round_no is not None:
        candidates = [(str(round_no), rounds.get(str(round_no)))]
    elif message is not None:
        candidates = [(k, r) for k, r in rounds.items()
                      if r.get("verdict_message") == message]
    else:
        candidates = sorted(
            ((k, r) for k, r in rounds.items()
             if r.get("status") == "FINAL" and not r.get("verdict_alert_sent", False)),
            key=lambda item: int(item[0]), reverse=True)
    for _, rnd in candidates:
        if rnd and rnd.get("status") == "FINAL":
            rnd["verdict_alert_sent"] = True
            return
    raise TJLIntegrityError("No se encontró un veredicto FINAL pendiente para marcar.")


# ---------------------------------------------------------------------------
# Alertas bloqueadas (Telegram + results.json) con deduplicación
# ---------------------------------------------------------------------------

def build_blocked_signal_text(paper_data: dict, assets: list[str]) -> str:
    gate = get_effective_tjl_gate(paper_data)
    header = ("⛔ SEÑAL TJL BLOQUEADA — NO COMPRAR"
              if gate["status"] != "disabled"
              else "⛔ TJL DESCARTADA — NO COMPRAR")
    lines = [header] + [f"• {a}" for a in assets]
    lines += ["", f"Estado del gate: {gate['status']}", f"Razón: {gate['reason']}"]
    if gate["status"] == "disabled":
        lines.append("La estrategia trend_joined_long_v2 fue descartada por veredicto A.")
    return "\n".join(lines)


def should_send_blocked_alert(paper_data: dict, assets: list[str],
                              now: datetime | None = None) -> bool:
    tv = paper_data.get("tjl_validation") or {}
    last = tv.get("last_blocked_signal_alert")
    today = (now or datetime.now(timezone.utc)).strftime("%d/%m/%Y")
    return not (last and last.get("date") == today
                and last.get("assets") == sorted(assets))


def record_blocked_alert(paper_data: dict, assets: list[str],
                         now: datetime | None = None) -> None:
    today = (now or datetime.now(timezone.utc)).strftime("%d/%m/%Y")
    paper_data["tjl_validation"]["last_blocked_signal_alert"] = {
        "date": today, "assets": sorted(assets)}


def build_blocked_alert_entry(paper_data: dict, asset: str) -> dict:
    gate = get_effective_tjl_gate(paper_data)
    return {
        "type": "SIGNAL_BLOCKED_BY_GATE",
        "asset": asset,
        "actionable": False,
        "execution_status": "BLOCKED",
        "gate_status": gate["status"],
        "reason": gate["reason"],
    }


# ---------------------------------------------------------------------------
# Snapshot para results.json
# ---------------------------------------------------------------------------

def build_results_snapshot(paper_data: dict) -> dict:
    tv = paper_data.get("tjl_validation") or {}
    gate = get_effective_tjl_gate(paper_data)
    rk = str(tv.get("current_round", 1))
    rnd = (tv.get("rounds") or {}).get(rk, {})
    ids = rnd.get("sample_ids") or []
    by_id = {t["id"]: t for t in paper_data.get("trades", [])}
    sample = [by_id[i] for i in ids if i in by_id]
    closed = [t for t in sample if t["status"] != "open"]
    for t in closed:
        ensure_pnl_net_base(t)
    exp_so_far = (round(statistics.mean(t["pnl_net_base"] for t in closed), 4)
                  if closed else None)
    verdict = None
    if rnd.get("status") == "FINAL":
        verdict = {
            "status": "FINAL",
            "verdict": rnd["verdict"],
            "expectancy_net_base": rnd["expectancy_net_base"],
            "n_sample": rnd["n_sample"],
            "decided_at": rnd["decided_at"],
        }
    return {
        "tjl_entry_gate": gate,
        "tjl_validation": {
            "current_round": tv.get("current_round"),
            "sample_target": rnd.get("target_n"),
            "sample_count": len(ids),
            "closed": len(closed),
            "open": len(sample) - len(closed),
            "expectancy_net_base_so_far": exp_so_far,
            "status": rnd.get("status"),
            "rules_hash": tv.get("rules_hash"),
            "protocol_hash": tv.get("protocol_hash"),
        },
        "tjl_verdict": verdict,
    }


# ---------------------------------------------------------------------------
# CONTRATO DE INTEGRACIÓN con analysis_engine.py
# ---------------------------------------------------------------------------
# 1. Arranque, antes de descargar datos o tocar archivos:
#      assert_protocol_hash()
#      assert_rules_spec_hash(extra_sources=[inspect.getsource(f) for f in
#          (trend_joined_long_signal, trend_joined_long_trigger,
#           trend_joined_long_levels, sma, atr_ohlc, <función_salida_OHLC>)],
#          expected=EXPECTED_TJL_HASH_ENGINE)   # recalcular UNA vez al integrar
#      paper = load_paper_trades(path)
#      paper = initialize_tjl_validation_state(paper)
#      save_paper_trades(path, paper)
#
# 2. Señales: si can_open_tjl_trade(paper) es False → NO crear trade;
#    publicar build_blocked_alert_entry(...) en results.json y, si
#    should_send_blocked_alert(...), enviar build_blocked_signal_text(...)
#    y record_blocked_alert(...). Nunca publicar type=BUY_SIGNAL con gate
#    closed/disabled. Entradas nuevas SOLO vía register_new_tjl_trade(...).
#
# 3. Cierres: para cada trade open, resolve_tjl_exit(trade, barra_diaria);
#    si devuelve dict → apply_exit(...). Tras save_paper_trades, enviar una
#    alerta por trade en pending_close_alerts(...) y mark_close_alert_sent
#    SOLO si Telegram respondió OK; volver a guardar.
#
# 4. Orden de guardado (crítico):
#      paper = update_paper_trades(...)
#      paper, events = evaluate_tjl_verdict(paper)
#      save_paper_trades(path, paper)          # veredicto persistido ANTES
#      enviar events por Telegram              # de cualquier módulo frágil
#      try:
#          paper = run_shadow_books(paper); save_paper_trades(path, paper)
#      except Exception: alertar sin perder el veredicto ya guardado.
#
# 5. TJL independiente del autodidacta: reputation/threshold_adj/WR pueden
#    calcularse pero con "used_for_decision": False; jamás filtran señales TJL.
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("protocol_hash:", compute_protocol_hash())
    print("rules_spec_hash:", compute_rules_spec_hash())

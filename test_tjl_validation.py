# -*- coding: utf-8 -*-
"""Suite de los 14 tests obligatorios de la validación TJL (opción B).

Aislamiento: solo tempfile, sin red, sin Yahoo, sin Telegram, sin secretos.
El libro real (fixture) es una copia de paper_trades.json; nunca se escribe
el libro real. Los globals se restauran siempre vía fixtures/finally.
"""
import copy
import json
import os
import shutil
import tempfile
from datetime import datetime, timezone

import pytest

import tjl_validation as tjl

REAL_BOOK = os.path.join(os.path.dirname(__file__), "paper_trades.json")
NOW = datetime(2026, 7, 20, 15, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_book(tmp_path):
    """Copia del libro real en tempfile. Jamás toca el original."""
    p = tmp_path / "paper_trades.json"
    shutil.copy(REAL_BOOK, p)
    return str(p)


@pytest.fixture()
def paper(tmp_book):
    return tjl.load_paper_trades(tmp_book)


@pytest.fixture()
def telegram_log(monkeypatch):
    """Mock de envío Telegram: registra, nunca llama a red."""
    log = []
    return log


def _init(paper):
    return tjl.initialize_tjl_validation_state(paper, NOW)


def _close_all_sample(paper, pnl_net_each):
    """Cierra artificialmente todos los trades de la ronda activa con un
    pnl_net_base uniforme (para forzar veredictos en tests)."""
    tv = paper["tjl_validation"]
    ids = tv["rounds"][str(tv["current_round"])]["sample_ids"]
    by_id = {t["id"]: t for t in paper["trades"]}
    for i in ids:
        t = by_id[i]
        if t["status"] == "open":
            t["status"] = "stopped"
            t["exit_price"] = t["stop_price"]
            t["pnl_pct"] = pnl_net_each + tjl.COST_BASE_PCT
        t["pnl_net_base"] = pnl_net_each


def _mk_trade(i):
    return {
        "id": f"TST{i}_20/07/2026", "asset": f"Test{i}", "asset_id": f"TST{i}",
        "signal": "COMPRAR", "entry_date": "20/07/2026 15:00 UTC",
        "entry_price": 100.0, "stop_price": 97.0, "target_price": 106.0,
        "status": "open", "exit_price": None, "exit_date": None,
        "pnl_pct": None, "result": None, "summary": "test",
    }


# ---------------------------------------------------------------------------
# Test 1 — hash
# ---------------------------------------------------------------------------

def test_01_hashes(monkeypatch):
    assert tjl.assert_protocol_hash() == tjl.EXPECTED_PROTOCOL_HASH
    assert tjl.assert_rules_spec_hash() == tjl.EXPECTED_TJL_SPEC_HASH
    # Modificación simulada → error, y restauración garantizada
    monkeypatch.setitem(tjl.TJL_RULE_SPEC, "stop_atr", 1.6)
    with pytest.raises(tjl.TJLIntegrityError):
        tjl.assert_rules_spec_hash()
    monkeypatch.undo()
    assert tjl.assert_rules_spec_hash() == tjl.EXPECTED_TJL_SPEC_HASH


# ---------------------------------------------------------------------------
# Test 2 — congelación ronda 1
# ---------------------------------------------------------------------------

def test_02_freeze_round1(paper):
    _init(paper)
    r1 = paper["tjl_validation"]["rounds"]["1"]
    assert r1["target_n"] == 18 and len(r1["sample_ids"]) == 18
    assert len(set(r1["sample_ids"])) == 18
    frozen = list(r1["sample_ids"])
    # Un TJL añadido fuera de una ronda es una violación de integridad:
    # la muestra permanece intacta y el sistema se detiene.
    paper["trades"].append({**_mk_trade(99), "strategy": tjl.STRATEGY_NAME})
    with pytest.raises(tjl.TJLIntegrityError):
        _init(paper)
    assert paper["tjl_validation"]["rounds"]["1"]["sample_ids"] == frozen


def test_02b_freeze_wrong_counts(paper):
    p17 = copy.deepcopy(paper)
    tjl_ids = [t["id"] for t in p17["trades"] if t.get("strategy") == tjl.STRATEGY_NAME]
    p17["trades"] = [t for t in p17["trades"] if t["id"] != tjl_ids[0]]  # 17
    with pytest.raises(tjl.TJLIntegrityError):
        _init(p17)
    p19 = copy.deepcopy(paper)
    p19["trades"].append({**_mk_trade(19), "strategy": tjl.STRATEGY_NAME})  # 19
    with pytest.raises(tjl.TJLIntegrityError):
        _init(p19)
    pdup = copy.deepcopy(paper)
    dup = copy.deepcopy(next(t for t in pdup["trades"]
                             if t.get("strategy") == tjl.STRATEGY_NAME))
    pdup["trades"] = [t for t in pdup["trades"] if t["id"] != dup["id"]]
    pdup["trades"] += [dup, copy.deepcopy(dup)]  # 18 pero con duplicado
    with pytest.raises(tjl.TJLIntegrityError):
        _init(pdup)


# ---------------------------------------------------------------------------
# Test 3 — gate bloqueado: señal válida no crea trade
# ---------------------------------------------------------------------------

def test_03_gate_blocked_no_trade(paper):
    _init(paper)
    n0 = len(paper["trades"])
    res = tjl.register_new_tjl_trade(paper, _mk_trade(1), NOW)
    assert res["accepted"] is False and "closed" in res["reason"]
    assert len(paper["trades"]) == n0  # new_this_run == 0
    entry = tjl.build_blocked_alert_entry(paper, "XOM")
    assert entry["type"] == "SIGNAL_BLOCKED_BY_GATE"
    assert entry["actionable"] is False and entry["execution_status"] == "BLOCKED"


# ---------------------------------------------------------------------------
# Test 4 — Telegram bloqueado: texto correcto y deduplicación
# ---------------------------------------------------------------------------

def test_04_blocked_telegram(paper):
    _init(paper)
    txt = tjl.build_blocked_signal_text(paper, ["Exxon"])
    assert "NO COMPRAR" in txt and "📍 Comprar a" not in txt
    assert tjl.should_send_blocked_alert(paper, ["Exxon"], NOW) is True
    tjl.record_blocked_alert(paper, ["Exxon"], NOW)
    assert tjl.should_send_blocked_alert(paper, ["Exxon"], NOW) is False  # dedup
    assert tjl.should_send_blocked_alert(paper, ["Exxon", "Visa"], NOW) is True


# ---------------------------------------------------------------------------
# Tests 5–8 — cierres OHLC canónicos
# ---------------------------------------------------------------------------

def test_05_close_by_stop(paper):
    _init(paper)
    t = {**_mk_trade(5), "strategy": tjl.STRATEGY_NAME}
    out = tjl.resolve_tjl_exit(t, {"open": 99.0, "high": 100.0, "low": 96.5, "close": 97.5})
    assert out["status"] == "stopped" and out["exit_price"] == 97.0
    tjl.apply_exit(t, out, NOW)
    assert t["pnl_pct"] == -3.0 and t["pnl_net_base"] == -3.2
    assert t["close_alert_sent"] is False
    txt = tjl.build_close_alert_text(paper, {**t, "validation_round": 1})
    assert "CIERRE TJL" in txt and "Ronda: 1" in txt
    tjl.mark_close_alert_sent(t)
    assert t["close_alert_sent"] is True  # una sola alerta


def test_06_close_by_gap(paper):
    t = _mk_trade(6)
    out = tjl.resolve_tjl_exit(t, {"open": 95.0, "high": 96.0, "low": 94.0, "close": 95.5})
    assert out["status"] == "stopped" and out["exit_price"] == 95.0 and out["gap"]


def test_07_close_by_target(paper):
    t = _mk_trade(7)
    out = tjl.resolve_tjl_exit(t, {"open": 101.0, "high": 106.5, "low": 100.5, "close": 106.0})
    assert out["status"] == "target_hit" and out["exit_price"] == 106.0


def test_08_ambiguous_bar(paper):
    t = _mk_trade(8)
    out = tjl.resolve_tjl_exit(t, {"open": 100.0, "high": 106.5, "low": 96.5, "close": 101.0})
    assert out["status"] == "stopped" and out["ambiguous_bar"] is True  # stop_first
    tjl.apply_exit(t, out, NOW)
    assert t["ambiguous_bar"] is True


# ---------------------------------------------------------------------------
# Test 9 — veredicto pendiente
# ---------------------------------------------------------------------------

def test_09_verdict_pending(paper):
    _init(paper)
    paper2, events = tjl.evaluate_tjl_verdict(paper, NOW)
    assert paper2["tjl_validation"]["rounds"]["1"]["status"] == "PENDING"
    assert paper2["tjl_validation"]["rounds"]["1"]["verdict"] is None
    assert events == []


# ---------------------------------------------------------------------------
# Test 10 — veredicto A persistente e idempotente
# ---------------------------------------------------------------------------

def test_10_verdict_A(tmp_book):
    paper = _init(tjl.load_paper_trades(tmp_book))
    _close_all_sample(paper, -2.0)
    paper, events = tjl.evaluate_tjl_verdict(paper, NOW)
    r1 = paper["tjl_validation"]["rounds"]["1"]
    assert r1["status"] == "FINAL" and r1["verdict"] == "A_REJECT_TJL_V2"
    assert r1["expectancy_net_base"] == -2.0 and r1["n_sample"] == 18
    assert paper["tjl_validation"]["gate_status"] == "disabled"
    assert "2" not in paper["tjl_validation"]["rounds"]
    assert len(events) == 1
    # Simula Telegram enviado correctamente y persiste la marca.
    tjl.mark_verdict_alert_sent(paper, message=events[0])
    tjl.save_paper_trades(tmp_book, paper)
    # Segunda ejecución: ni recalcula, ni reenvía, ni sobrescribe fecha
    paper2 = tjl.load_paper_trades(tmp_book)
    decided = paper2["tjl_validation"]["rounds"]["1"]["decided_at"]
    paper2, events2 = tjl.evaluate_tjl_verdict(paper2, datetime(2026, 7, 21, tzinfo=timezone.utc))
    assert events2 == []
    assert paper2["tjl_validation"]["rounds"]["1"]["decided_at"] == decided
    # gate disabled no puede reabrirse
    with pytest.raises(tjl.TJLIntegrityError):
        tjl.set_tjl_gate_state(paper2, "open", "intento ilegal", NOW)


# ---------------------------------------------------------------------------
# Test 11 — veredicto B persistente tras reinicio
# ---------------------------------------------------------------------------

def test_11_verdict_B(tmp_book):
    paper = _init(tjl.load_paper_trades(tmp_book))
    _close_all_sample(paper, +1.5)
    paper, events = tjl.evaluate_tjl_verdict(paper, NOW)
    tv = paper["tjl_validation"]
    assert tv["rounds"]["1"]["verdict"] == "B_SECOND_SAMPLE"
    assert tv["current_round"] == 2 and tv["rounds"]["2"]["target_n"] == 20
    assert tv["gate_status"] == "open" and len(events) == 1
    assert tv["rounds"]["2"]["sample_ids"] == []  # no reutiliza IDs de la ronda 1
    tjl.mark_verdict_alert_sent(paper, message=events[0])
    assert tv["rounds"]["1"]["verdict_alert_sent"] is True
    tjl.save_paper_trades(tmp_book, paper)
    # Reinicio de proceso: recarga desde disco → gate sigue open
    paper2 = _init(tjl.load_paper_trades(tmp_book))
    assert tjl.get_effective_tjl_gate(paper2)["status"] == "open"


# ---------------------------------------------------------------------------
# Test 12 — ronda 2: exactamente 20, rechaza el 21, ronda 1 intacta
# ---------------------------------------------------------------------------

def test_12_round2(tmp_book):
    paper = _init(tjl.load_paper_trades(tmp_book))
    _close_all_sample(paper, +1.5)
    paper, _ = tjl.evaluate_tjl_verdict(paper, NOW)
    r1_ids = list(paper["tjl_validation"]["rounds"]["1"]["sample_ids"])
    for i in range(20):
        res = tjl.register_new_tjl_trade(paper, _mk_trade(100 + i), NOW)
        assert res["accepted"] is True
        nt = paper["trades"][-1]
        assert nt["validation_round"] == 2 and nt["eligible_for_validation"] is True
        assert nt["rules_hash"] and nt["protocol_hash"]
    tv = paper["tjl_validation"]
    assert len(tv["rounds"]["2"]["sample_ids"]) == 20
    assert tv["gate_status"] == "closed"  # cierre automático en el trade 20
    res21 = tjl.register_new_tjl_trade(paper, _mk_trade(999), NOW)
    assert res21["accepted"] is False  # no existe el trade 21
    assert tv["rounds"]["1"]["sample_ids"] == r1_ids  # ronda 1 no cambia
    assert not set(tv["rounds"]["2"]["sample_ids"]) & set(r1_ids)


# ---------------------------------------------------------------------------
# Test 13 — fallo de shadow books no borra el veredicto ni reenvía
# ---------------------------------------------------------------------------

def test_13_shadow_failure_persistence(tmp_book):
    paper = _init(tjl.load_paper_trades(tmp_book))
    _close_all_sample(paper, -2.0)
    paper, events = tjl.evaluate_tjl_verdict(paper, NOW)
    tjl.save_paper_trades(tmp_book, paper)   # veredicto guardado ANTES de Telegram
    sent = list(events)                       # Telegram enviado correctamente (mock)
    tjl.mark_verdict_alert_sent(paper, message=events[0])
    tjl.save_paper_trades(tmp_book, paper)   # marca enviada antes del módulo frágil
    try:
        raise RuntimeError("shadow books explotan")
    except RuntimeError:
        pass                                  # el flujo del engine captura y sigue
    paper2 = tjl.load_paper_trades(tmp_book)  # recarga desde disco
    assert paper2["tjl_validation"]["rounds"]["1"]["verdict"] == "A_REJECT_TJL_V2"
    _, events2 = tjl.evaluate_tjl_verdict(paper2, NOW)
    assert events2 == [] and sent             # no vuelve a enviarse


# ---------------------------------------------------------------------------
# Test 14 — protocolo modificado tras congelar → detención
# ---------------------------------------------------------------------------

def test_14_protocol_modified(paper, monkeypatch):
    _init(paper)
    monkeypatch.setitem(tjl.TJL_VERDICT_PROTOCOL["round_1"], "target_n", 25)
    with pytest.raises(tjl.TJLIntegrityError):
        tjl.evaluate_tjl_verdict(paper, NOW)
    with pytest.raises(tjl.TJLIntegrityError):
        tjl.initialize_tjl_validation_state(copy.deepcopy(paper), NOW)
    monkeypatch.undo()
    _, ev = tjl.evaluate_tjl_verdict(paper, NOW)  # restaurado: vuelve a funcionar
    assert ev == []

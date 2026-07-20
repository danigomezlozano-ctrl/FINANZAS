# -*- coding: utf-8 -*-
"""Pruebas de integración entre analysis_engine.py y tjl_validation.py.

Sin red y sin Telegram real. Todos los libros se copian a tempfile.
"""
from __future__ import annotations

import copy
import datetime
import json
from pathlib import Path

import pytest

import analysis_engine as eng
import tjl_validation as tjl

BASE = Path(__file__).resolve().parent
REAL_BOOK = BASE / "paper_trades.json"


def _load_book() -> dict:
    return json.loads(REAL_BOOK.read_text(encoding="utf-8"))


def _signal(asset_id: str = "XOM", name: str = "Exxon") -> dict:
    return {
        "meta": {"id": asset_id, "name": name, "type": "energy"},
        "quant": {
            "price": 100.0,
            "levels": {
                "entry": 100.0,
                "stop": 97.0,
                "target": 106.0,
                "stop_pct": 3.0,
                "target_pct": 6.0,
                "rr": 2.0,
            },
            "atr": 2.0,
            "composite_score": 75,
        },
        "analysis": {
            "calibration": {
                "signal": "COMPRAR",
                "strategy": tjl.STRATEGY_NAME,
                "source": "trend_joined_long",
                "trigger": 99.0,
                "evidence": {"hist_wr_pct": 44, "n_oos": 124},
                "summary": "Ruptura de prueba",
                "conviction": "provisional",
            }
        },
        "log": {"audit_passed": True},
    }


def _neutral_result(asset_id: str, name: str, price: float) -> dict:
    return {
        "meta": {"id": asset_id, "name": name, "type": "energy"},
        "quant": {"price": price, "levels": None, "composite_score": 50},
        "analysis": {"calibration": {"signal": "ESPERAR", "strategy": tjl.STRATEGY_NAME}},
        "log": {"audit_passed": True},
    }


def test_engine_hash_is_blocking(monkeypatch):
    assert eng._tjl_engine_rules_hash() == eng.EXPECTED_TJL_ENGINE_HASH
    assert eng._assert_tjl_integrity() == eng.EXPECTED_TJL_ENGINE_HASH
    monkeypatch.setitem(tjl.TJL_RULE_SPEC, "stop_atr", 1.6)
    with pytest.raises(tjl.TJLIntegrityError):
        eng._assert_tjl_integrity()


def test_engine_gate_blocks_new_trade(monkeypatch):
    paper = _load_book()
    before = len(paper["trades"])
    monkeypatch.setattr(eng, "OHLC_BARS", {})
    monkeypatch.setattr(eng, "OHLC_TODAY", {})
    updated, runtime = eng.update_paper_trades([_signal()], paper)
    assert len(updated["trades"]) == before
    assert runtime["accepted_trade_ids"] == []
    assert runtime["blocked_assets"] == ["Exxon"]
    assert tjl.get_effective_tjl_gate(updated)["status"] == "closed"
    tjl.verify_tjl_validation_state(updated)


def test_blocked_telegram_is_non_actionable_and_deduplicated(tmp_path, monkeypatch):
    paper = _load_book()
    paper, runtime = eng.update_paper_trades([_signal()], paper)
    out = tmp_path / "paper_trades.json"
    tjl.save_paper_trades(str(out), paper)
    monkeypatch.setattr(eng, "PAPER_FILE", str(out))
    sent: list[tuple[str, str]] = []

    def fake_send(message: str, level: str = "info") -> bool:
        sent.append((message, level))
        return True

    monkeypatch.setattr(eng, "send_telegram", fake_send)
    eng.send_tjl_notifications(paper, runtime, [])
    assert len(sent) == 1
    assert "NO COMPRAR" in sent[0][0]
    assert "Comprar a" not in sent[0][0]
    # Misma señal y mismo día: no se repite.
    eng.send_tjl_notifications(paper, runtime, [])
    assert len(sent) == 1


def test_engine_resolves_exit_using_actual_bar_date(monkeypatch):
    paper = _load_book()
    trade = next(t for t in paper["trades"]
                 if t.get("strategy") == tjl.STRATEGY_NAME and t.get("status") == "open")
    asset_id = trade["asset_id"]
    entry_day = eng._parse_trade_day(trade["entry_date"])
    assert entry_day is not None
    # Usa una fecha posterior tanto a la entrada como a la última barra ya
    # evaluada. El libro real avanza en cada ejecución, por lo que una fecha
    # fija terminaría quedándose obsoleta y el motor la ignoraría correctamente.
    last_day = trade.get("last_evaluated_bar_date")
    base_day = max(day for day in (entry_day, last_day) if day)
    bar_day = (datetime.date.fromisoformat(base_day) + datetime.timedelta(days=1)).isoformat()
    assert bar_day > entry_day
    if last_day:
        assert bar_day > last_day
    stop = trade["stop_price"]
    monkeypatch.setattr(eng, "OHLC_BARS", {
        asset_id: [{"date": bar_day, "open": stop + 1.0, "high": stop + 2.0,
                    "low": stop - 0.5, "close": stop + 0.2}]
    })
    monkeypatch.setattr(eng, "OHLC_TODAY", {})
    result = _neutral_result(asset_id, trade["asset"], stop + 0.2)
    updated, runtime = eng.update_paper_trades([result], paper)
    closed = next(t for t in updated["trades"] if t["id"] == trade["id"])
    assert closed["status"] == "stopped"
    assert closed["exit_bar_date"] == bar_day
    expected_exit_day = datetime.date.fromisoformat(bar_day).strftime("%d/%m/%Y")
    assert closed["exit_date"].startswith(expected_exit_day)
    assert closed["pnl_net_base"] == pytest.approx(closed["pnl_pct"] - tjl.COST_BASE_PCT)
    assert closed["id"] in runtime["closed_trade_ids"]
    assert closed["close_alert_sent"] is False


def test_verdict_b_notification_marks_round_one(monkeypatch):
    paper = tjl.initialize_tjl_validation_state(_load_book())
    ids = paper["tjl_validation"]["rounds"]["1"]["sample_ids"]
    by_id = {t["id"]: t for t in paper["trades"]}
    for trade_id in ids:
        trade = by_id[trade_id]
        trade["status"] = "target_hit"
        trade["pnl_pct"] = 1.2
        trade["pnl_net_base"] = 1.0
        trade["result"] = "win"
    paper, events = tjl.evaluate_tjl_verdict(paper)
    assert paper["tjl_validation"]["current_round"] == 2
    assert len(events) == 1
    sent = []
    monkeypatch.setattr(eng, "send_telegram", lambda message, level="info": sent.append(message) or True)
    monkeypatch.setattr(eng, "save_paper_trades", lambda data: None)
    eng.send_tjl_notifications(paper, {"blocked_assets": [], "accepted_trade_ids": []}, events)
    assert sent == events
    assert paper["tjl_validation"]["rounds"]["1"]["verdict_alert_sent"] is True
    assert paper["tjl_validation"]["rounds"]["2"].get("verdict_alert_sent") is not True


def test_full_orchestration_publishes_blocked_not_buy(tmp_path, monkeypatch):
    """Ejecuta run() sin red y comprueba la coherencia gate/results/Telegram."""
    paper_path = tmp_path / "paper_trades.json"
    paper_path.write_text(REAL_BOOK.read_text(encoding="utf-8"), encoding="utf-8")
    fake_engine_path = tmp_path / "analysis_engine.py"
    fake_engine_path.write_text("# synthetic path", encoding="utf-8")

    monkeypatch.setattr(eng, "PAPER_FILE", str(paper_path))
    monkeypatch.setattr(eng, "__file__", str(fake_engine_path))
    monkeypatch.setattr(eng, "FRED_KEY", "")
    monkeypatch.setattr(eng, "KAHNEMAN_ENABLED", False)
    monkeypatch.setattr(eng, "ANTHROPIC_KEY", "")
    monkeypatch.setattr(eng, "fetch_fx", lambda: {})
    monkeypatch.setattr(eng, "fetch_eia", lambda: None)
    monkeypatch.setattr(eng, "fetch_yahoo_ohlc", lambda *args, **kwargs: None)
    monkeypatch.setattr(eng, "run_trading_module", lambda *args, **kwargs: [_signal()])
    monkeypatch.setattr(eng, "run_shadow_books", lambda paper: paper)
    monkeypatch.setattr(eng, "run_backtest_module", lambda: [])
    sent: list[str] = []
    monkeypatch.setattr(eng, "send_telegram", lambda message, level="info": sent.append(message) or True)

    before = len(_load_book()["trades"])
    eng.run()
    results = json.loads((tmp_path / "results.json").read_text(encoding="utf-8"))
    after_paper = json.loads(paper_path.read_text(encoding="utf-8"))
    alert_types = [a["type"] for a in results["alerts"]]
    assert "SIGNAL_BLOCKED_BY_GATE" in alert_types
    assert "BUY_SIGNAL" not in alert_types
    assert len(after_paper["trades"]) == before
    assert results["tjl_entry_gate"]["status"] == "closed"
    assert any("NO COMPRAR" in message for message in sent)
    assert not any("📍 Comprar a" in message for message in sent)

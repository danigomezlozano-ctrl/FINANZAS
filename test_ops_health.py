from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from pathlib import Path

import ops_health
import tjl_validation as tjl


NOW = datetime(2026, 7, 20, 20, 0, tzinfo=timezone.utc)


def make_paper() -> dict:
    trades = []
    for index in range(18):
        trade_id = f"T{index:02d}"
        closed = index < 6
        trades.append(
            {
                "id": trade_id,
                "strategy": tjl.STRATEGY_NAME,
                "asset": f"Asset {index}",
                "asset_id": f"A{index}",
                "signal": "COMPRAR",
                "entry_price": 100.0,
                "stop_price": 95.0,
                "target_price": 110.0,
                "status": "stopped" if closed else "open",
                "result": "loss" if closed else None,
                "pnl_pct": -5.0 if closed else None,
                "pnl_net_base": -5.2 if closed else None,
            }
        )
    paper = {
        "trades": trades,
        "stats": {
            "total_signals": 18,
            "open": 12,
            "closed": 6,
            "wins": 0,
            "losses": 6,
            "last_updated": "20/07/2026 19:30 UTC",
            "strategy_breakdown": {
                tjl.STRATEGY_NAME: 6,
                "legacy_momentum_pullback": 0,
                "unknown": 0,
            },
        },
        "tjl_validation": {
            "schema_version": 1,
            "strategy": tjl.STRATEGY_NAME,
            "current_round": 1,
            "gate_status": "closed",
            "gate_since": "20/07/2026 10:00 UTC",
            "gate_reason": "test",
            "protocol_hash": tjl.EXPECTED_PROTOCOL_HASH,
            "rules_hash": tjl.EXPECTED_TJL_SPEC_HASH,
            "last_blocked_signal_alert": None,
            "rounds": {
                "1": {
                    "status": "PENDING",
                    "target_n": 18,
                    "sample_ids": [f"T{i:02d}" for i in range(18)],
                    "sample_frozen_at": "20/07/2026 10:00 UTC",
                    "open_at_freeze": 12,
                    "closed_at_freeze": 6,
                    "protocol_hash": tjl.EXPECTED_PROTOCOL_HASH,
                    "rules_hash": tjl.EXPECTED_TJL_SPEC_HASH,
                    "verdict": None,
                }
            },
        },
    }
    for trade in trades:
        trade["validation_round"] = 1
        trade["eligible_for_validation"] = True
        trade["strategy_version"] = tjl.STRATEGY_NAME
        trade["protocol_hash"] = tjl.EXPECTED_PROTOCOL_HASH
        trade["rules_hash"] = tjl.EXPECTED_TJL_SPEC_HASH
    return paper


def make_results(paper: dict) -> dict:
    trading = [
        {
            "meta": {"id": f"A{i}", "name": f"Asset {i}"},
            "quant": {"price": 100 + i},
            "analysis": {"calibration": {"signal": "ESPERAR"}},
        }
        for i in range(40)
    ]
    return {
        "generated_at": "20/07/2026 19:30 UTC",
        "live_mode": False,
        "is_weekly": False,
        "strategy_freeze": {"frozen": True},
        "trading": trading,
        "integrity": {"ok": True, "problems": []},
        "shadow_health": {"status": "OK", "error": None},
        "fred_health": {"status": "OK", "missing_series": []},
        "macro": {"eia_crude": {"latest": 1}},
        "alerts": [],
        "paper_trading": copy.deepcopy(paper),
        "backtesting": [],
    }


def write_fixture(tmp_path: Path, paper: dict, results: dict) -> tuple[Path, Path]:
    paper_path = tmp_path / "paper_trades.json"
    results_path = tmp_path / "results.json"
    paper_path.write_text(json.dumps(paper), encoding="utf-8")
    results_path.write_text(json.dumps(results), encoding="utf-8")
    return paper_path, results_path


def test_healthy_run_passes(tmp_path: Path):
    paper = make_paper()
    results = make_results(paper)
    paper_path, results_path = write_fixture(tmp_path, paper, results)
    report = ops_health.audit(results_path, paper_path, max_age_hours=8, now=NOW)
    assert report.critical == []
    assert report.status == "OK"
    assert report.metrics["trading_assets_ok"] == 40


def test_stale_run_fails(tmp_path: Path):
    paper = make_paper()
    results = make_results(paper)
    results["generated_at"] = "18/07/2026 10:00 UTC"
    paper["stats"]["last_updated"] = "18/07/2026 10:00 UTC"
    results["paper_trading"] = copy.deepcopy(paper)
    paper_path, results_path = write_fixture(tmp_path, paper, results)
    report = ops_health.audit(results_path, paper_path, max_age_hours=8, now=NOW)
    assert any("obsoletos" in item for item in report.critical)


def test_closed_gate_rejects_actionable_buy(tmp_path: Path):
    paper = make_paper()
    results = make_results(paper)
    results["alerts"] = [
        {"type": "BUY_SIGNAL", "asset": "Chevron", "actionable": True}
    ]
    paper_path, results_path = write_fixture(tmp_path, paper, results)
    report = ops_health.audit(results_path, paper_path, max_age_hours=8, now=NOW)
    assert any("BUY_SIGNAL" in item for item in report.critical)


def test_inconsistent_stats_fail(tmp_path: Path):
    paper = make_paper()
    paper["stats"]["open"] = 99
    results = make_results(paper)
    paper_path, results_path = write_fixture(tmp_path, paper, results)
    report = ops_health.audit(results_path, paper_path, max_age_hours=8, now=NOW)
    assert any("Stats incoherentes" in item for item in report.critical)


def test_fred_degradation_is_warning_not_critical(tmp_path: Path):
    paper = make_paper()
    results = make_results(paper)
    results["fred_health"] = {
        "status": "ERROR",
        "missing_series": ["dxy_index"],
    }
    paper_path, results_path = write_fixture(tmp_path, paper, results)
    report = ops_health.audit(results_path, paper_path, max_age_hours=8, now=NOW)
    assert report.critical == []
    assert report.status == "WARN"
    assert any("FRED" in item for item in report.warnings)

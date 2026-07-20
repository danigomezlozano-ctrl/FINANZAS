#!/usr/bin/env python3
"""Operational health audit for GeoMacro Intel.

This script is deliberately independent from the trading engine. It validates
persisted outputs after a run and exits non-zero when the system cannot be
trusted. It uses only the Python standard library plus the local
`tjl_validation.py` module.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_MIN_TRADING_ASSETS = 36
DEFAULT_MAX_AGE_HOURS = 30.0


@dataclass
class AuditReport:
    checked_at: str
    status: str = "OK"
    critical: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    context: dict[str, Any] = field(default_factory=dict)

    def add_critical(self, message: str) -> None:
        if message not in self.critical:
            self.critical.append(message)
        self.status = "ERROR"

    def add_warning(self, message: str) -> None:
        if message not in self.warnings:
            self.warnings.append(message)
        if self.status == "OK":
            self.status = "WARN"

    def as_dict(self) -> dict[str, Any]:
        return {
            "checked_at": self.checked_at,
            "status": self.status,
            "critical": self.critical,
            "warnings": self.warnings,
            "metrics": self.metrics,
            "context": self.context,
        }


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(dt: datetime | None = None) -> str:
    return (dt or utc_now()).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_json(path: Path, report: AuditReport, label: str) -> dict[str, Any] | None:
    if not path.exists():
        report.add_critical(f"Falta {label}: {path}")
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except json.JSONDecodeError as exc:
        report.add_critical(f"{label} no es JSON válido: {exc}")
        return None
    except OSError as exc:
        report.add_critical(f"No se pudo leer {label}: {exc}")
        return None
    if not isinstance(data, dict):
        report.add_critical(f"{label} debe contener un objeto JSON en la raíz")
        return None
    return data


def parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    formats = (
        "%d/%m/%Y %H:%M UTC",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d",
    )
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def check_freshness(
    report: AuditReport,
    value: Any,
    label: str,
    max_age_hours: float,
    now: datetime,
) -> None:
    parsed = parse_timestamp(value)
    if parsed is None:
        report.add_critical(f"{label}: fecha ausente o ilegible ({value!r})")
        return
    age_hours = (now - parsed).total_seconds() / 3600.0
    report.metrics[f"{label}_age_hours"] = round(age_hours, 2)
    if age_hours < -0.5:
        report.add_critical(f"{label}: fecha futura ({age_hours:.2f} h)")
    elif age_hours > max_age_hours:
        report.add_critical(
            f"{label}: datos obsoletos ({age_hours:.1f} h; máximo {max_age_hours:.1f} h)"
        )


def check_trade_book(report: AuditReport, paper: dict[str, Any]) -> None:
    trades = paper.get("trades")
    stats = paper.get("stats")
    if not isinstance(trades, list):
        report.add_critical("paper_trades.json: 'trades' no es una lista")
        return
    if not isinstance(stats, dict):
        report.add_critical("paper_trades.json: bloque 'stats' ausente o inválido")
        return

    ids = [trade.get("id") for trade in trades if isinstance(trade, dict)]
    valid_ids = [trade_id for trade_id in ids if trade_id]
    duplicates = sorted({trade_id for trade_id in valid_ids if valid_ids.count(trade_id) > 1})
    if len(valid_ids) != len(trades):
        report.add_critical("Hay trades sin ID válido")
    if duplicates:
        report.add_critical("IDs de trade duplicados: " + ", ".join(duplicates))

    open_count = sum(1 for trade in trades if trade.get("status") == "open")
    closed_count = sum(
        1 for trade in trades if trade.get("status") in ("stopped", "target_hit")
    )
    wins = sum(1 for trade in trades if trade.get("result") == "win")
    losses = sum(1 for trade in trades if trade.get("result") == "loss")

    report.metrics.update(
        {
            "paper_total": len(trades),
            "paper_open": open_count,
            "paper_closed": closed_count,
            "paper_wins": wins,
            "paper_losses": losses,
        }
    )

    expected = {
        "total_signals": len(trades),
        "open": open_count,
        "closed": closed_count,
        "wins": wins,
        "losses": losses,
    }
    for key, actual in expected.items():
        if stats.get(key) != actual:
            report.add_critical(
                f"Stats incoherentes: {key}={stats.get(key)!r}, real={actual}"
            )

    breakdown = stats.get("strategy_breakdown") or {}
    if (breakdown.get("unknown") or 0) != 0:
        report.add_critical(
            f"Hay {breakdown.get('unknown')} trades sin estrategia etiquetada"
        )


def check_tjl_integrity(report: AuditReport, paper: dict[str, Any]) -> dict[str, Any] | None:
    try:
        import tjl_validation as tjl

        tjl.assert_protocol_hash()
        tjl.verify_tjl_validation_state(paper)
        gate = tjl.get_effective_tjl_gate(paper)
        report.metrics["tjl_gate_status"] = gate.get("status")
        report.metrics["tjl_round"] = gate.get("round")
        report.metrics["tjl_protocol_hash"] = tjl.EXPECTED_PROTOCOL_HASH
        report.metrics["tjl_rules_hash"] = tjl.EXPECTED_TJL_SPEC_HASH
        return gate
    except Exception as exc:  # Integrity must fail closed.
        report.add_critical(f"Integridad TJL inválida: {type(exc).__name__}: {exc}")
        return None


def check_results(
    report: AuditReport,
    results: dict[str, Any],
    paper: dict[str, Any],
    gate: dict[str, Any] | None,
    min_trading_assets: int,
) -> None:
    if results.get("live_mode") is True:
        report.add_critical("LIVE_MODE está activado; este proyecto debe seguir en paper trading")

    freeze = results.get("strategy_freeze")
    if not isinstance(freeze, dict) or freeze.get("frozen") is not True:
        report.add_critical("No consta el bloqueo de versión de la estrategia")

    trading = results.get("trading")
    if not isinstance(trading, list):
        report.add_critical("results.json: bloque 'trading' ausente o inválido")
        trading = []
    trading_ok = sum(1 for item in trading if isinstance(item, dict) and item.get("quant"))
    report.metrics["trading_assets_total"] = len(trading)
    report.metrics["trading_assets_ok"] = trading_ok
    if len(trading) < min_trading_assets or trading_ok < min_trading_assets:
        report.add_critical(
            f"Cobertura de mercado insuficiente: total={len(trading)}, OK={trading_ok}, "
            f"mínimo={min_trading_assets}"
        )

    integrity = results.get("integrity")
    if not isinstance(integrity, dict):
        report.add_critical("results.json no contiene el bloque de integridad")
    elif integrity.get("ok") is not True:
        problems = [str(item) for item in (integrity.get("problems") or [])]
        fred_only = problems and all(item.startswith("FRED series ausentes:") for item in problems)
        if fred_only:
            report.add_warning("Integridad macro degradada: " + "; ".join(problems))
        else:
            report.add_critical("Integridad funcional ERROR: " + "; ".join(problems))

    shadow = results.get("shadow_health")
    if not isinstance(shadow, dict):
        report.add_critical("No existe estado de salud de shadow books")
    elif shadow.get("status") != "OK":
        report.add_critical(f"Shadow books en ERROR: {shadow.get('error') or shadow}")

    fred = results.get("fred_health")
    if not isinstance(fred, dict):
        report.add_warning("No existe estado de salud FRED")
    elif fred.get("status") != "OK":
        report.add_warning(
            "FRED degradado: " + ", ".join(fred.get("missing_series") or ["sin detalle"])
        )

    macro = results.get("macro") or {}
    if macro.get("eia_crude") is None:
        report.add_warning("EIA no disponible; el motor continuó sin ese dato")

    if results.get("is_weekly") is True:
        backtesting = results.get("backtesting")
        count = len(backtesting) if isinstance(backtesting, list) else 0
        report.metrics["weekly_backtests"] = count
        if count < 30:
            report.add_critical(f"Backtesting semanal incompleto: {count}/40")

    alerts = results.get("alerts") or []
    actionable = [
        alert for alert in alerts
        if isinstance(alert, dict)
        and alert.get("type") == "BUY_SIGNAL"
        and alert.get("actionable", True)
    ]
    if gate and gate.get("status") != "open" and actionable:
        names = [str(alert.get("asset") or "?") for alert in actionable]
        report.add_critical(
            "Gate TJL cerrado pero existen BUY_SIGNAL accionables: " + ", ".join(names)
        )

    snapshot = results.get("paper_trading")
    if not isinstance(snapshot, dict):
        report.add_critical("results.json no contiene snapshot de paper trading")
    else:
        paper_ids = {trade.get("id") for trade in paper.get("trades", [])}
        snapshot_ids = {trade.get("id") for trade in snapshot.get("trades", [])}
        if paper_ids != snapshot_ids:
            report.add_critical("results.json y paper_trades.json no contienen el mismo libro")


def audit(
    results_path: Path,
    paper_path: Path,
    max_age_hours: float = DEFAULT_MAX_AGE_HOURS,
    min_trading_assets: int = DEFAULT_MIN_TRADING_ASSETS,
    now: datetime | None = None,
) -> AuditReport:
    current = now or utc_now()
    report = AuditReport(
        checked_at=iso_utc(current),
        context={
            "repository": os.environ.get("GITHUB_REPOSITORY"),
            "workflow": os.environ.get("GITHUB_WORKFLOW"),
            "run_id": os.environ.get("GITHUB_RUN_ID"),
            "sha": os.environ.get("GITHUB_SHA"),
            "max_age_hours": max_age_hours,
            "min_trading_assets": min_trading_assets,
        },
    )

    paper = load_json(paper_path, report, "paper_trades.json")
    results = load_json(results_path, report, "results.json")
    if paper is None or results is None:
        return report

    check_freshness(report, results.get("generated_at"), "results_generated", max_age_hours, current)
    stats = paper.get("stats") or {}
    check_freshness(report, stats.get("last_updated"), "paper_updated", max_age_hours, current)

    check_trade_book(report, paper)
    gate = check_tjl_integrity(report, paper)
    check_results(report, results, paper, gate, min_trading_assets)
    return report


def write_report(path: Path, report: AuditReport) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(report.as_dict(), handle, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def print_report(report: AuditReport) -> None:
    print(f"OPS HEALTH: {report.status}")
    for message in report.critical:
        print(f"  CRITICAL: {message}")
    for message in report.warnings:
        print(f"  WARNING: {message}")
    print("  METRICS:", json.dumps(report.metrics, ensure_ascii=False, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Auditor operativo de GeoMacro Intel")
    sub = parser.add_subparsers(dest="command", required=True)
    audit_parser = sub.add_parser("audit", help="Valida results.json y paper_trades.json")
    audit_parser.add_argument("--results", default="results.json")
    audit_parser.add_argument("--paper", default="paper_trades.json")
    audit_parser.add_argument("--write", default="health_status.json")
    audit_parser.add_argument("--max-age-hours", type=float, default=DEFAULT_MAX_AGE_HOURS)
    audit_parser.add_argument(
        "--min-trading-assets", type=int, default=DEFAULT_MIN_TRADING_ASSETS
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "audit":
        report = audit(
            Path(args.results),
            Path(args.paper),
            max_age_hours=args.max_age_hours,
            min_trading_assets=args.min_trading_assets,
        )
        write_report(Path(args.write), report)
        print_report(report)
        return 1 if report.critical else 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

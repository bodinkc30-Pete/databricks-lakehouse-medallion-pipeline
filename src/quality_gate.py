"""Deterministic, dependency-free validation for Project 07 synthetic order lines.

No original data is loaded here. Functions are suitable for fast unit/failure tests.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Iterable, Mapping

REQUIRED_COLUMNS = ("Order ID", "SKU ID", "Quantity", "Order Amount")
SENSITIVE_COLUMNS = ("Phone #", "Detail Address", "Tax Info - Buyer Tax ID")


@dataclass(frozen=True)
class QualityResult:
    passed: bool
    total_rows: int
    failed_rules: tuple[str, ...]
    metrics: dict[str, int]

    def require_pass(self) -> None:
        if not self.passed:
            raise ValueError("DATA_QUALITY_GATE_FAILED: " + ",".join(self.failed_rules))


def validate_orders(rows: Iterable[Mapping[str, str]]) -> QualityResult:
    records = list(rows)
    failures: list[str] = []
    counts: Counter[str] = Counter()
    if not records:
        failures.append("empty_batch")
    if records and not all(all(key in row for key in REQUIRED_COLUMNS) for row in records):
        failures.append("missing_columns")
    seen: set[tuple[str, str]] = set()
    for row in records:
        order_id = str(row.get("Order ID") or "").strip()
        sku_id = str(row.get("SKU ID") or "").strip()
        if not order_id or not sku_id:
            counts["missing_business_key"] += 1
        else:
            key = (order_id, sku_id)
            if key in seen:
                counts["duplicate_business_key"] += 1
            seen.add(key)

        try:
            quantity = Decimal(str(row.get("Quantity") or ""))
            if not quantity.is_finite() or quantity != quantity.to_integral_value() or quantity <= 0:
                counts["invalid_quantity"] += 1
        except InvalidOperation:
            counts["invalid_quantity"] += 1

        try:
            amount = Decimal(str(row.get("Order Amount") or ""))
            if not amount.is_finite() or amount < 0:
                counts["invalid_amount"] += 1
        except InvalidOperation:
            counts["invalid_amount"] += 1

        if any(str(row.get(name) or "").strip() for name in SENSITIVE_COLUMNS):
            counts["sensitive_field_populated"] += 1

    for rule in ("missing_business_key", "duplicate_business_key", "invalid_quantity",
                 "invalid_amount", "sensitive_field_populated"):
        if counts[rule]:
            failures.append(rule)
    metrics = {"total_rows": len(records),
               **{rule: counts[rule] for rule in
                  ("missing_business_key", "duplicate_business_key", "invalid_quantity",
                   "invalid_amount", "sensitive_field_populated")}}
    return QualityResult(not failures, len(records), tuple(failures), metrics)

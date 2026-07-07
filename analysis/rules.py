"""Rule engine: turn mappings.yaml rule specs into scores, flags and panels.

EXPLORATORY / TESTING. One place (instead of three near-copies in
run_consistency / validate_consistency / backtest) that knows how to:

  - read a rule from mappings.yaml and check it is implemented
  - build the aligned panel of implied series for the rule's indicators
  - compute the inconsistency score from the structured `logic.score` spec
  - evaluate the structured `logic.flag` spec

Score specs (mappings.yaml):
  product rules  {type: product, terms: [roleA, roleB], sign: +1|-1}
      score = sign * z_roleA * z_roleB
      sign +1: the identity says the roles move OPPOSITE (Phillips, Okun,
               Beveridge) -> same-direction pricing (positive product) is the
               inconsistency.
      sign -1: the identity says the roles move TOGETHER (UIP, claims/labor)
               -> opposite-direction pricing is the inconsistency.
  linear rules   {type: linear, weights: {roleA: 1, roleB: -1, ...}}
      score = sum_i w_i * z_role_i   (spread/residual rules: Sahm, Taylor)

Flag specs:
  {metric: value|abs, threshold: T}   ->   score > T   |   |score| > T
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

import signals as sig


class RuleError(ValueError):
    """Rule missing, unimplemented, or malformed."""


# --------------------------------------------------------------------------- #
# spec access
# --------------------------------------------------------------------------- #
def get_rule(cfg: dict, rule_key: str) -> dict:
    if rule_key not in cfg["rules"]:
        raise RuleError(f"unknown rule {rule_key!r}; have {list(cfg['rules'])}")
    rule = cfg["rules"][rule_key]
    if rule.get("status", "planned") == "planned":
        raise RuleError(f"rule {rule_key!r} is planned, not implemented yet")
    return rule


def implemented_rules(cfg: dict) -> list[str]:
    return [k for k, r in cfg["rules"].items() if r.get("status", "planned") != "planned"]


def flag_threshold(rule: dict) -> float:
    return float(rule["logic"]["flag"]["threshold"])


def flag_metric(rule: dict) -> str:
    metric = rule["logic"]["flag"].get("metric", "abs")
    if metric not in ("value", "abs"):
        raise RuleError(f"flag metric must be 'value' or 'abs', got {metric!r}")
    return metric


def flag_series(rule: dict, score: pd.Series, threshold: float | None = None) -> pd.Series:
    thr = flag_threshold(rule) if threshold is None else threshold
    val = score.abs() if flag_metric(rule) == "abs" else score
    return val > thr


# --------------------------------------------------------------------------- #
# scoring
# --------------------------------------------------------------------------- #
def score_from_logic(rule: dict, z: dict[str, pd.Series]) -> pd.Series:
    spec = rule["logic"]["score"]
    kind = spec.get("type")
    if kind == "product":
        terms = spec["terms"]
        if len(terms) != 2:
            raise RuleError(f"product score needs exactly 2 terms, got {terms}")
        sign = float(spec.get("sign", 1))
        out = sign * z[terms[0]] * z[terms[1]]
    elif kind == "linear":
        weights = spec["weights"]
        out = sum(float(w) * z[role] for role, w in weights.items())
    else:
        raise RuleError(f"unknown score type {kind!r} (use 'product' or 'linear')")
    return out.rename("score")


# --------------------------------------------------------------------------- #
# panel construction (shared by run / validate / backtest)
# --------------------------------------------------------------------------- #
def build_rule_panel(
    rule_key: str,
    zip_path: Path,
    z_window: int,
    kind: str | None = None,
    min_volume: int = 0,
    with_prices: bool = False,
    cfg: dict | None = None,
    history: pd.DataFrame | None = None,
    markets: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, list[str], dict]:
    """Aligned panel for one rule: implied series per role, causal z-scores,
    inconsistency score. Warmup rows (z NaN) are dropped.

    Columns: <role> (signal series), z_<role>, score, and with_prices=True
    additionally px_<role> (the tradeable single-contract prob series) and
    pxref_<role> (that contract's conid per bar — the stitched px series jumps
    when the reference switches, so execution must exit before a change).

    `kind` defaults to the rule's `signal:` key, else "median". A per-indicator
    `signal:` key overrides it (e.g. a thin leg forced to "prob").
    Pass `history`/`markets` to reuse an already-loaded bundle across rules.
    """
    cfg = cfg or sig.load_mappings()
    rule = get_rule(cfg, rule_key)
    defaults = cfg["defaults"]
    band = tuple(defaults["prob_band"])
    freq = defaults["resample_freq"]
    ffill_limit = int(defaults.get("ffill_limit", sig.DEFAULT_FFILL_LIMIT))
    roll_days = int(defaults.get("roll_days", sig.DEFAULT_ROLL_DAYS))
    kind = kind or rule.get("signal", "median")

    if markets is None:
        markets = sig.load_markets(zip_path)
    if history is None:
        history = sig.load_history(zip_path)

    roles = list(rule["indicators"])
    cols: dict[str, pd.Series] = {}
    for role, spec in rule["indicators"].items():
        role_kind = spec.get("signal", kind)
        cols[role] = sig.implied_series(
            history, markets, spec["market_name"], kind=role_kind, band=band,
            freq=freq, ffill_limit=ffill_limit, roll_days=roll_days,
            min_volume=min_volume)
        if with_prices:
            frame = sig.implied_prob_frame(
                history, markets, spec["market_name"], band=band,
                freq=freq, ffill_limit=ffill_limit, roll_days=roll_days,
                min_volume=min_volume)
            cols[f"px_{role}"] = frame["value"]
            cols[f"pxref_{role}"] = frame["ref_conid"]

    panel = sig.align(*cols.values())
    panel.columns = list(cols.keys())
    if panel.empty:
        raise RuleError(f"no overlapping observations for rule {rule_key!r}")

    z = {role: sig.zscore_rolling(panel[role], z_window) for role in roles}
    for role in roles:
        panel[f"z_{role}"] = z[role]
    panel["score"] = score_from_logic(rule, z)
    panel = panel.dropna(subset=["score"])
    if panel.empty:
        raise RuleError(f"no score points after z-score warmup for {rule_key!r}")
    return panel, roles, rule

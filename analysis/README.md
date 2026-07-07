# analysis/

Research pipeline for the cross-indicator consistency-arbitrage strategy.

> **Status: EXPLORATORY / TESTING.** The strategy is unproven. These scripts
> test whether economic identities carry tradeable signal on ForecastTrader
> markets. Nothing here is a production trading system. We trade only on
> ForecastTrader; FRED is ground-truth context only.

## Data sources

- **IBKR forecast bundle** — `../forecast_analysis_dataset_*.zip` (export from
  the VPS Postgres). Refresh procedure + VPS details: `../RUNBOOK.local.md`
  (gitignored).
- **FRED macro ground truth** — `macro/fred.sqlite`, built by `collect_fred.py`.
  NOTE: indexed by reference period, not release date — context only, never a
  causal conditioning variable (vintages via `realtime_start` if ever needed).

The canonical implied signal is the `contract_history` table (avg traded
price). `projected_probabilities` is NOT used for signal: it has gaps (9-day
outage Jun 4–12 2026) and is empty for some markets.

## Scripts

| Script | Purpose |
|---|---|
| `run_all.py` | **One command**: readiness → static-arb scan → all rules → validation (+`--grid`) → backtest → `report/REPORT.md`. |
| `mappings.yaml` | Single source of truth: rule → markets + FRED series + structured score/flag logic. |
| `rules.py` | Rule engine: product/linear scorers, flags, shared panel builder. |
| `signals.py` | Implied series from the bundle (front-expiry ladders, causal z, cached loader). |
| `run_consistency.py` | One rule end-to-end: score, flags, CSV + plot. |
| `validate_consistency.py` | Event study: does a flag predict re-alignment beyond a magnitude-matched control? `--grid` sweeps z-window × threshold. |
| `backtest.py` | Toy costed backtest of the convergence trade; `--min-volume` marks-sensitivity. |
| `oos_test.py` | **Out-of-sample gate**: frozen params, post-split events only, OOS controls + OOS backtest. |
| `arbitrage_scan.py` | Model-free within-market checks: ladder monotonicity, YES/NO parity, persistence. |
| `fed_path_check.py` | Same-event identity: Fed Decision (categorical) vs Fed Funds ladder at the same meeting. |
| `discover_rules.py` | Pair mining: 6h-change correlations, Bonferroni screen, OOS confirmation, YAML stubs. |
| `collect_fred.py` | Pull mapped macro series from FRED into `macro/fred.sqlite`. |
| `check_readiness.py` | PASS/WARN/FAIL audit of both sources before inference. |
| `explore_dataset.py` | Summary stats + figures for a bundle. |

## Signal construction (methodology-critical)

1. Per market, YES contracts only, band-filter degenerate prices
   (`prob_band`, default 0.001–0.999).
2. **Per expiration** survival ladder; the signal tracks the **front expiry**
   and rolls `roll_days` (default 2) before settlement. Pooling expiries —
   the original implementation — mixed e.g. April-CPI and June-CPI contracts
   into one fictitious distribution (42–66% of timestamps were mixed).
3. Signal kinds: `median` (strike where the front ladder crosses P=0.5, in
   underlying units) or `prob` (one reference contract chosen causally by
   trailing-window activity — no full-window liquidity look-ahead).
4. Duplicated `(conid, ts)` rows across `period_requested` (43% of the bundle,
   4.9% disagreeing) are deduplicated preferring the finer `chart_step`.
5. Resample to a common 1h grid with **bounded** forward-fill
   (`ffill_limit=48` bars) so dead markets go NaN instead of flatlining.
6. Causal trailing z-scores (`z_window`, default 48h); rule score per
   mappings.yaml (`product` or `linear`); flag per the rule's metric.

Validation hardening: events = crossings of the rule's own flag metric,
non-overlapping (`--min-gap` ≥ max horizon), block-bootstrap CIs, random
baseline AND magnitude-matched baseline (equally-extreme non-event bars — the
control that matters).

## Current findings (Jul-07 2026 bundle, ~138d of history; IN-SAMPLE)

Full table: `report/REPORT.md` (regenerate with `run_all.py --grid`).

**Static arbitrage (scanner).** Within-expiry ladder inversions occur at ~8%
of adjacent-strike pairs across 229 markets; **3,593 persistent runs (≥2
consecutive bars), 1,394 of them with volume on both legs** — the credible
subset. YES/NO parity is tight (mean gap +0.4c; >5c in 0.02% of 922K pairs).
So: small but real static mispricings exist; whether they are executable
after spread/fees needs order-book data.

**Consistency rules (10 implemented, 9 ran; okun's GDP leg too thin).**
- Most robust: **payrolls_labor** — EDGE-SUGGESTIVE in every z≤48h grid cell
  (12–17 events; 72h mean reversion ~2× the matched control; backtest
  break-even ~5.5c/leg vs realistic 1–2c cost).
- **core_headline** similar but slightly less stable (break-even ~3.7c/leg).
- **sahm**, **taylor**: EDGE-SUGGESTIVE at the default cell, patchy across
  the grid — parameter-sensitive, treat with suspicion.
- phillips (the original headline result) is now **WEAK**: the earlier
  EDGE-SUGGESTIVE verdict did not survive the expiry-mixing fix + more data.
- uip, pce_cpi INCONCLUSIVE (too few events); beveridge, claims_labor WEAK.

**Out-of-sample gate (split 2026-05-01, ~68 OOS days).** The regression
toward null that the caveats predicted happened: phillips, taylor, beveridge
and claims_labor fall to WEAK; sahm/uip/pce_cpi have too few OOS events.
payrolls_labor briefly looked like a survivor (OOS EDGE-SUGGESTIVE + positive
OOS backtest) — see the artifact paragraph below for why that did not hold.

**Reference-switch artifact (the big catch, A11).** The tradeable `prob`
series is stitched across reference-contract switches (expiry rolls,
activity migration). Backtests holding through a switch booked the jump
between two *different contracts* as PnL. Taylor leg-attribution exposed it:
"policy-leg fades" earning +0.15–0.26/trade were entries at px≈0.97/0.05
followed by ±0.9 stitched jumps (median PnL +0.02, win 56% — nothing).
With forced exits before any reference switch (`reason="ref_roll"`),
**payrolls_labor's break-even collapsed 5.5c → ~0.1c and its edge over
random entry went to zero.** As of the Jul-07 bundle, NO rule has a
positive costed edge under execution-integrity constraints. This mirrors
the project's original conclusion: inconsistencies revert in z-space, but
the reversion is not captureable on these contracts with this execution.

**Permutation test (strictest null).** Circular-shift permutation
(`--permute`) keeps each leg's own dynamics but destroys cross-leg
alignment. payrolls_labor p ≈ 0.11 (leg mechanics explain most of it);
taylor is the only rule with real cross-leg structure (p ≈ 0.003) — but that
structure has no captureable PnL (see above). sahm's large reversion:
p ≈ 0.53, pure mechanics.

**Effective-cost proxy (no order book available).** No public REST quote
endpoint exists (probed; the bid/ask sample comes from the deferred
websocket feed). Interim proxy: per-market |YES+NO−1| parity gaps — p75 ≈ 1c
on every rule market. Break-evens now sit at ~0–1c, i.e. at or below the
proxy cost floor.

**Same-event identity (fed_path).** Fed Decision vs the Fed Funds ladder
price the same meeting within mean |gap| ~3–4c (staleness-inflated bars),
tails to ~11c. Coherent overall; the persistent tails are the leads worth
checking against live quotes.

**Rule discovery (pair mining).** 115-market universe, 814 pairs with enough
overlap, ZERO Bonferroni-significant co-movement pairs. Every strong raw
correlation sits on a ~25-bar overlap — chance. No data-mined rules exist
yet at this venue's liquidity; rerun as data accrues.

## Known limitations

- **The OOS window is one 68-day segment.** Next gate: evaluate only on data
  collected after 2026-07-07 (the collector keeps running; parameters are
  frozen in mappings.yaml).
- `avg` bars are not executable quotes; no bid/ask exists in the bundle; the
  cost model is a flat proxy.
- 87% of bars have volume 0 (carried marks). Use `backtest.py --min-volume 1`
  to gauge sensitivity.
- Event counts are still 10–20 per rule; the power table in the report says
  ~3–6 events/30d accrue per rule — several more months to reach ~40.

The full improvement backlog (with what was already fixed) lives in
[../docs/IMPROVEMENTS.md](../docs/IMPROVEMENTS.md).

## TODO (next research steps)

- [ ] **Websocket bid/ask capture (F3)** — now doubly decisive: it gives real
      spreads AND per-contract executable quotes, removing both the cost
      proxy and the stitched-series problem at once.
- [ ] **Next OOS round**: at the next export, rerun
      `oos_test.py --split 2026-07-07` (parameters frozen today). Under
      execution-integrity constraints nothing currently survives; the OOS
      round tests whether that stays true as data grows.
- [ ] Trade construction that respects rolls by design: hold-to-settlement
      of a single contract (PnL vs realized FRED outcome) instead of
      mark-to-market fades — sidesteps reference switching entirely.
- [ ] fed_path extensions (B9): Number of Fed Rate Cuts convolution;
      non-front meetings. Regional-CPI aggregation rule (B10, needs BLS
      weights).
- [ ] Factor residuals (C2); rerun `discover_rules.py` as data accrues.
- [ ] Move FRED collection to the VPS timers (F4).

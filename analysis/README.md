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
| `settlement_test.py` | **Hold-to-settlement construction**: buy the front contract on a flag, hold to resolution (FRED-mapped or price-pinned outcome). No rolls, no mark exit. |
| `oos_test.py` | **Out-of-sample gate**: frozen params, post-split events only, OOS controls + OOS backtest. |
| `factor_residuals.py` | C2: PCA macro factor on the implied panel; do deviations from the factor revert (train-frozen loadings, hard split)? |
| `regional_cpi_check.py` | B10: national CPI yoy vs BLS-weighted census-region CPI markets (levels → yoy via FRED bases). Accounting identity. |
| `release_event_study.py` | D4: does \|score\| compress at macro releases (leg-market expiration days) vs random times? Mechanism evidence. |
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

## Current findings (Jul-23 2026 bundle, ~137d of history; IN-SAMPLE)

Full table: `report/REPORT.md` (regenerate with `run_all.py --grid`).

**Static arbitrage (scanner).** Within-expiry ladder inversions occur at ~8.7%
of adjacent-strike pairs across 237 markets; **4,188 persistent runs (≥2
consecutive bars), 1,685 of them with volume on both legs** — the credible
subset, still growing roughly linearly with data. YES/NO parity is tight
(mean gap +0.4c; >5c in 0.02% of 1.06M pairs). So: small but real static
mispricings exist; whether they are executable after spread/fees needs
order-book data.

**Consistency rules (11 implemented, 10 ran; okun's GDP leg too thin).**
Under the ref-roll execution constraint (A11), z-space reversion and $
capture now tell one consistent story:
- **taylor** is the only rule with real cross-leg structure (perm p ≈ 0.01,
  23 events) — and it still loses money per trade (break-even −0.1c/leg).
- **payrolls_labor**, **core_headline**, **sahm** remain EDGE-SUGGESTIVE in
  z-space but their permutation p ≈ 0.14–0.52 (leg mechanics) and
  break-evens sit at 0–0.1c/leg, below the ~1c parity-gap cost proxy.
- phillips, beveridge, claims_labor WEAK; uip, pce_cpi, okun_canada
  INCONCLUSIVE (too few events).
- **Every rule's mean net/trade is negative at a realistic 2c cost.**

**Out-of-sample gates.** Split 2026-05-01 (~84 OOS days): verdict pattern
holds, but NO rule's OOS break-even clears the ~1c proxy spread. Split
2026-07-07 (frozen params, first 16 OOS days): 0–4 events per rule,
INCONCLUSIVE everywhere — too early; rerun as the post-Jul-07 window grows.

**Reference-switch artifact (the big catch, A11).** The tradeable `prob`
series is stitched across reference-contract switches (expiry rolls,
activity migration). Backtests holding through a switch booked the jump
between two *different contracts* as PnL. Taylor leg-attribution exposed it:
"policy-leg fades" earning +0.15–0.26/trade were entries at px≈0.97/0.05
followed by ±0.9 stitched jumps (median PnL +0.02, win 56% — nothing).
With forced exits before any reference switch (`reason="ref_roll"`),
**payrolls_labor's break-even collapsed 5.5c → ~0.1c and its edge over
random entry went to zero.** As of the Jul-23 bundle, NO rule has a
positive costed edge under execution-integrity constraints. This mirrors
the project's original conclusion: inconsistencies revert in z-space, but
the reversion is not captureable on these contracts with this execution.

**Hold-to-settlement construction (new 2026-07-23).** The construction that
sidesteps reference switching by design: on a flag, hold each leg's front
contract to resolution. Outcomes come from a FRED settlement mapping
(mappings.yaml `settlement:` blocks — NSA CPI basis, venue-consistent
reference months) with price-pin/ladder-inference fallback and a FRED-vs-pin
disagreement guard (caught the SA-vs-NSA CPI trap and JOLTS initial-print
revisions during development). First pass: 5–20 settled trades per rule,
mixed signs, |t| ≤ 2.3 — no edge evidence either, but this instrument's N
grows automatically as expiries resolve.

**Permutation test (strictest null).** Circular-shift permutation
(`--permute`) keeps each leg's own dynamics but destroys cross-leg
alignment. payrolls_labor p ≈ 0.14 (leg mechanics explain most of it);
taylor is the only rule with real cross-leg structure (p ≈ 0.01, stable
across two bundles) — but that structure has no captureable PnL (see
above). sahm's large reversion: p ≈ 0.52, pure mechanics.

**Effective-cost proxy (no order book available).** No public REST quote
endpoint exists (probed; the bid/ask sample comes from the deferred
websocket feed). Interim proxy: per-market |YES+NO−1| parity gaps — p75 ≈ 1c
on every rule market. Break-evens now sit at ~0–1c, i.e. at or below the
proxy cost floor.

**Same-event identity (fed_path).** Fed Decision vs the Fed Funds ladder
price the same meeting within mean |gap| ~2.7–3.7c across 3 meetings
(staleness-inflated bars), tails to ~13c; the no-cut boundary sits >5c
apart 28% of the time. Coherent overall; the persistent tails are the
leads worth checking against live quotes.

**Rule discovery (pair mining).** 123-market universe, 822 pairs with enough
overlap, ZERO Bonferroni-significant co-movement pairs. Every strong raw
correlation sits on a ~25-bar overlap — chance. No data-mined rules exist
yet at this venue's liquidity; rerun as data accrues.

**Regional CPI aggregation (B10, new 2026-07-25).** National CPI yoy vs the
BLS-weighted census-region markets (which price NSA index LEVELS — converted
to implied yoy via FRED regional bases). Mean gap +0.32pp is a constant
(weights/base approximation); demeaned deviations sit within strike
granularity with ZERO persistent 24h+ runs → the venue prices the
aggregation identity **coherently**.

**Release-time behavior (D4, new 2026-07-25).** Do inconsistencies close AT
macro releases (leg-market expiration days) rather than at random times?
Pooled post/pre |score| ratio 0.99; NO rule shows release-specific
compression. Inconsistencies decay on their own clock — mechanism evidence
AGAINST "the data release resolves a real mispricing" and FOR
signal-construction noise. Consistent with the ref-roll backtest result.

## Known limitations

- **The post-Jul-07 OOS window is only ~16 days** (first check 2026-07-23:
  INCONCLUSIVE everywhere). Rerun `oos_test.py --split 2026-07-07` at each
  new export; parameters stay frozen in mappings.yaml.
- `avg` bars are not executable quotes; no bid/ask exists in the bundle; the
  cost model is a flat proxy.
- 87% of bars have volume 0 (carried marks). Use `backtest.py --min-volume 1`
  to gauge sensitivity.
- Event counts are still 12–23 per rule; the power table in the report says
  ~3–5 events/30d accrue per rule — ~3–8 more months to reach ~40.

The full improvement backlog (with what was already fixed) lives in
[../docs/IMPROVEMENTS.md](../docs/IMPROVEMENTS.md).

## TODO (next research steps)

- [ ] **Websocket bid/ask capture (F3)** — now doubly decisive: it gives real
      spreads AND per-contract executable quotes, removing both the cost
      proxy and the stitched-series problem at once.
- [ ] **Next OOS round**: rerun `oos_test.py --split 2026-07-07` at each new
      export (first check 2026-07-23 on 16 OOS days: INCONCLUSIVE, all
      net/trade negative). Under execution-integrity constraints nothing
      currently survives; the OOS round tests whether that stays true as
      data grows.
- [x] Trade construction that respects rolls by design →
      `settlement_test.py` (2026-07-23): hold the front contract to
      resolution, outcome from a FRED settlement mapping (mappings.yaml
      `settlement:` blocks) with price-pin/ladder fallback and a
      FRED-vs-pin disagreement guard. First pass: mixed signs, |t| <= 2.3,
      no edge; N grows as expiries resolve.
- [x] fed_path extensions (B9, 2026-07-23): non-front meetings measured
      (~2x wider gaps, staleness); cuts-count bound implemented but NOT
      TESTABLE yet (cuts market tail categories are never-traded marks).
- [x] Regional-CPI aggregation (B10, 2026-07-25): `regional_cpi_check.py` —
      venue prices the identity coherently (no persistent violations).
- [x] Release-time event study (D4, 2026-07-25): `release_event_study.py` —
      no release-specific compression; inconsistencies decay on their own
      clock (mechanism evidence against tradeable mispricing).
- [x] Factor residuals (C2, 2026-07-23): `factor_residuals.py` — factor
      explains ~3% of change variance; deviations revert but not beyond
      matched control (WEAK). Runs inside run_all; discover_rules reruns
      there too.
- [x] FRED collection: superseded by `scripts/refresh_data.sh` (F2) — FRED
      refreshes on every data pull.

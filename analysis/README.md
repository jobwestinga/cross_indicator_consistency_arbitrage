# analysis/

Exploratory analysis + first vertical slice of the cross-indicator
consistency-arbitrage strategy.

> **Status: EXPLORATORY / TESTING.** The strategy is unproven. These scripts
> exist to validate whether the economic identities in `../strats.txt` carry any
> tradeable signal. Nothing here is a production trading system. We trade only on
> ForecastTrader; FRED is ground-truth context only.

## Data sources

- **IBKR forecast bundle** — `../forecast_analysis_dataset_*.zip` (export from the
  VPS Postgres). Refresh procedure + VPS details are in `../RUNBOOK.local.md`
  (gitignored).
- **FRED macro ground truth** — `macro/fred.sqlite`, built by `collect_fred.py`.

The canonical implied-probability signal is the IBKR `contract_history` table
(avg traded price). `projected_probabilities` is intentionally **not** used for
signal: it has gaps and is empty for some markets. See `signals.py`.

## Scripts

| Script | Purpose |
|---|---|
| `collect_fred.py` | Pull mapped macro series from FRED into `macro/fred.sqlite`. |
| `check_readiness.py` | PASS/WARN/FAIL audit of both sources before inference. |
| `explore_dataset.py` | Summary stats (JSON + per-market CSV) + figures. |
| `signals.py` | Reusable: implied-prob series, band filter, 1h resample, z-score, mapping/FRED loaders. |
| `run_consistency.py` | Run one consistency rule end-to-end (Phillips, Sahm). |
| `validate_consistency.py` | Test whether a flag predicts re-alignment (mean reversion) vs baseline. |
| `backtest.py` | Toy backtest of the fade-the-flag convergence trade, with costs. |
| `mappings.yaml` | Single source of truth: rule -> markets + FRED series + flag logic. |

## Run a consistency check

```bash
cd analysis
python3 run_consistency.py --rule phillips     # unemployment vs core CPI
python3 run_consistency.py --rule sahm         # unemployment vs recession (low power)
```
Outputs land in `analysis/consistency/<rule>_consistency.{csv,png}`.

## Validate a signal (does a flag predict re-alignment?)

```bash
python3 validate_consistency.py --rule phillips
python3 validate_consistency.py --rule sahm --z-window 72 --threshold 1.5
```
Gate question for the whole strategy: when a rule flags an inconsistency, do the
markets subsequently CONVERGE more than baseline? Uses a **causal trailing
z-score** (no look-ahead), event-study forward outcomes, and a random-entry
baseline with bootstrap CIs. Outputs to `analysis/validation/`.

**This is a necessary-condition test + opportunity sizing, NOT proof of profit**
(no costs/execution/slippage modeled - that is a later backtest).

The validation is hardened (vs the first pass):
- **Median signal** (full-window, no strike collapse) for ladder-rich markets;
  thin markets (recession) fall back to the single-strike `prob` signal via the
  rule's `signal:` key in mappings.yaml.
- **Non-overlapping events** (`--min-gap`, default = max horizon) so forward
  windows don't share bars -> events are ~independent.
- **Block-bootstrap** CIs (robust to residual autocorrelation).
- **Magnitude-matched baseline**: compares flagged entries against *equally
  extreme* non-event bars. This is the real control - it asks whether entry
  timing adds anything beyond "the score is large".

### Current findings (exploratory, ~93-day window)
- **Phillips: EDGE-SUGGESTIVE.** 25 independent events. %revert (~0.9) is similar
  to equally-extreme bars, BUT reversion *magnitude* is ~2-3x larger (mean_rev
  ~2-7 vs matched ~0.6-2.8): entering at the flag crossing captures more of the
  reversion swing than entering at a random extreme moment. A real timing edge,
  preliminarily.
- **Sahm: INCONCLUSIVE.** Only 4 independent events after de-overlapping (its
  earlier "edge" was an overlap artifact). Genuinely data-starved.
- Still no costs/execution; short window; small N. Directional, not conclusive.

## Backtest (does the convergence actually make money?)

```bash
python3 backtest.py --rule phillips            # median signal, fade the flag
python3 backtest.py --rule phillips --signal prob --cost 0.01
```
Signal = the validated causal score; execution = fade each leg on its ATM YES
contract (`position = -sign(z_leg)`); enter one bar AFTER the signal; exit when
`|score|` falls below an exit band or after max-hold; charge a round-trip cost
per leg and sweep cost to find break-even. Outputs to `analysis/backtest/`.

### Current finding: NOT TRADEABLE (as built)
This is the important, sobering result and the reason the backtest exists:
- The score **does** converge (100% / ~89% of trades exit on convergence), so
  step 2 was right that inconsistencies revert.
- But the convergence is **tiny in actual contract-price terms**. Phillips: gross
  PnL ~ +0.013 over 17 trades, **break-even round-trip cost ~ $0.0004/leg** -
  any realistic spread wipes it out. With signal+execution on the same contract,
  **gross is ~zero/negative even before costs** (mean -0.003/trade, t ~ 0).
- Win rates are low (11-14%); the strategy does not beat random entry.

**Conclusion:** the inconsistency reverts in z-space, but that reversion does not
translate into a profitable, costed trade on these contracts with this simple
execution. The apparent step-2 edge does **not** survive a real backtest.

Caveats cut both ways: cost is a flat proxy (no bid/ask in the bundle), execution
is a single ATM contract (not the whole ladder), exit is mark-to-market, and N is
tiny. So this is "no evidence of a tradeable edge yet," not "proven impossible".

## Method (current slice)

1. For each market take the YES side, pick the single most-traded strike as a
   stable reference, track its traded price = P(outcome > strike) over time.
2. Drop degenerate values outside the `prob_band` (default 0.001-0.999).
3. Resample each market to a common 1h grid (forward-fill) and align.
4. Z-score each series over the window; combine per the rule's `logic.score`.
5. Flag timestamps in the inconsistent region; write CSV + plot + summary.

## Known limitations (to revisit before trusting results)

- **Thin macro-event count.** ~93-day window = only a handful of real CPI / jobs
  releases and few independent flag events (Phillips 25, Sahm 4), so power is
  low. The single biggest improvement is simply MORE DATA - keep collecting.
- **Sahm is data-starved.** Recession market is thin (prob signal only) and
  gives too few independent events -> INCONCLUSIVE.
- **No costs / execution / sizing.** Validation tests a necessary condition
  (does the inconsistency revert beyond a matched control), not profit.

Fixed since the first pass: single-strike window collapse (now median signal),
whole-window look-ahead z-score (now causal trailing z in run_consistency and
validate), overlapping events + optimistic CIs (now min-gap + block-bootstrap +
magnitude-matched baseline).

## TODO

- [ ] **C5 (b): move FRED collection to the VPS systemd timers** when the project
      goes live, so `fred.sqlite` stays fresh without manual runs. For now it is
      manual — run `collect_fred.py` before a backtest; `check_readiness.py`
      flags staleness.
- [x] Rolling implied-median signal (fixes single-strike window collapse).
- [x] Causal trailing z-score in run_consistency.py and validate_consistency.py.
- [x] Harden validation: min-gap (non-overlapping events) + block-bootstrap +
      magnitude-matched baseline.
- [x] Step 3: real backtest with costs, execution, sizing -> NOT TRADEABLE as
      built (gross edge ~0, killed by costs). See Backtest section.
- [ ] If pursuing further: trade the whole ladder / contract nearest the median
      (not one ATM contract); model real bid/ask; revisit entry/exit rules.
- [ ] More data: keep the collector running to grow the window / event count.
- [ ] Implement remaining rules in `mappings.yaml` (taylor, okun, beveridge, uip).

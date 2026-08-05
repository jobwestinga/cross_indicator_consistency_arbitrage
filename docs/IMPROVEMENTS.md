# Improvement Roadmap

Full audit of the project (2026-07-07) against the research goal: **is
cross-indicator consistency arbitrage possible on ForecastTrader markets, using
existing macroeconomic identities — and can we formalize new rules from the data
itself?**

Statuses: `[x]` done in this pass · `[ ]` open · `[~]` partially done.

---

## A. Scientific-validity fixes (critical — affect existing findings)

- [x] **A1. Expiration-mixing bug in the median signal.** `implied_median_series`
  pooled *all* YES contracts at each timestamp into one survival ladder,
  ignoring expiration. Measured on the Apr-30 bundle: 42% of US Core CPI
  timestamps and 66% of Fed Funds timestamps mix 2–7 different expiries
  (April CPI and June CPI contracts interpolated as one distribution).
  The published phillips EDGE-SUGGESTIVE verdict was computed on this.
  **Fix: build the ladder per (timestamp, expiration) and track the front
  (nearest unexpired) expiry with a roll buffer. Re-run all findings.**
- [x] **A2. Period-duplication bug.** 320K of 743K `(conid, ts)` pairs exist as
  both `1week` (30-min bars) and `1month` (1-h bars) rows; 4.9% disagree on
  `avg`. The old dedupe (`keep="last"` after sorting by time) picked
  arbitrarily. **Fix: deterministically prefer the finer `chart_step`.**
- [x] **A3. Unbounded forward-fill.** `resample().ffill()` carried the last
  price forever — across outages and dead markets — producing flat segments
  that make trailing z-scores spike spuriously when trading resumes.
  **Fix: `ffill(limit=...)` (default 48 bars) and drop the stale tail.**
- [x] **A4. Look-ahead in reference-strike choice.** `implied_prob_series`
  picked the most-traded strike *over the whole window* — future liquidity
  information leaks into the past. **Fix: causal choice (most observations up
  to each timestamp, within the front expiry).**
- [x] **A5. Zero-volume marks.** 87% of history bars have `volume == 0`: the
  `avg` there is a carried mark, not a trade. Signals may use marks
  (documented), but backtest fills on marks are optimistic.
  **Done: loader exposes volume; backtest gained `--min-volume` entry filter
  as a sensitivity knob.** `[x]` Volume-weighted resampling (2026-07-25):
  `implied_prob_frame(agg="vwap")` prices each bar from actual prints only
  (zero-volume bars go NaN); `backtest.py --agg vwap` is the fills-realism
  view. First check: payrolls gross PnL on trades-only fills ≈ 0 — the
  mark-based result was not hiding profit.
- [x] **A6. Robust timestamp parsing.** `ts_utc` has mixed dtypes in the real
  export; loader now parses with `errors="coerce"` and drops unparseable rows.
- [x] **A7. FRED vintage discipline.** Done 2026-07-25: `collect_fred.py`
  also pulls ALFRED `output_type=4` initial releases into `value_initial` +
  `initial_release_date` (a true release calendar);
  `load_fred_series(vintage="initial")` serves them. The settlement test now
  settles on INITIAL prints — and the hierarchy was inverted to FRED-first
  after a live counterexample: April JOLTS pinned 0.05 pre-release at strike
  7.4, initial print 7.618M → settled YES (pins are pre-release
  expectations; surprises flip them). NOTE: `obs_date` still indexes the
  reference period; the causal-conditioning caveat stands for signal use.
- [x] **A8. Multiple-testing awareness.** We test many rules × horizons ×
  thresholds. The report now shows the whole grid rather than a cherry-picked
  cell. `[x]` Formal FDR control (2026-07-23): the report's rules table now
  carries Benjamini–Hochberg q-values over the permutation p's across rules.
- [x] **A9. Out-of-sample discipline.** `analysis/oos_test.py`: frozen
  thresholds, events starting after `--split` (default 2026-05-01) only,
  OOS-restricted controls, OOS-only backtest trades. Wired into run_all
  (report section 3). First result: only payrolls_labor survives both
  validation and backtest OOS; most in-sample promise regresses.
- [ ] **A10. Executable-price realism.** `avg` is a bar average, not a quote;
  there is no bid/ask in the bundle. The scanner reports *persistence* of
  violations (≥2 consecutive bars) to separate artifacts from actionable
  gaps; real cost modeling needs order-book capture (F3).
- [x] **A11. Reference-switch PnL artifact (critical, found 2026-07-07 pm).**
  The tradeable `prob` series is stitched across reference-contract switches
  (expiry rolls, activity migration); a backtest holding through a switch
  books the jump between two DIFFERENT contracts as PnL. Exposed by taylor
  leg attribution: "policy-leg fades" earning +0.15–0.26/trade were entries
  at px≈0.97/0.05 followed by ±0.9 stitched jumps. **Fix: `implied_prob_frame`
  exposes `ref_conid`; simulate() and the random baseline force an exit the
  bar before any leg's reference switches (`reason="ref_roll"`).** Effect:
  payrolls_labor's OOS break-even collapsed 5.5c → 0.1c and its edge vs
  random went to zero — the previous "surviving rule" was this artifact.

## B. New rules from existing macro identities (data confirmed present)

Coverage measured in the Apr-30 bundle (rows of YES history):

| rule | markets | rows | expectation |
|---|---|---|---|
- [x] **B1. taylor** — Fed Funds (34K) vs CPI Yearly (25K) + Unemployment (11K). Linear residual: policy z minus Taylor-implied z.
- [x] **B2. okun** — Real GDP (774) vs Unemployment. Product rule; GDP thin → likely INCONCLUSIVE but should be measured, not assumed.
- [x] **B3. beveridge** — JOLTS (2.5K) vs Unemployment. Product rule.
- [x] **B4. uip** — Fed Funds vs USDJPY (14K). Opposite-direction product rule.
- [x] **B5. claims_labor** — Initial Jobless Claims (22K) vs Unemployment. Leading-indicator coherence (same-direction expected).
- [x] **B6. payrolls_labor** — Payroll Employment (6.3K) vs Unemployment. Establishment-vs-household survey coherence.
- [x] **B7. core_headline** — Core CPI vs headline CPI Yearly. Divergence-bounded spread.
- [x] **B8. pce_cpi** — Core PCE (812) vs Core CPI. Same-direction; thin.
- [~] **B9. Same-event redundancy (strongest class).**
  `analysis/fed_path_check.py`: Fed Decision (categorical, strike-coded
  1=hike50+..5=cut50+) vs the Fed Funds ladder at the same meeting
  expiration. IB quotes the target as the range MIDPOINT; boundaries are
  S(r_mid-0.25)=1-P(cut), S(r_mid)=P(hike), r_mid snapped from FRED DFF.
  First result: mean |gap| ~3-4c (staleness-inflated), tails to 11c.
  `[x]` Extended (2026-07-23): non-front meetings measured (mean|gap| ~6-7c,
  ~2x front — staleness); cuts-count bound implemented as a model-free
  one-sided identity (P(net decline >= k) <= P(cuts >= k)) but NOT TESTABLE
  yet — the cuts market's tail categories are never-traded marks whose
  prices sum to ~2.5; re-checked automatically each run.
- [x] **B10. Regional CPI aggregation.** `analysis/regional_cpi_check.py`
  (2026-07-25): national yoy vs BLS-weighted census-region markets. Regions
  price NSA INDEX LEVELS (own base periods!) — converted to implied yoy via
  FRED CUUR0x00SA0 bases at the front reference month. First result: mean
  gap +0.32pp (constant ≈ weights/base error), demeaned deviations within
  strike granularity, ZERO persistent 24h+ runs → **aggregation-coherent**.
- [x] **B11. Cross-country replication.** Checked coverage: Canada/Eurozone
  Recession markets are dead (1 contract each) and Eurozone Unemployment
  stopped printing in April → only `okun_canada` was viable (added, thin).

## C. Data-driven rule discovery (formalize our own rules)

- [x] **C1. Pair mining with a hard split.** `analysis/discover_rules.py`:
  115-market universe, 6h-change correlations, Bonferroni family-wise screen,
  same-sign p<0.05 OOS confirmation, YAML stubs with provenance for
  survivors. First result: **zero robust pairs** — every strong raw
  correlation sits on a ~25-bar overlap (chance across 814 tested pairs).
  The venue is too thin/asynchronous for data-mined rules yet; rerun as data
  accrues.
- [x] **C2. Factor residuals.** `analysis/factor_residuals.py`: first PC on
  train-standardized 6h changes (loadings frozen at the split), residual-z
  events on the test period vs a magnitude-matched control. First result
  (Jul-23 bundle): factor explains only ~3% of change variance; deviations
  revert but NOT beyond equally-extreme bars (t(diff) ≈ 1.3) → WEAK. Reruns
  in run_all as data accrues.
- [x] **C3. Within-market static-arbitrage scanner.** Monotonicity of the
  survival ladder P(X>K₁) ≥ P(X>K₂) for K₁<K₂ (within expiry!), YES/NO
  parity, persistence of violations. Measured: 1.5–6.5% adjacent-pair
  violation rates within expiry; parity gap mean ~1c. This is the most
  direct evidence on "is there *any* free money" and needs no economic
  theory. → `analysis/arbitrage_scan.py`.
- [x] **C4. Ladder-shape rules.** Done 2026-07-25:
  `signals.implied_quantile_series` (any p; median now a special case) +
  `analysis/unit_spreads.py` — implied headline−core and PCE−CPI wedges in
  %-points vs the realized 10y [p5, p95] band. First result: both wedges
  sit INSIDE their realized bands (0.1% / 4.4% of bars outside, zero
  persistent 24h runs) → coherent in units too. Implied IQRs ~0.02-0.10pp.

## D. Statistical-method upgrades

- [x] **D1. Robustness grid.** Sweep z-window × threshold per rule
  (`--grid` in validate_consistency.py); verdicts must be stable across a
  neighborhood, not one lucky cell. Grid lands in the report.
- [x] **D2. Permutation test.** `validate_consistency.py --permute N` (and
  `run_all.py --permute N`): circular-shift null that keeps each leg's own
  dynamics but destroys cross-leg alignment; statistic = flagged-event mean
  reversion at max horizon. First result: payrolls_labor p≈0.11 — most of
  its "edge" is explained by leg mechanics; observed sits at ~89th
  percentile of the null. The honest stat now lives in the report table.
- [x] **D3. Power analysis.** Given observed event rate (~25 events/93d) and
  effect size, the report estimates how many days of collection are needed
  for a conclusive test — turns "keep collecting" into a number.
- [x] **D4. Release-time event studies.** `analysis/release_event_study.py`
  (2026-07-25). No external calendar needed: contracts EXPIRE on release day,
  so each rule's releases = its leg markets' expirations. Statistic: median
  post/pre |score| ratio (±48h) vs bootstrap random anchors. First result:
  pooled ratio 0.99, NO rule shows release-specific compression —
  inconsistencies decay on their own clock, which argues AGAINST the
  "data resolves mispricing" mechanism and FOR construction noise.
- [x] **D5. Machine-readable outputs.** validate/backtest now write JSON
  summaries (`analysis/validation/*.json`, `analysis/backtest/*.json`) so
  the report generator and future meta-analyses don't scrape stdout.
- [x] **D6. Headline metric = $ vs break-even.** The report leads with
  expected net $ per trade against break-even cost per rule, not z-space
  reversion.

## E. Code structure & quality

- [x] **E1. Shared rule engine.** `analysis/rules.py`: structured YAML logic
  (`product` / `linear` scorers, structured `flag`), panel builder shared by
  run/validate/backtest (was 3 near-copies), no more `validate → run_consistency`
  import chain, no string-parsed `flag_when`.
- [x] **E2. One loader with caching.** `signals.load_history` gains a local
  pickle cache keyed by zip name+mtime (`analysis/cache/`, gitignored):
  ~30s CSV parse → sub-second reloads. No new dependencies.
- [x] **E3. Dedupe utilities.** `find_latest_zip` existed in 3 files → one
  canonical implementation in `signals.py`, re-exported.
- [x] **E4. Scripts runnable from anywhere.** `sys.path` shim so
  `python3 analysis/run_consistency.py` works from repo root and `analysis/`.
- [x] **E5. explore_dataset memory.** Load the 669MB probabilities CSV with
  `usecols` instead of whole-table.
- [x] **E6. Collector tests skip cleanly** when server-side deps
  (pydantic-settings, httpx, psycopg) aren't installed locally, instead of
  erroring the whole suite.
- [x] **E7. Dependencies made explicit.** `requirements-analysis.txt` +
  `[project.optional-dependencies].analysis`; test reqs no longer smuggle
  analysis deps.
- [x] **E8. One-command research run.** `analysis/run_all.py`: readiness →
  all implemented rules → validation (+grid) → backtest → scanner →
  `analysis/report/REPORT.md`. The "does the whole thesis hold" button.
- [x] **E9. Small cleanups.** Unused `align(freq=)` param, duplicated
  constants, dead branches, docstring drift.
- [x] **E10. CI test workflow.** `.github/workflows/tests.yml`: pytest + ruff
  on push/PR (analysis deps only; collector tests skip as locally).
- [x] **E11. README restructure.** Research goal + findings first; collector
  ops second; PDF moved to `docs/`.
- [x] **E12. Lint config.** ruff in pyproject.toml (E4/E7/E9, F, B, UP; repo
  clean as of 2026-07-23) + a Lint step in the tests workflow.

- [x] **A12. Dead-leg detection (found 2026-07-28).** A rule whose leg stops
  printing produced "no events", which the OOS table reported as
  "INCONCLUSIVE (too few events)" — indistinguishable from a live rule that
  simply did not fire. Four of ten rules were in this state (pce_cpi 120d,
  okun 93d, beveridge 19d, sahm/uip 7d since last print). **Fixes:**
  `check_readiness.py` gained a "Rule legs" section (per-leg last print +
  traded-contract count); `oos_test.py` reports `NO OOS DATA (leg stale …)`
  with `has_oos_data`/`panel_end` fields and no longer emits negative
  `oos_days`; the report's OOS table shows the same.
  **Root cause verified against the live VPS DB — NOT a collector bug and NOT
  a delisting:** those markets are actively listed with fresh contracts, but
  the venue returns `no_data` for nearly all of them (US Core PCE: 78 of 78
  active contracts never traded; USDJPY 60/68; JOLTS 57/70). The export only
  carries history-backed contracts, which is why the bundle looked like a
  delisting. Real conclusion: only 5 of 10 rules have a live OOS window.

- [x] **A13. Expiry tracking: measured, and deliberately NOT changed
  (2026-08-03).** `front_expiry_filter` keeps only the nearest unexpired
  expiry and so discards **52–96% of prints**: US Real GDP 269 rows → 10
  (which is the whole reason `okun` never produced a score), Fed Funds keeps
  17%, CPI Yearly 38%. A front expiry that goes quiet mid-life also kills the
  signal while a later expiry trades (US Recession: front silent from Jun-26,
  Dec-10 printing Jul-17).
  Built `active_expiry_filter` + `expiry_mode: front|active` (mappings
  default, per-indicator override, `--expiry-mode` on the per-rule scripts):
  tracks the most-traded unexpired expiry, causal and still one expiry per bar
  so A1 holds. Coverage gains are real (GDP 10 → 258 points, Fed Funds +22%,
  Recession extends Jun-26 → Jul-17).
  **A/B verdict (`analysis/expiry_mode_ab.py`): keep `front`.** `active`
  multiplies expiry switches 4–5× (taylor 10 → 59) and the verdicts move
  incoherently rather than uniformly improving — taylor's genuine cross-leg
  structure evaporates (perm p 0.007 → 0.847) while payrolls_labor swings to
  p = 0.000 and core_headline 0.160 → 0.687. Cause: expiry rolls are
  SYNCHRONISED across legs (both CPI legs roll on the same release), so
  cross-leg-aligned jumps inflate significance in a way the circular-shift
  null cannot reproduce — the A11 hazard at panel scale. `active` stays
  opt-in; re-run the A/B as data grows.
- [x] **A14. Roll buffer skipped for single-expiry markets (2026-08-03).**
  Both expiry filters short-circuited on `len(exps) <= 1` and returned rows
  *past* the roll cutoff, so single-expiry markets (e.g. US Core PCE) kept
  settlement-pinned bars the buffer exists to exclude. Now `== 0`, so the
  cutoff always applies. Pre-existing bug, found by a test written for A13.

- [x] **A15. FDR family restricted to powered rules (2026-08-03).** The BH
  q-values ran over every rule, including ones the validator itself calls
  INCONCLUSIVE. On the Aug-03 bundle that produced `pce_cpi` perm p = 0.005 →
  **q = 0.045 on 2 events** — a significant-looking number in the headline
  table resting on noise (`okun_canada` likewise p = 0.000 on 2 events).
  Rules with < 8 events (the validator's own floor) are now excluded from the
  family and shown as `p (n/a)` with no q.
- [x] **A16. FRED API key leaked into error output (2026-08-03).** The DFF
  vintage request returns HTTP 400 (26k-row daily series), and `requests`
  embeds the full query string — `api_key` included — in the exception
  message, which `collect_fred.py` printed verbatim to stdout/logs. Added
  `redact()` over every printed exception/payload plus a test asserting the
  key never appears. **The existing key appeared in terminal output before
  the fix and should be rotated.**

## F. Data & infrastructure extensions

- [x] **F1. Fresh export.** Bundle was Apr-30 (~70d of data); collector has
  been running through Jul-07 → new export roughly doubles the window and
  provides the OOS segment for A9. (Pulled during this pass; Jul-23 bundle
  pulled 2026-07-23 — ~137d window, findings re-run, conclusions unchanged.)
- [x] **F2. One-command export pulls.** `scripts/refresh_data.sh`: VPS export →
  download → FRED refresh → readiness audit. Host details stay in `.env`
  (gitignored; placeholders in `.env.example`). Scheduling it (cron/CI) is
  left to the operator.
- [~] **F3. Bid/ask capture.** Investigated (2026-07-07): no public REST
  quote endpoint exists — probed `md/snapshot`, `iserver/marketdata/snapshot`,
  `event-contract/{snapshot,market-data,...}` (all 404); the
  `samples/snapshot_response.json` field-code payload (84=bid, 86=ask) is
  from the deferred websocket feed, and the public root redirects to a
  marketing page. Real spreads need the websocket consumer the collector
  README already defers. **Interim: per-market |YES+NO−1| parity gaps as an
  effective-cost proxy** (`arbitrage_scan.py` → `parity_by_market.csv`,
  joined against break-evens in the report): p75 ≈ 1c on every rule market,
  which payrolls_labor's ~4c break-even clears. Real books are likely wider.
  **Route confirmed (2026-07-25):** the public web app is a marketing page
  only (`interactivebrokers.ie/predictionmarkets`); live quotes require an
  AUTHENTICATED IBKR Client Portal session — CP Gateway on the VPS +
  `wss://api.ibkr.com/v1/api/ws` (`smd+conid` with fields 84/86), i.e. real
  account credentials and a session-keepalive job. Deliberate decision, not
  an engineering gap; build only if a rule ever survives the OOS gate.
- [x] **F4. FRED on the VPS timer.** Superseded (2026-07-23): `fred.sqlite`
  is consumed locally, so a VPS timer would also need a sync path back.
  `scripts/refresh_data.sh` (F2) refreshes FRED on every data pull instead.
- [x] **F5. Signal materialization.** `signals.cached_implied_series` /
  `cached_implied_prob_frame` (2026-07-25): per-bundle disk cache keyed by
  bundle name+mtime + every construction parameter + a cache version;
  wired into build_rule_panel, discover_rules, factor_residuals and
  regional_cpi_check. Bump `SIGNALS_CACHE_VERSION` when signal code changes.
- [ ] **F6. Live monitor.** Evaluate implemented rules against the freshest
  data on a timer; alert on new flags. Only worth building if validation
  survives OOS (A9).
- [ ] **F7. Cross-venue extension.** Kalshi/Polymarket price the same macro
  events; same-event redundancy (B9) generalizes to cross-venue arbitrage.
  Out of scope while the project trades only ForecastTrader.

---

## Priority order (research value per effort)

1. A1–A4 signal fixes + re-run existing rules (findings may legitimately change).
2. B1–B8: eight new rules — more independent hypotheses from the same data.
3. C3 scanner: direct arbitrage evidence, no theory needed.
4. F1 fresh export + A9 walk-forward: the single biggest power gain.
5. D1/D3: robustness + "how much data do we need".
6. E8 report: one command → one document with every verdict.
7. B9/B10 identity-class rules (strongest economics, most engineering).
8. C1/C2 rule discovery (the "formalize our own" track) — needs the longer window first.

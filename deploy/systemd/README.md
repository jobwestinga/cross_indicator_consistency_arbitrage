# Systemd Timers

The collector is designed to run as a set of host-level `systemd` timers on the
VPS instead of an in-app scheduler loop.

Generate unit files from the repo root:

```bash
python -m forecast_collector.scheduler \
  --workdir /srv/cross_indicator_consistency_arbitrage \
  --output-dir deploy/systemd/generated
```

That command writes one `.service` and one `.timer` file for each scheduled job:

- `forecast-discover`
- `forecast-structure`
- `forecast-open-interest`
- `forecast-probabilities`
- `forecast-history-incremental`
- `forecast-history-backfill`

Typical install flow on the VPS:

```bash
sudo cp deploy/systemd/generated/* /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now forecast-discover.timer
sudo systemctl enable --now forecast-structure.timer
sudo systemctl enable --now forecast-open-interest.timer
sudo systemctl enable --now forecast-probabilities.timer
sudo systemctl enable --now forecast-history-incremental.timer
sudo systemctl enable --now forecast-history-backfill.timer
```

Each unit runs `docker compose run --rm collector ...` from the configured
working directory, and the collector itself uses PostgreSQL advisory locks to
prevent overlapping runs of the same job type.

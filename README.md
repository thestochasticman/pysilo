# silo

**Cached [SILO](https://www.longpaddock.qld.gov.au/silo/) daily climate
for Australia — fetch once per grid point, never twice.** Every daily
observation this machine ever fetches lands in one SQLite store keyed
by SILO's native 0.05° (~5 km) grid, so repeat requests, nearby farms
in the same cell, and extended date ranges all reuse the same rows.
Part of the [Borevitz Lab](https://borevitzlab.anu.edu.au/) ecosystem.

## How it works

```
{data_root}/silo_store/
└── silo.db
    ├── observations(point, date, variable, value)   # every value ever fetched
    └── coverage(point, start, end)                  # which date spans are populated
```

- Any coordinate snaps deterministically to its nearest SILO grid
  point (~5 km cells — the resolution SILO interpolates at anyway).
- `Store.get_df(lat, lon, start, end)` diffs the requested range
  against the coverage ledger and fetches **only the missing spans**
  from the DataDrill endpoint, then reads the range.
- Coverage records only what SILO actually returned — if the record
  lags behind a requested recent date, the tail stays uncovered and is
  re-requested next time.
- Writes are transactional (SQLite/WAL): a crash mid-fetch leaves the
  span unrecorded, and the next run re-fetches it.

## Usage

The core API is **query-agnostic** — a coordinate and dates:

```python
from datetime import date
from pysilo.store import Store

store = Store()   # email from ~/.config/BorevitzLab.json, or pass email=...

df = store.get_df(-33.516, 148.373, date(2023, 1, 1), date(2023, 12, 31))
#    one row per day: date, daily_rain, max_temp, min_temp, radiation,
#    vp, et_short_crop, ... (18 variables)

store.fill(-33.516, 148.373, date(2023, 1, 1), date(2023, 12, 31))  # → 0: already local
```

Pipelines that speak the shared `borevitz_lab.query.Query` use the
adapters (evaluated at the bbox centre):

```python
df = store.get_df_query(query)
```

`download_silo(query)` remains as a thin wrapper returning the classic
`YYYY-MM-DD`-columned frame.

SILO requires a registration email (sent as the API username) — set
`email` in `~/.config/BorevitzLab.json`, `BOREVITZ_LAB_EMAIL`, or pass
`email=` per call.

## Performance

Live measurements against SILO — one grid point, all 18 variables:

| Scenario | Fetched | Time |
|---|---|---|
| Cold fill — one year (365 days) | 365 days | 0.9 s |
| Same request again | nothing | **0.0 s** |
| Nearby farm, same ~5 km cell | nothing | **0.0 s** |
| Date range extended +6 months | 182 days — *the extension only* | 2.8 s |
| Read cached year (365 × 18) | — | 0.02 s |

Store footprint: **~0.5 MB per point-year** across all variables.
Absolute times vary with network and SILO load; the zeros are the
point — they are ledger lookups, no network involved.

## Install

All lab repos share one conda environment, **`borevitz_lab`** — each
repo's `environment.yml` creates it if missing and adds its own
packages if it exists (never use `--prune`):

```bash
conda env update -n borevitz_lab -f environment.yml
conda activate borevitz_lab
pip install -e ../borevitz_lab   # shared core (not yet on PyPI)
pip install -e .
```

Package design (shared across the lab's packages — no inheritance,
composition only):

- **`Query`** (from `borevitz-lab`) — identity: what region, what dates.
- **`SILO`** (`pysilo.silo`) — config: endpoint, comment codes, variables.
- **`Paths`** (`pysilo.paths`) — derived location of the store for a
  given `Config`.
- **`grid`** — the fixed 0.05° grid (pure, offline-testable math).
- **`Store`** (`pysilo.store`) — ties them together.

## Test

```bash
# offline (pure math + synthetic store):
python pysilo/grid.py     # True
python pysilo/paths.py    # True
python pysilo/store.py    # True

# live (small real fetches from SILO, incl. dedup assertions):
python pysilo/download_silo.py  # True
```

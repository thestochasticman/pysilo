"""One machine-wide SILO climate store that fills itself on demand.

Every daily observation this machine ever fetches lands in a single
SQLite database, keyed by SILO grid point (:mod:`pysilo.grid`) and date:

    {config.tmp_dir}/silo_store/
    └── silo.db
        ├── observations(point, date, variable, value)
        └── coverage(point, start, end)   # which date spans are populated

``Store.get_df(lat, lon, start, end)`` snaps the coordinate to its
~5 km grid point, diffs the requested date range against the coverage
ledger, fetches only the missing spans from the DataDrill endpoint,
then reads the range. Nothing is ever fetched twice — repeat requests,
nearby farms in the same cell, and extended date ranges all reuse the
same rows. Coverage recording is transactional: a crash mid-fetch
leaves the span unrecorded and the next run re-fetches it.
"""
import sqlite3
from attrs import frozen, field
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from os import makedirs
from urllib.request import urlopen

import pandas as pd

from borevitz_lab.config import Config, config as default_config
from pysilo import grid
from pysilo.paths import Paths
from pysilo.silo import SILO, defaultsilo

_SCHEMA = """
CREATE TABLE IF NOT EXISTS observations (
    point    TEXT NOT NULL,
    date     TEXT NOT NULL,
    variable TEXT NOT NULL,
    value    REAL,
    PRIMARY KEY (point, date, variable)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS coverage (
    point TEXT NOT NULL,
    start TEXT NOT NULL,
    end   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS coverage_by_point ON coverage(point);
"""

_DAY = timedelta(days=1)


def missing_spans(covered: list[tuple[date, date]], start: date, end: date) -> list[tuple[date, date]]:
    """Sub-ranges of ``[start, end]`` not covered by any span in ``covered``.

    Pure interval arithmetic (inclusive dates) — the heart of
    "fetch only what's missing", kept free of I/O so it's testable.
    """
    gaps = []
    cur = start
    for s, e in sorted(covered):
        if e < cur:
            continue
        if s > end:
            break
        if s > cur:
            gaps.append((cur, min(end, s - _DAY)))
        cur = max(cur, e + _DAY)
        if cur > end:
            break
    if cur <= end:
        gaps.append((cur, end))
    return gaps


@frozen
class Store:
    """The machine-wide SILO store: one grid, one ledger, zero re-fetches.

    Composed from :class:`borevitz_lab.config.Config` (where the store
    lives, and the SILO registration email) and
    :class:`pysilo.silo.SILO` (endpoint + variables). No inheritance.

    Example:
        ```python
        from datetime import date
        from pysilo.store import Store

        store = Store()
        df = store.get_df(-33.516, 148.373, date(2023, 1, 1), date(2023, 12, 31))
        ```
    """

    config: Config = default_config
    silo: SILO = defaultsilo
    paths: Paths = field(init=False)

    paths.default(lambda s: Paths(s.config))

    def __attrs_post_init__(s):
        makedirs(s.paths.root, exist_ok=True)

    def _db(s) -> sqlite3.Connection:
        db = sqlite3.connect(s.paths.db)
        db.execute('PRAGMA journal_mode=WAL')
        db.executescript(_SCHEMA)
        return db

    # -- fill -------------------------------------------------------------

    def fill(s, lat: float, lon: float, start: date, end: date,
             email: str = None) -> int:
        """Ensure the grid point containing ``(lat, lon)`` is populated for
        ``[start, end]``. Returns the number of days actually fetched —
        0 means full coverage already existed and no network was touched.

        Args:
            lat: Latitude in decimal degrees (EPSG:4326).
            lon: Longitude in decimal degrees.
            start: Inclusive start date.
            end: Inclusive end date.
            email: SILO registration email; falls back to ``config.email``.

        Raises:
            ValueError: If the coordinate is outside SILO's grid, or no
                email is configured when a fetch is required.
        """
        if not grid.in_bounds(lat, lon):
            raise ValueError(f'({lat}, {lon}) is outside the SILO grid')
        pid = grid.point_id(lat, lon)
        db = s._db()
        try:
            covered = [
                (date.fromisoformat(a), date.fromisoformat(b))
                for a, b in db.execute(
                    'SELECT start, end FROM coverage WHERE point = ?', (pid,)
                ).fetchall()
            ]
            gaps = missing_spans(covered, start, end)
            fetched = 0
            for gap_start, gap_end in gaps:
                fetched += s._fetch_span(db, pid, gap_start, gap_end, email)
            return fetched
        finally:
            db.close()

    def _fetch_span(s, db, pid: str, start: date, end: date, email: str = None) -> int:
        """Fetch one contiguous missing span for a grid point and record it."""
        email = email or s.config.email
        if not email:
            raise ValueError('Set email in ~/.config/BorevitzLab.json or pass email parameter')
        slat, slon = (float(v) for v in pid.split(','))
        url = (
            f'{s.silo.base_url}?lat={slat}&lon={slon}'
            f'&start={start.strftime("%Y%m%d")}&finish={end.strftime("%Y%m%d")}'
            f'&format=csv&comment={s.silo.codes}'
            f'&username={email}&password={s.silo.password}'
        )
        text = urlopen(url, timeout=120).read().decode('utf-8')
        try:
            df = pd.read_csv(StringIO(text))
            assert 'YYYY-MM-DD' in df.columns
        except Exception:
            raise RuntimeError(f'SILO returned no data for {pid}: {text[:200]}')

        drop = [c for c in df.columns if c.endswith('_source')]
        df = df.drop(columns=drop + ['metadata', 'latitude', 'longitude'], errors='ignore')
        df = df.rename(columns={'YYYY-MM-DD': 'date'})
        long = df.melt(id_vars=['date'], var_name='variable', value_name='value')

        # Record only what actually came back — if SILO's record lags the
        # requested end (recent dates), the tail stays uncovered and is
        # re-requested next time.
        got_end = min(end, date.fromisoformat(str(df['date'].max())))
        with db:
            db.executemany(
                'INSERT OR REPLACE INTO observations (point, date, variable, value) '
                'VALUES (?, ?, ?, ?)',
                [(pid, str(r.date), r.variable,
                  None if pd.isna(r.value) else float(r.value))
                 for r in long.itertuples()],
            )
            s._record_coverage(db, pid, start, got_end)
        return len(df)

    def _record_coverage(s, db, pid: str, start: date, end: date) -> None:
        """Insert a span and coalesce all overlapping/adjacent spans."""
        spans = [
            (date.fromisoformat(a), date.fromisoformat(b))
            for a, b in db.execute(
                'SELECT start, end FROM coverage WHERE point = ?', (pid,)
            ).fetchall()
        ] + [(start, end)]
        merged = []
        for a, b in sorted(spans):
            if merged and a <= merged[-1][1] + _DAY:
                merged[-1] = (merged[-1][0], max(merged[-1][1], b))
            else:
                merged.append((a, b))
        db.execute('DELETE FROM coverage WHERE point = ?', (pid,))
        db.executemany(
            'INSERT INTO coverage (point, start, end) VALUES (?, ?, ?)',
            [(pid, str(a), str(b)) for a, b in merged],
        )

    # -- read -------------------------------------------------------------

    def get_df(s, lat: float, lon: float, start: date, end: date,
               email: str = None) -> pd.DataFrame:
        """Return the daily climate table for ``(lat, lon)`` x ``[start, end]``,
        fetching only what's missing first.

        Query-agnostic — the data layer of the package. Pipelines that
        speak :class:`borevitz_lab.query.Query` use :meth:`get_df_query`.

        Returns:
            pandas.DataFrame: One row per day, a ``date`` column
            (datetime64) plus one column per climate variable.
        """
        s.fill(lat, lon, start, end, email=email)
        pid = grid.point_id(lat, lon)
        db = s._db()
        try:
            long = pd.read_sql_query(
                'SELECT date, variable, value FROM observations '
                'WHERE point = ? AND date >= ? AND date <= ? ORDER BY date',
                db, params=(pid, str(start), str(end)),
            )
        finally:
            db.close()
        df = long.pivot(index='date', columns='variable', values='value').reset_index()
        df.columns.name = None
        df['date'] = pd.to_datetime(df['date'])
        return df

    # -- Query adapters (the reproducibility layer speaks Query) ----------

    def fill_query(s, query, email: str = None) -> int:
        """:meth:`fill` at the centre of a :class:`borevitz_lab.query.Query`."""
        return s.fill(query.centre_lat, query.centre_lon, query.start, query.end, email=email)

    def get_df_query(s, query, email: str = None) -> pd.DataFrame:
        """:meth:`get_df` at the centre of a :class:`borevitz_lab.query.Query`."""
        return s.get_df(query.centre_lat, query.centre_lon, query.start, query.end, email=email)


# -- offline tests (synthetic rows, no network) -----------------------------

def _tmp_store() -> Store:
    import tempfile
    tmpdir = tempfile.mkdtemp(prefix='silo_store_test_')
    return Store(config=Config(out_dir=tmpdir, tmp_dir=tmpdir))


def _prime(store: Store, pid: str, start: date, end: date, value: float = 1.0):
    """Insert synthetic observations + coverage directly, bypassing the network."""
    db = store._db()
    days = pd.date_range(start, end, freq='D')
    with db:
        db.executemany(
            'INSERT OR REPLACE INTO observations (point, date, variable, value) '
            'VALUES (?, ?, ?, ?)',
            [(pid, str(d.date()), v, value)
             for d in days for v in ('daily_rain', 'max_temp')],
        )
        store._record_coverage(db, pid, start, end)
    db.close()


def test_missing_spans_arithmetic():
    d = date
    full = missing_spans([], d(2024, 1, 1), d(2024, 1, 31))
    none = missing_spans([(d(2024, 1, 1), d(2024, 1, 31))], d(2024, 1, 5), d(2024, 1, 20))
    tail = missing_spans([(d(2024, 1, 1), d(2024, 1, 10))], d(2024, 1, 1), d(2024, 1, 31))
    hole = missing_spans([(d(2024, 1, 1), d(2024, 1, 10)), (d(2024, 1, 21), d(2024, 1, 31))],
                         d(2024, 1, 1), d(2024, 1, 31))
    return (
        full == [(d(2024, 1, 1), d(2024, 1, 31))]
        and none == []
        and tail == [(d(2024, 1, 11), d(2024, 1, 31))]
        and hole == [(d(2024, 1, 11), d(2024, 1, 20))]
    )


def test_coverage_coalesces():
    store = _tmp_store()
    pid = '-33.50,148.35'
    _prime(store, pid, date(2024, 1, 1), date(2024, 1, 10))
    _prime(store, pid, date(2024, 1, 11), date(2024, 1, 20))  # adjacent -> one span
    db = store._db()
    spans = db.execute('SELECT start, end FROM coverage WHERE point = ?', (pid,)).fetchall()
    db.close()
    return spans == [('2024-01-01', '2024-01-20')]


def test_fill_skips_covered_range():
    """Full coverage -> fill() returns 0 without touching the network
    (no email is even required)."""
    store = _tmp_store()
    lat, lon = -33.516, 148.373
    _prime(store, grid.point_id(lat, lon), date(2024, 1, 1), date(2024, 1, 31))
    return store.fill(lat, lon, date(2024, 1, 5), date(2024, 1, 25)) == 0


def test_read_pivots_wide():
    store = _tmp_store()
    lat, lon = -33.516, 148.373
    _prime(store, grid.point_id(lat, lon), date(2024, 1, 1), date(2024, 1, 31), value=7.5)
    df = store.get_df(lat, lon, date(2024, 1, 1), date(2024, 1, 31))
    return (
        len(df) == 31
        and 'daily_rain' in df.columns and 'max_temp' in df.columns
        and float(df['daily_rain'].iloc[0]) == 7.5
    )


def test_nearby_coordinate_shares_point():
    """A coordinate ~1 km away inside the same cell needs no fetch."""
    store = _tmp_store()
    _prime(store, grid.point_id(-33.514, 148.371), date(2024, 1, 1), date(2024, 1, 31))
    return store.fill(-33.516, 148.373, date(2024, 1, 1), date(2024, 1, 31)) == 0


def test():
    return all([
        test_missing_spans_arithmetic(),
        test_coverage_coalesces(),
        test_fill_skips_covered_range(),
        test_read_pivots_wide(),
        test_nearby_coordinate_shares_point(),
    ])


if __name__ == '__main__':
    print(test())

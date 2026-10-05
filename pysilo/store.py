"""One machine-wide SILO climate store that fills itself on demand.

Every daily value this machine ever fetches lands in a single sparse
Zarr store, one ``(time, y, x)`` array per variable on SILO's national
0.05° lattice (:mod:`pysilo.grid`), plus one ``{variable}_source``
array of SILO's per-value provenance codes beside each:

    {config.tmp_dir}/silo_store/
    ├── silo.zarr/
    │   ├── daily_rain            # sparse (time, y, x) float32; only written chunks exist
    │   ├── daily_rain_source     # uint8 SILO source code per value (255 = none)
    │   └── max_temp ...
    ├── ledger/                   # one JSON marker per (8x8 block, year)
    │   └── 017_091/2023.json     # {"through": {"<slot>": "2023-12-31", ...}}
    └── claims/                   # cross-node mutex dirs while a block is being written

SILO is a raster product, but it is served point by point: DataDrill
returns one CSV per grid point per date span, all 18 variables at
once. So the unit of *fetching* is a (point, span) request and the
unit of *storage* is the national raster -- ``get_ds(bbox, start,
end)`` fills every grid point whose cell intersects the bbox and
returns a ``(time, lat, lon)`` cube, never a single point standing in
for an area. ``get_df(lat, lon, ...)`` is the one-point view of the
same store.

The ledger is files, not a database (see ``troi/docs/ledger.md``): on
Gadi's Lustre, file locks are node-local, so SQLite is unsafe when
several PBS jobs share a store, and no server can run there. A block's
marker records, per point, the last date fetched for each year; it is
committed by atomic rename, and a block is written under a
:class:`troi.ledger.Claim` so two jobs never read-modify-write the
same Zarr chunks at once.

Coverage records only what SILO actually returned: if the record lags
a requested recent date, the tail stays uncovered and is re-requested
next time. ``fill`` clamps ``end`` to today. A failed request raises
after the points that did succeed in the same block have been written.
SILO requires a registration email (``config.email`` or the ``email``
argument), sent as the API username.

Nothing is ever resampled here: ``get_ds`` returns native points with
``crs``, ``transform``, ``nodata`` and ``native_res_m`` attrs so a
consumer can regrid reproducibly.
"""
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from os import makedirs
from urllib.request import urlopen

import numpy as np
import pandas as pd
import xarray as xr
import zarr
from attrs import frozen, field

from troi import Config, config as default_config
from troi.ledger import Markers, Claim, ensure_array, Gap, GapReport
from troi.absent import clamp_end
from pysilo import grid
from pysilo.paths import Paths
from pysilo.silo import SILO, defaultsilo

_DAY = timedelta(days=1)
NATIVE_RES_M = 5000
NO_SOURCE = 255                 # uint8 fill for the *_source arrays


def missing_spans(covered: list[tuple[date, date]], start: date, end: date) -> list[tuple[date, date]]:
    """Sub-ranges of ``[start, end]`` not covered by any span in ``covered``.

    Pure interval arithmetic (inclusive dates) -- the heart of
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


def _melt_with_sources(df: pd.DataFrame) -> pd.DataFrame:
    """Long frame ``(date, variable, value, source)`` from a wide DataDrill
    CSV whose ``{variable}_source`` columns sit beside each variable.

    Pure reshaping, no I/O. A variable without a source column (SILO
    omits none, but be safe) gets a NULL source.
    """
    src_cols = [c for c in df.columns if c.endswith('_source')]
    values = df.drop(columns=src_cols).melt(id_vars=['date'], var_name='variable',
                                            value_name='value')
    if not src_cols:
        values['source'] = pd.array([None] * len(values), dtype='Int64')
        return values
    sources = df[['date'] + src_cols].melt(id_vars=['date'], var_name='variable',
                                           value_name='source')
    sources['variable'] = sources['variable'].str.removesuffix('_source')
    return values.merge(sources, on=['date', 'variable'], how='left')


def _block_parts(by: int, bx: int, year: int) -> tuple:
    return (f'{by:03d}_{bx:03d}', str(year))


@frozen
class Store:
    """The machine-wide SILO store: one lattice, one ledger, zero re-fetches.

    Composed from :class:`troi.Config` (where the store lives, and the
    SILO email) and :class:`pysilo.silo.SILO` (endpoint + variables).
    No inheritance.

    Example:
        ```python
        from datetime import date
        from pysilo.store import Store

        store = Store()
        ds = store.get_ds(bbox, date(2023, 1, 1), date(2023, 12, 31))   # (time, lat, lon)
        df = store.get_df(-33.516, 148.373, date(2023, 1, 1), date(2023, 12, 31))
        store.gaps(bbox, date(2023, 1, 1), date(2023, 12, 31)).summary()
        ```
    """

    config: Config = default_config
    silo: SILO = defaultsilo
    workers: int = 4                 # concurrent DataDrill requests (politeness)
    lease_s: float = 900.0           # a block claim older than this is presumed abandoned
    paths: Paths = field(init=False)

    paths.default(lambda s: Paths(s.config))

    def __attrs_post_init__(s):
        makedirs(s.paths.root, exist_ok=True)

    # -- ledger -------------------------------------------------------------

    @property
    def _ledger(s) -> Markers:
        return Markers(s.paths.ledger)

    def _through(s, by: int, bx: int, year: int) -> dict:
        """``{slot: 'YYYY-MM-DD'}`` of the last day fetched per point for a
        (block, year); empty if nothing is recorded."""
        m = s._ledger.read(_block_parts(by, bx, year))
        return dict(m['through']) if m else {}

    def _covered(s, row: int, col: int, years) -> list[tuple[date, date]]:
        """Date spans recorded for a point across ``years``."""
        by, bx = grid.block_of(row, col)
        slot = str(grid.point_slot(row, col))
        spans = []
        for year in years:
            through = s._through(by, bx, year).get(slot)
            if through:
                spans.append((date(year, 1, 1), date.fromisoformat(through)))
        return spans

    def _mark(s, by: int, bx: int, year: int, through: dict) -> None:
        """Merge ``{slot: 'YYYY-MM-DD'}`` into the (block, year) marker,
        keeping the later date per slot. Call only under the block's claim."""
        merged = s._through(by, bx, year)
        for slot, d in through.items():
            if slot not in merged or d > merged[slot]:
                merged[slot] = d
        s._ledger.write(_block_parts(by, bx, year), {'through': merged})

    def _needs(s, points, start: date, end: date) -> dict:
        """``{(row, col): [(gap_start, gap_end), ...]}`` for the points not
        fully covered over ``[start, end]``."""
        years = range(start.year, end.year + 1)
        needs = {}
        for row, col in points:
            gaps = missing_spans(s._covered(row, col, years), start, end)
            if gaps:
                needs[(row, col)] = gaps
        return needs

    def _email(s, email: str = None) -> str:
        email = email or s.config.email
        if not email:
            raise ValueError('Set email in ~/.config/Troi.json or pass email parameter')
        return email

    def _array(s, name: str, source: bool = False, mode: str = 'a') -> zarr.Array:
        root = zarr.open_group(s.paths.store, mode=mode)
        key = f'{name}_source' if source else name
        if mode == 'r':
            return root[key]
        return ensure_array(
            s.paths.root, root, key,
            shape=(grid.NDAYS, grid.HEIGHT, grid.WIDTH),
            chunks=(grid.TCHUNK, grid.BLOCK, grid.BLOCK),
            dtype='uint8' if source else 'float32',
            fill_value=NO_SOURCE if source else np.nan,
        )

    # -- fill -------------------------------------------------------------

    def fill(s, bbox: list[float], start: date, end: date, email: str = None,
             log=None) -> int:
        """Ensure every grid point whose cell intersects ``bbox`` is
        populated for ``[start, end]``, all variables.

        Troi-agnostic. Returns the number of point-days actually fetched;
        0 means full coverage already existed and no network was touched.
        ``end`` is clamped to today. Safe to run from many processes and
        nodes at once: each 8x8 block is written under a
        :class:`troi.ledger.Claim`. ``log`` receives one line per block.

        Raises:
            ValueError: If the bbox misses SILO's grid, or no email is
                configured when a fetch is required.
        """
        end = clamp_end(end)
        start = max(start, grid.EPOCH)
        if end < start:
            return 0
        points = grid.points_in_window(grid.window_for_bbox(bbox))
        blocks = {}
        for p in points:
            blocks.setdefault(grid.block_of(*p), []).append(p)
        fetched = 0
        for (by, bx), bpoints in sorted(blocks.items()):
            if not s._needs(bpoints, start, end):
                continue                                 # full hit: no claim, no email, no network
            email = s._email(email)
            with Claim(s.paths.root, ('silo', by, bx), lease_s=s.lease_s) as claim:
                needs = s._needs(bpoints, start, end)   # re-diff under the claim
                if not needs:
                    continue
                n = s._fill_block(by, bx, needs, email, claim)
            fetched += n
            if log:
                log(f'block {by},{bx}: {n} point-days over {len(needs)} points')
        return fetched

    def _fill_block(s, by: int, bx: int, needs: dict, email: str, claim: Claim) -> int:
        """Fetch every missing span of one block concurrently, write each
        touched Zarr chunk once per variable, then mark the ledger.

        A failed request raises only after the spans that did succeed
        have been written and marked."""
        jobs = [(p, a, b) for p, gaps in needs.items() for a, b in gaps]
        got, errors = [], []
        with claim.keepalive(), ThreadPoolExecutor(max_workers=s.workers) as ex:
            futures = {ex.submit(s._fetch_span_result, p, a, b, email): (p, a, b) for p, a, b in jobs}
            for f in as_completed(futures):
                p, a, b = futures[f]
                r = f.result()
                if isinstance(r, Exception):
                    errors.append(((p, a, b), r))
                else:
                    got.append((p, a, b, r))
        fetched = s._write_block(by, bx, got)
        if errors:
            (p, a, b), e = errors[0]
            raise RuntimeError(
                f'{len(errors)} DataDrill request(s) failed in block {by},{bx} (first: point '
                f'{grid.point_coords(*p)} {a}..{b}); {fetched} point-days from others were written'
            ) from e
        return fetched

    def _write_block(s, by: int, bx: int, got: list) -> int:
        """``got`` is ``[((row, col), span_start, span_end, long_df), ...]``.
        Writes values and sources per variable per time chunk, then the
        ledger. Returns the number of point-days written."""
        if not got:
            return 0
        r0, r1, c0, c1 = grid.block_window(by, bx)
        # Group by variable and time chunk: {var: {tc: [(k, r, c, vals, srcs)]}}
        per_var, through, fetched = {}, {}, 0
        for (row, col), a, b, long in got:
            if long.empty:
                continue
            days = pd.to_datetime(long['date']).dt.date
            idx = np.array([grid.day_index(d) for d in days.unique()])
            fetched += len(idx)
            got_end = min(b, max(days))
            for year in range(a.year, got_end.year + 1):
                through.setdefault(year, {})[str(grid.point_slot(row, col))] = \
                    str(min(got_end, date(year, 12, 31)))
            for var, sub in long.groupby('variable', sort=False):
                k = np.array([grid.day_index(d) for d in pd.to_datetime(sub['date']).dt.date])
                vals = sub['value'].to_numpy(dtype='float32', na_value=np.nan)
                srcs = sub['source'].to_numpy(dtype='float64', na_value=NO_SOURCE).astype('uint8')
                for tc in grid.time_chunks(int(k.min()), int(k.max())):
                    t0, t1 = grid.tchunk_range(tc)
                    m = (k >= t0) & (k < t1)
                    per_var.setdefault(var, {}).setdefault(tc, []).append(
                        (k[m] - t0, row - r0, col - c0, vals[m], srcs[m]))
        for var, chunks in per_var.items():
            for source in (False, True):
                arr = s._array(var, source=source)
                for tc, items in chunks.items():
                    t0, t1 = grid.tchunk_range(tc)
                    block = arr[t0:t1, r0:r1, c0:c1]
                    for k, r, c, vals, srcs in items:
                        block[k, r, c] = srcs if source else vals
                    arr[t0:t1, r0:r1, c0:c1] = block       # zarr 3 commits each chunk by rename
        for year, slots in through.items():
            s._mark(by, bx, year, slots)                  # ...then the ledger says so
        return fetched

    def _fetch_span_result(s, point, start, end, email):
        try:
            return s._fetch_span(point, start, end, email)
        except Exception as e:                            # noqa: BLE001 -- re-raised by caller
            return e

    def _fetch_span(s, point: tuple, start: date, end: date, email: str) -> pd.DataFrame:
        """One DataDrill request for a grid point and date span, all
        variables. Returns the long frame ``(date, variable, value, source)``."""
        slat, slon = grid.point_coords(*point)
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
            raise RuntimeError(f'SILO returned no data for {slat},{slon}: {text[:200]}')
        df = df.drop(columns=['metadata', 'latitude', 'longitude'], errors='ignore')
        df = df.rename(columns={'YYYY-MM-DD': 'date'})
        return _melt_with_sources(df)

    # -- audit --------------------------------------------------------------

    def gaps(s, bbox: list[float], start: date, end: date, today: date = None) -> GapReport:
        """Which point-years of the request are not fully in the store, and
        why. Touches no network. A point-year is present when its ledger
        ``through`` reaches the earlier of the year's end and the clamped
        request end; otherwise ``never_fetched`` (with the recorded
        ``through`` as detail), ``claimed_in_progress`` if another process
        holds the block, ``before_product_start`` for years before 1889,
        ``after_today`` for years starting after today."""
        today = today or datetime.now(timezone.utc).date()
        points = grid.points_in_window(grid.window_for_bbox(bbox))
        years = list(range(start.year, end.year + 1))
        want_end = min(end, today)
        gaps = []
        for row, col in points:
            by, bx = grid.block_of(row, col)
            slot = str(grid.point_slot(row, col))
            claimed = os.path.isdir(Claim(s.paths.root, ('silo', by, bx)).dir)
            for year in years:
                unit = (grid.point_coords(row, col), year)
                if year < grid.EPOCH.year:
                    gaps.append(Gap(unit, 'before_product_start'))
                    continue
                if date(year, 1, 1) > today:
                    gaps.append(Gap(unit, 'after_today'))
                    continue
                through = s._through(by, bx, year).get(slot)
                need = min(want_end, date(year, 12, 31))
                if through and date.fromisoformat(through) >= need:
                    continue
                status = 'claimed_in_progress' if claimed else 'never_fetched'
                gaps.append(Gap(unit, status, detail=f'through {through}' if through else None))
        return GapReport(expected=len(points) * len(years), gaps=tuple(gaps))

    # -- read -------------------------------------------------------------

    def get_ds(s, bbox: list[float], start: date, end: date, variables=None,
               email: str = None, sources: bool = False, log=None) -> xr.Dataset:
        """Return the SILO cube for ``bbox`` x ``[start, end]``, fetching
        only what's missing first.

        Troi-agnostic -- the data layer of the package. Pipelines that
        speak :class:`troi.Troi` use :meth:`get_ds_troi`.

        Args:
            bbox: ``[west, south, east, north]`` in EPSG:4326.
            start, end: Inclusive dates.
            variables: Subset of :attr:`pysilo.silo.SILO.variables`; all by default.
            email: SILO registration email; falls back to ``config.email``.
            sources: Also return ``{variable}_source`` (uint8 SILO
                provenance code; 255 where there is no value).

        Returns:
            xarray.Dataset with dims ``(time, lat, lon)`` on the native
            0.05° lattice (never resampled), one variable per climate
            variable. Days SILO has not published are NaN. Attrs carry the
            georeferencing contract: ``crs``, ``transform`` (six affine
            numbers of this window), ``nodata``, ``native_res_m``.
        """
        variables = tuple(variables) if variables else s.silo.variables
        unknown = set(variables) - set(s.silo.variables)
        if unknown:
            raise ValueError(f'Unknown SILO variable(s): {sorted(unknown)}')
        s.fill(bbox, start, end, email=email, log=log)
        window = grid.window_for_bbox(bbox)
        row0, row1, col0, col1 = window
        i0, i1 = grid.day_index(max(start, grid.EPOCH)), grid.day_index(end)
        lat, lon = grid.coords_for_window(window)
        time = pd.date_range(grid.date_of(i0), end, freq='D')
        root = zarr.open_group(s.paths.store, mode='a')
        data_vars = {}
        for var in variables:
            for source in ((False, True) if sources else (False,)):
                key = f'{var}_source' if source else var
                if key in root:
                    block = root[key][i0:i1 + 1, row0:row1, col0:col1]
                else:
                    fill = NO_SOURCE if source else np.nan
                    block = np.full((i1 - i0 + 1, row1 - row0, col1 - col0), fill,
                                    'uint8' if source else 'float32')
                data_vars[key] = (('time', 'lat', 'lon'), block)
        transform = (grid.STEP, 0.0, grid.X0 + col0 * grid.STEP,
                     0.0, -grid.STEP, grid.Y_TOP - row0 * grid.STEP)
        return xr.Dataset(
            data_vars, coords={'time': time, 'lat': lat, 'lon': lon},
            attrs={'crs': 'EPSG:4326', 'transform': list(transform), 'nodata': None,
                   'native_res_m': NATIVE_RES_M, 'source_nodata': NO_SOURCE,
                   'source': 'SILO DataDrill (Queensland Government)', 'url': s.silo.base_url},
        )

    def get_df(s, lat: float, lon: float, start: date, end: date,
               email: str = None, sources: bool = False) -> pd.DataFrame:
        """Return the daily climate table of the grid point nearest
        ``(lat, lon)`` for ``[start, end]``, fetching only what's missing.

        The one-point view of :meth:`get_ds`.

        Returns:
            pandas.DataFrame: One row per day, a ``date`` column
            (datetime64) plus one column per climate variable, and with
            ``sources=True`` a nullable ``{variable}_source`` beside each.
        """
        if not grid.in_bounds(lat, lon):
            raise ValueError(f'({lat}, {lon}) is outside the SILO grid')
        slat, slon = grid.snap(lat, lon)
        ds = s.get_ds([slon, slat, slon, slat], start, end, email=email, sources=sources)
        df = ds.isel(lat=0, lon=0).drop_vars(['lat', 'lon']).to_dataframe().reset_index()
        df = df.rename(columns={'time': 'date'})
        for c in [c for c in df.columns if c.endswith('_source')]:
            df[c] = df[c].astype('Int64').where(df[c] != NO_SOURCE)
        return df[['date'] + [c for c in df.columns if c != 'date']]

    # -- Troi adapters (the reproducibility layer speaks Troi) ----------

    def fill_troi(s, troi, email: str = None, log=None) -> int:
        """:meth:`fill` over the whole bbox of a :class:`troi.Troi`."""
        return s.fill(troi.bbox, troi.start, troi.end, email=email, log=log)

    def get_ds_troi(s, troi, variables=None, email: str = None, sources: bool = False) -> xr.Dataset:
        """:meth:`get_ds` for a :class:`troi.Troi`."""
        return s.get_ds(troi.bbox, troi.start, troi.end, variables=variables,
                        email=email, sources=sources)

    def get_df_troi(s, troi, email: str = None, sources: bool = False) -> pd.DataFrame:
        """:meth:`get_df` at the centre of a :class:`troi.Troi` -- one
        point, for pipelines that want a table. Use :meth:`get_ds_troi`
        for the whole area."""
        return s.get_df(troi.centre_lat, troi.centre_lon, troi.start, troi.end,
                        email=email, sources=sources)

    def gaps_troi(s, troi) -> GapReport:
        """:meth:`gaps` for a :class:`troi.Troi`."""
        return s.gaps(troi.bbox, troi.start, troi.end)


# -- offline tests (synthetic values, no network) ---------------------------

_TEST_BBOX = [147.30, -35.52, 147.62, -35.10]   # Kyeamba Creek: 7 x 9 points, 2+ blocks
_PT = (-33.516, 148.373)


def _tmp_store(**config_kw) -> Store:
    import tempfile
    tmpdir = tempfile.mkdtemp(prefix='silo_store_test_')
    return Store(config=Config(out_dir=tmpdir, tmp_dir=tmpdir, **config_kw))


def _synthetic_long(start: date, end: date, value: float, source: int = 25,
                    variables=('daily_rain', 'max_temp')) -> pd.DataFrame:
    days = pd.date_range(start, end, freq='D')
    return pd.DataFrame({
        'date': [str(d.date()) for d in days for _ in variables],
        'variable': [v for _ in days for v in variables],
        'value': value, 'source': source,
    })


def _prime(store: Store, bbox, start: date, end: date, value: float = 1.0):
    """Populate every point of bbox for [start, end] directly, bypassing
    the network, through the same block writer a fill uses."""
    points = grid.points_in_window(grid.window_for_bbox(bbox))
    blocks = {}
    for p in points:
        blocks.setdefault(grid.block_of(*p), []).append(p)
    for (by, bx), bpoints in blocks.items():
        store._write_block(by, bx, [(p, start, end, _synthetic_long(start, end, value)) for p in bpoints])


def _fake_fetch(value: float = 7.0, calls=None, lag_to: date = None, fail_points=()):
    """Stand-in for ``Store._fetch_span``: a flat field; ``lag_to`` cuts the
    returned record at that date (SILO's lag); ``fail_points`` raise."""
    def fake(self, point, start, end, email):
        if calls is not None:
            calls.append((point, start, end))
        if point in fail_points:
            raise OSError(f'synthetic failure for {point}')
        stop = min(end, lag_to) if lag_to else end
        return _synthetic_long(start, stop, value) if stop >= start else _synthetic_long(start, start, value).iloc[0:0]
    return fake


class _patched:
    def __init__(self, cls, name, value):
        self.cls, self.name, self.value = cls, name, value

    def __enter__(self):
        self.real = getattr(self.cls, self.name)
        setattr(self.cls, self.name, self.value)

    def __exit__(self, *exc):
        setattr(self.cls, self.name, self.real)


def _leftovers(store: Store) -> list[str]:
    out = []
    for base, dirs, files in os.walk(store.paths.root):
        out += [f for f in files if f.endswith('.tmp') or f.endswith('.partial')]
        if base.endswith('/claims'):
            out += dirs
    return out


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


def test_synthetic_write_read_roundtrip():
    store = _tmp_store()
    _prime(store, _TEST_BBOX, date(2023, 1, 1), date(2023, 1, 10), 3.5)
    ds = store.get_ds(_TEST_BBOX, date(2023, 1, 1), date(2023, 1, 10),
                      variables=('daily_rain', 'max_temp'), sources=True)
    window = grid.window_for_bbox(_TEST_BBOX)
    return (
        ds['daily_rain'].shape == (10, window[1] - window[0], window[3] - window[2])
        and float(ds['daily_rain'].mean()) == 3.5
        and int(ds['daily_rain_source'][0, 0, 0]) == 25
        and ds.lat[0] > ds.lat[-1]
        and bool(np.isnan(store.get_ds(_TEST_BBOX, date(2023, 1, 1), date(2023, 1, 10),
                                       variables=('radiation',))['radiation']).all())
    )


def test_get_ds_attrs_follow_the_contract():
    store = _tmp_store()
    _prime(store, _TEST_BBOX, date(2023, 1, 1), date(2023, 1, 1))
    ds = store.get_ds(_TEST_BBOX, date(2023, 1, 1), date(2023, 1, 1), variables=('daily_rain',))
    a, b, c, d, e, f = ds.attrs['transform']
    return (ds.attrs['crs'] == 'EPSG:4326' and ds.attrs['native_res_m'] == 5000
            and abs(c + 0.5 * a - float(ds.lon[0])) < 1e-9
            and abs(f + 0.5 * e - float(ds.lat[0])) < 1e-9
            and abs(float(ds.lon[0]) / grid.STEP - round(float(ds.lon[0]) / grid.STEP)) < 1e-9)


def test_get_df_is_the_one_point_view():
    store = _tmp_store()
    lat, lon = _PT
    _prime(store, [lon, lat, lon, lat], date(2023, 1, 1), date(2023, 1, 31), 9.0)
    df = store.get_df(lat, lon, date(2023, 1, 1), date(2023, 1, 31), sources=True)
    near = store.get_df(-33.514, 148.371, date(2023, 1, 1), date(2023, 1, 31))   # same cell
    return (len(df) == 31 and 'daily_rain' in df.columns and float(df['daily_rain'].iloc[0]) == 9.0
            and str(df['date'].dtype).startswith('datetime64')
            and int(df['daily_rain_source'].iloc[0]) == 25 and df['radiation_source'].isna().all()
            and float(near['max_temp'].iloc[-1]) == 9.0)


def test_fill_skips_populated_points():
    store = _tmp_store()
    _prime(store, _TEST_BBOX, date(2023, 1, 1), date(2023, 3, 31))
    return store.fill(_TEST_BBOX, date(2023, 1, 5), date(2023, 2, 20)) == 0


def test_fill_fetches_only_missing_spans_per_point():
    """Half the points primed for January; a fill for Jan-Feb requests
    February for them and Jan-Feb for the rest, once each."""
    store = _tmp_store(email='test@example.com')
    west = [147.30, -35.52, 147.45, -35.10]
    _prime(store, west, date(2023, 1, 1), date(2023, 1, 31), 1.0)
    calls = []
    with _patched(Store, '_fetch_span', _fake_fetch(2.0, calls=calls)):
        n = store.fill(_TEST_BBOX, date(2023, 1, 1), date(2023, 2, 28))
        again = store.fill(_TEST_BBOX, date(2023, 1, 1), date(2023, 2, 28))
    pts_all = set(grid.points_in_window(grid.window_for_bbox(_TEST_BBOX)))
    pts_west = set(grid.points_in_window(grid.window_for_bbox(west)))
    spans = {}
    for p, a, b in calls:
        spans.setdefault(p, []).append((a, b))
    ds = store.get_ds(_TEST_BBOX, date(2023, 1, 1), date(2023, 2, 28), variables=('daily_rain',))
    return (
        again == 0 and set(spans) == pts_all
        and all(spans[p] == [(date(2023, 2, 1), date(2023, 2, 28))] for p in pts_west)
        and all(spans[p] == [(date(2023, 1, 1), date(2023, 2, 28))] for p in pts_all - pts_west)
        and n == 28 * len(pts_west) + 59 * len(pts_all - pts_west)
        and not np.isnan(ds['daily_rain'].values).any()
        and _leftovers(store) == []
    )


def test_lagging_record_leaves_tail_uncovered():
    """SILO returns data only to Jan 20: coverage stops there, the tail is
    re-asked next time, and gaps() reports the point-year as incomplete."""
    store = _tmp_store(email='test@example.com')
    lat, lon = _PT
    bbox = [lon, lat, lon, lat]
    calls = []
    with _patched(Store, '_fetch_span', _fake_fetch(1.0, calls=calls, lag_to=date(2023, 1, 20))):
        n1 = store.fill(bbox, date(2023, 1, 1), date(2023, 1, 31))
        r = store.gaps(bbox, date(2023, 1, 1), date(2023, 1, 31), today=date(2023, 2, 1))
        n2 = store.fill(bbox, date(2023, 1, 1), date(2023, 1, 31))
    return (n1 == 20 and n2 == 0 and calls[1][1:] == (date(2023, 1, 21), date(2023, 1, 31))
            and r.counts['never_fetched'] == 1 and r.gaps[0].detail == 'through 2023-01-20')


def test_one_failed_point_does_not_discard_its_block():
    """One request of a block fails: the block's other points are still
    written and marked, then the error is raised."""
    store = _tmp_store(email='test@example.com')
    r0, r1, c0, c1 = grid.block_window(*grid.block_of(*grid.point_index(*_PT)))
    (south, west), (north, east) = grid.point_coords(r1 - 1, c0), grid.point_coords(r0, c1 - 1)
    bbox = [west, south, east, north]                       # exactly one 8x8 block
    pts = grid.points_in_window(grid.window_for_bbox(bbox))
    with _patched(Store, '_fetch_span', _fake_fetch(1.0, fail_points=(pts[0],))):
        try:
            store.fill(bbox, date(2023, 1, 1), date(2023, 1, 5))
            return False
        except RuntimeError as e:
            raised = isinstance(e.__cause__, OSError)
    r = store.gaps(bbox, date(2023, 1, 1), date(2023, 1, 5), today=date(2023, 2, 1))
    return (raised and len(pts) == 64 and r.expected == 64
            and r.counts['never_fetched'] == 1 and r.gaps[0].unit[0] == grid.point_coords(*pts[0])
            and _leftovers(store) == [])


def test_end_is_clamped_to_today():
    store = _tmp_store(email='test@example.com')
    today = datetime.now(timezone.utc).date()
    lat, lon = _PT
    calls = []
    with _patched(Store, '_fetch_span', _fake_fetch(1.0, calls=calls)):
        store.fill([lon, lat, lon, lat], today - timedelta(days=2), today + timedelta(days=30))
    return [c[1:] for c in calls] == [(today - timedelta(days=2), today)]


def _worker_fill(tmp_dir, bbox, start, end, value):
    store = Store(config=Config(out_dir=tmp_dir, tmp_dir=tmp_dir, email='test@example.com'))
    with _patched(Store, '_fetch_span', _fake_fetch(value)):
        store.fill(bbox, start, end)


def test_two_processes_fill_overlapping_ranges():
    import multiprocessing as mp
    store = _tmp_store()
    tmp = store.config.tmp_dir
    west = [147.30, -35.52, 147.50, -35.10]
    east = [147.40, -35.52, 147.62, -35.10]
    ctx = mp.get_context('fork')
    ps = [ctx.Process(target=_worker_fill, args=(tmp, west, date(2023, 1, 1), date(2023, 1, 31), 1.0)),
          ctx.Process(target=_worker_fill, args=(tmp, east, date(2023, 1, 1), date(2023, 1, 31), 2.0))]
    for p in ps:
        p.start()
    for p in ps:
        p.join(timeout=300)
    r = store.gaps(_TEST_BBOX, date(2023, 1, 1), date(2023, 1, 31), today=date(2023, 2, 1))
    ds = store.get_ds(_TEST_BBOX, date(2023, 1, 1), date(2023, 1, 31), variables=('daily_rain',))
    vals = ds['daily_rain'].values
    return (all(p.exitcode == 0 for p in ps) and r.complete
            and not np.isnan(vals).any() and set(np.unique(vals)) <= {1.0, 2.0}
            and _leftovers(store) == [])


def test_gaps_classifies_every_status():
    store = _tmp_store()
    lat, lon = _PT
    bbox = [lon, lat, lon, lat]
    _prime(store, bbox, date(2023, 1, 1), date(2023, 12, 31))
    row, col = grid.point_index(lat, lon)
    held = Claim(store.paths.root, ('silo', *grid.block_of(row, col))).acquire()
    try:
        r = store.gaps(bbox, date(1888, 1, 1), date(2025, 12, 31), today=date(2024, 6, 1))
    finally:
        held.release()
    c = r.counts
    r2 = store.gaps(bbox, date(2022, 1, 1), date(2023, 12, 31), today=date(2024, 6, 1))
    return (r.expected == 2025 - 1888 + 1
            and c['before_product_start'] == 1 and c['after_today'] == 1          # 1888, 2025
            and c['claimed_in_progress'] == 2023 - 1889 + 1                       # 1889..2022, 2024
            and c['never_fetched'] == 0
            and r2.counts['never_fetched'] == 1 and not r2.complete)


def test_out_of_bounds_raises():
    store = _tmp_store()
    try:
        store.fill([100.0, -33.5, 101.0, -33.0], date(2023, 1, 1), date(2023, 1, 2))
    except ValueError:
        return True
    return False


def test_missing_email_raises_before_network():
    store = _tmp_store()      # config with no email
    try:
        store.fill(_TEST_BBOX, date(2023, 1, 1), date(2023, 1, 2))
    except ValueError as e:
        return 'email' in str(e)
    return False


def test():
    return all([
        test_missing_spans_arithmetic(),
        test_synthetic_write_read_roundtrip(),
        test_get_ds_attrs_follow_the_contract(),
        test_get_df_is_the_one_point_view(),
        test_fill_skips_populated_points(),
        test_fill_fetches_only_missing_spans_per_point(),
        test_lagging_record_leaves_tail_uncovered(),
        test_one_failed_point_does_not_discard_its_block(),
        test_end_is_clamped_to_today(),
        test_two_processes_fill_overlapping_ranges(),
        test_gaps_classifies_every_status(),
        test_out_of_bounds_raises(),
        test_missing_email_raises_before_network(),
    ])


if __name__ == '__main__':
    print(test())

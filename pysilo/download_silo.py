"""Fetch the SILO daily climate table for a troi — via the machine-wide store.

Thin compatibility wrapper: the heavy lifting (grid snapping, span
diffing, coverage ledger) lives in :class:`pysilo.store.Store`. Kept as
a module so the familiar ``download_silo(troi)`` entry point survives.
"""
import pandas as pd
from troi import Troi
from pysilo.silo import SILO, defaultsilo


def download_silo(troi: Troi, email: str = None, silo: SILO = defaultsilo,
                  sources: bool = False) -> pd.DataFrame:
    """Return SILO daily climate for the centre of ``troi.bbox`` -- one
    point, as a table. For the whole area use
    :meth:`pysilo.store.Store.get_ds_troi`.

    Fetches only the date spans of the grid point that no previous
    request has covered — repeat, nearby, and extended queries
    re-download nothing.

    Args:
        troi: The :class:`troi.Troi` (centre + date range).
        email: SILO registration email; falls back to ``config.email``.
        silo: Endpoint/variable configuration; defaults to the bundled one.
        sources: If True, include SILO's ``{variable}_source`` provenance
            columns (see :meth:`pysilo.store.Store.get_df`).

    Returns:
        pandas.DataFrame: One row per day with a ``YYYY-MM-DD`` column and
        one column per climate variable.
    """
    from pysilo.store import Store
    store = Store(config=troi.config, silo=silo)
    df = store.get_df_troi(troi, email=email, sources=sources)
    return df.rename(columns={'date': 'YYYY-MM-DD'})


def test_live_fetch_and_dedup():
    """Live: cold fetch returns a full year for one point; identical and
    nearby repeats fetch nothing; a bbox fill fetches every point once."""
    import tempfile
    from datetime import date
    from troi import Config, config
    from pysilo.store import Store

    tmpdir = tempfile.mkdtemp(prefix='silo_live_test_')
    cfg = Config(out_dir=tmpdir, tmp_dir=tmpdir, email=config.email or 'yasaradeel@gmail.com')
    store = Store(config=cfg)
    lat, lon = -33.516, 148.373
    point = [lon, lat, lon, lat]

    fetched = store.fill(point, date(2023, 1, 1), date(2023, 12, 31))
    if fetched < 365:
        return False
    df = store.get_df(lat, lon, date(2023, 1, 1), date(2023, 12, 31))
    if len(df) != 365 or 'daily_rain' not in df.columns:
        return False
    # identical repeat -> nothing
    if store.fill(point, date(2023, 1, 1), date(2023, 12, 31)) != 0:
        return False
    # a coordinate a few hundred metres away inside the same ~5 km cell -> nothing
    if store.fill([148.371, -33.514, 148.371, -33.514], date(2023, 6, 1), date(2023, 6, 30)) != 0:
        return False
    # extend six months -> only the extension is fetched
    extended = store.fill(point, date(2023, 1, 1), date(2024, 6, 30))
    if not 0 < extended <= 182:
        return False
    # provenance came back with the data: every variable has a source code
    src = store.get_df(lat, lon, date(2023, 1, 1), date(2023, 1, 31), sources=True)
    if not (src['daily_rain_source'].notna().all() and src['max_temp_source'].notna().all()):
        return False
    # a small bbox is a raster: every point fetched, once, and audited complete
    bbox = [148.30, -33.55, 148.45, -33.45]
    n = store.fill(bbox, date(2023, 6, 1), date(2023, 6, 30))
    ds = store.get_ds(bbox, date(2023, 6, 1), date(2023, 6, 30), variables=('daily_rain',))
    npts = ds['daily_rain'].shape[1] * ds['daily_rain'].shape[2]
    report = store.gaps(bbox, date(2023, 6, 1), date(2023, 6, 30))
    return (npts > 1 and n == 30 * (npts - 1) and store.fill(bbox, date(2023, 6, 1), date(2023, 6, 30)) == 0
            and report.complete and not ds['daily_rain'].isnull().any())


def test():
    return test_live_fetch_and_dedup()


if __name__ == '__main__':
    print(test())

"""Fetch the SILO daily climate table for a query — via the machine-wide store.

Thin compatibility wrapper: the heavy lifting (grid snapping, span
diffing, coverage ledger) lives in :class:`silo.store.Store`. Kept as
a module so the familiar ``download_silo(query)`` entry point survives.
"""
import pandas as pd
from borevitz_lab.query import Query
from silo.datadrill import SILO, defaultsilo


def download_silo(query: Query, email: str = None, silo: SILO = defaultsilo) -> pd.DataFrame:
    """Return SILO daily climate for the centre of ``query.bbox``.

    Fetches only the date spans of the grid point that no previous
    request has covered — repeat, nearby, and extended queries
    re-download nothing.

    Args:
        query: The :class:`borevitz_lab.query.Query` (centre + date range).
        email: SILO registration email; falls back to ``config.email``.
        silo: Endpoint/variable configuration; defaults to the bundled one.

    Returns:
        pandas.DataFrame: One row per day with a ``YYYY-MM-DD`` column and
        one column per climate variable.
    """
    from silo.store import Store
    store = Store(config=query.config, silo=silo)
    df = store.get_df_query(query, email=email)
    return df.rename(columns={'date': 'YYYY-MM-DD'})


def test_live_fetch_and_dedup():
    """Live: cold fetch returns a full year; identical and nearby repeats
    fetch nothing."""
    import tempfile
    from datetime import date
    from borevitz_lab.config import Config
    from silo.store import Store

    tmpdir = tempfile.mkdtemp(prefix='silo_live_test_')
    cfg = Config(out_dir=tmpdir, tmp_dir=tmpdir, email='yasaradeel@gmail.com')
    store = Store(config=cfg)
    lat, lon = -33.516, 148.373

    fetched = store.fill(lat, lon, date(2023, 1, 1), date(2023, 12, 31))
    if fetched < 365:
        return False
    df = store.get_df(lat, lon, date(2023, 1, 1), date(2023, 12, 31))
    if len(df) != 365 or 'daily_rain' not in df.columns:
        return False
    # identical repeat -> nothing
    if store.fill(lat, lon, date(2023, 1, 1), date(2023, 12, 31)) != 0:
        return False
    # a coordinate a few hundred metres away inside the same ~5 km cell -> nothing
    if store.fill(-33.514, 148.371, date(2023, 6, 1), date(2023, 6, 30)) != 0:
        return False
    # extend six months -> only the extension is fetched
    extended = store.fill(lat, lon, date(2023, 1, 1), date(2024, 6, 30))
    return 0 < extended <= 182


def test():
    return test_live_fetch_and_dedup()


if __name__ == '__main__':
    print(test())

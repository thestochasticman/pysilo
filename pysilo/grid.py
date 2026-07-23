"""The fixed SILO grid every stored climate record is keyed to.

SILO interpolates station data onto a 0.05° (~5 km) national grid
covering Australia (lon 112–154 E, lat 10–44 S). Any requested
coordinate snaps deterministically to its nearest grid point, which is
what makes the store dedup-able: two farms inside the same ~5 km cell
resolve to the same point, and a point's record is only ever fetched
once per date span.

All functions here are pure — no I/O, no store access.
"""

STEP = 0.05                     # degrees between grid points
LON_MIN, LON_MAX = 112.0, 154.0
LAT_MIN, LAT_MAX = -44.0, -10.0


def snap(lat: float, lon: float) -> tuple[float, float]:
    """Nearest SILO grid point ``(lat, lon)`` to the requested coordinate."""
    return (round(lat / STEP) * STEP, round(lon / STEP) * STEP)


def point_id(lat: float, lon: float) -> str:
    """Stable string key for the grid point containing ``(lat, lon)``."""
    slat, slon = snap(lat, lon)
    return f'{slat:.2f},{slon:.2f}'


def in_bounds(lat: float, lon: float) -> bool:
    """True iff the coordinate lies inside SILO's national grid."""
    return LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX


def test_snap_is_idempotent():
    slat, slon = snap(-33.51606, 148.37265)
    return snap(slat, slon) == (slat, slon)


def test_nearby_points_share_a_cell():
    # Two coordinates ~1 km apart inside one ~5 km cell -> same point
    return point_id(-33.514, 148.371) == point_id(-33.516, 148.373)


def test_distant_points_differ():
    return point_id(-33.51, 148.37) != point_id(-33.51, 148.47)


def test_bounds():
    return in_bounds(-33.5, 148.4) and not in_bounds(-33.5, 100.0)


def test():
    return all([
        test_snap_is_idempotent(),
        test_nearby_points_share_a_cell(),
        test_distant_points_differ(),
        test_bounds(),
    ])


if __name__ == '__main__':
    print(test())

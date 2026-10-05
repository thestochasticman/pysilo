"""The fixed SILO lattice every stored climate value is keyed to.

SILO interpolates station data onto a 0.05° (~5 km) national grid
covering Australia: 841 columns from 112.00 E to 154.00 E and 681 rows
from 10.00 S to 44.00 S, with the grid *points* at exact multiples of
0.05°. The store treats each point as the centre of a 0.05° cell, so
the raster's pixel-edge origin is (111.975, -9.975). Any bbox maps
deterministically to a window of points, which is what makes the store
dedup-able: overlapping AOIs resolve to overlapping point sets, and a
point's series is only ever fetched once per date.

Points are grouped into 8 x 8 blocks (0.4°, ~40 km): the unit the
ledger marks and the unit a fill claims, so two jobs filling different
blocks never contend.

The time axis starts at SILO's first day, 1889-01-01. All functions
here are pure -- no I/O, no store access.
"""
from datetime import date, timedelta

STEP = 0.05                     # degrees between grid points
LON_MIN, LON_MAX = 112.0, 154.0
LAT_MIN, LAT_MAX = -44.0, -10.0
WIDTH = round((LON_MAX - LON_MIN) / STEP) + 1      # 841 columns
HEIGHT = round((LAT_MAX - LAT_MIN) / STEP) + 1     # 681 rows (row 0 at 10 S)
X0, Y_TOP = LON_MIN - STEP / 2, LAT_MAX + STEP / 2  # pixel-edge origin
TRANSFORM = (STEP, 0.0, X0, 0.0, -STEP, Y_TOP)      # rasterio order (a b c d e f)

BLOCK = 8                       # points per block edge (ledger + claim unit)
TCHUNK = 366                    # days per zarr time chunk
EPOCH = date(1889, 1, 1)        # day index 0: SILO's first day
HORIZON = date(2049, 12, 31)
NDAYS = (HORIZON - EPOCH).days + 1


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


def point_index(lat: float, lon: float) -> tuple[int, int]:
    """``(row, col)`` of the grid point nearest ``(lat, lon)``."""
    slat, slon = snap(lat, lon)
    return (round((LAT_MAX - slat) / STEP), round((slon - LON_MIN) / STEP))


def point_coords(row: int, col: int) -> tuple[float, float]:
    """``(lat, lon)`` of a grid point."""
    return (round(LAT_MAX - row * STEP, 2), round(LON_MIN + col * STEP, 2))


def day_index(d: date) -> int:
    """Index of ``d`` on the time axis (0 = 1889-01-01)."""
    i = (d - EPOCH).days
    if not 0 <= i < NDAYS:
        raise ValueError(f'{d} is outside the store time axis {EPOCH}..{HORIZON}')
    return i


def date_of(i: int) -> date:
    """Inverse of :func:`day_index`."""
    return EPOCH + timedelta(days=int(i))


def window_for_bbox(bbox: list[float]) -> tuple[int, int, int, int]:
    """Point window ``(row0, row1, col0, col1)`` (half-open) of every grid
    point whose 0.05° cell intersects ``bbox``, clipped to the grid."""
    west, south, east, north = bbox
    col0 = max(0, int((west - X0) / STEP // 1))
    col1 = min(WIDTH, -int(-((east - X0) / STEP) // 1))
    row0 = max(0, int((Y_TOP - north) / STEP // 1))
    row1 = min(HEIGHT, -int(-((Y_TOP - south) / STEP) // 1))
    if col1 <= col0 or row1 <= row0:
        raise ValueError(f'bbox {bbox} does not intersect the SILO grid')
    return (row0, row1, col0, col1)


def points_in_window(window: tuple[int, int, int, int]) -> list[tuple[int, int]]:
    """All ``(row, col)`` points in a window."""
    row0, row1, col0, col1 = window
    return [(r, c) for r in range(row0, row1) for c in range(col0, col1)]


def block_of(row: int, col: int) -> tuple[int, int]:
    """Block id ``(by, bx)`` holding a point."""
    return (row // BLOCK, col // BLOCK)


def block_window(by: int, bx: int) -> tuple[int, int, int, int]:
    """Point window of one block, clipped to the grid (edge blocks are partial)."""
    return (by * BLOCK, min(HEIGHT, (by + 1) * BLOCK),
            bx * BLOCK, min(WIDTH, (bx + 1) * BLOCK))


def point_slot(row: int, col: int) -> int:
    """Index 0..63 of a point inside its block (the ledger's key for it)."""
    return (row % BLOCK) * BLOCK + (col % BLOCK)


def time_chunks(i0: int, i1: int) -> list[int]:
    """Time-chunk ids covering day indices ``[i0, i1]`` inclusive."""
    return list(range(i0 // TCHUNK, i1 // TCHUNK + 1))


def tchunk_range(tc: int) -> tuple[int, int]:
    """Day-index range ``[t0, t1)`` of a time chunk, clipped to the axis."""
    return (tc * TCHUNK, min(NDAYS, (tc + 1) * TCHUNK))


def coords_for_window(window: tuple[int, int, int, int]):
    """Grid-point coordinate arrays ``(lat, lon)`` for a window (lat descending)."""
    import numpy as np
    row0, row1, col0, col1 = window
    lon = LON_MIN + np.arange(col0, col1) * STEP
    lat = LAT_MAX - np.arange(row0, row1) * STEP
    return lat, lon


# -- offline tests ------------------------------------------------------------

def test_snap_is_idempotent():
    slat, slon = snap(-33.51606, 148.37265)
    return snap(slat, slon) == (slat, slon)


def test_nearby_points_share_a_cell():
    # ~1 km apart inside one ~5 km cell -> same point
    return point_id(-33.514, 148.371) == point_id(-33.516, 148.373)


def test_point_index_roundtrip():
    r, c = point_index(-33.516, 148.373)
    lat, lon = point_coords(r, c)
    return (lat, lon) == snap(-33.516, 148.373) and (r, c) == point_index(lat, lon)


def test_window_covers_bbox_and_a_point_bbox_is_one_point():
    w = window_for_bbox([147.30, -35.52, 147.62, -35.10])
    lat, lon = coords_for_window(w)
    one = window_for_bbox([148.373, -33.516, 148.373, -33.516])
    return (lon[0] <= 147.30 + STEP / 2 and lon[-1] >= 147.62 - STEP / 2
            and lat[0] >= -35.10 - STEP / 2 and lat[-1] <= -35.52 + STEP / 2
            and lat[0] > lat[-1] and len(points_in_window(one)) == 1
            and points_in_window(one)[0] == point_index(-33.516, 148.373))


def test_grid_corners():
    return (point_index(LAT_MAX, LON_MIN) == (0, 0)
            and point_index(LAT_MIN, LON_MAX) == (HEIGHT - 1, WIDTH - 1)
            and (HEIGHT, WIDTH) == (681, 841))


def test_blocks_nest():
    r, c = point_index(-33.516, 148.373)
    by, bx = block_of(r, c)
    r0, r1, c0, c1 = block_window(by, bx)
    return r0 <= r < r1 and c0 <= c < c1 and 0 <= point_slot(r, c) < BLOCK * BLOCK


def test_time_axis():
    return (day_index(EPOCH) == 0 and date_of(day_index(date(2023, 6, 1))) == date(2023, 6, 1)
            and tchunk_range(0) == (0, TCHUNK))


def test_bounds():
    ok = in_bounds(-33.5, 148.4) and not in_bounds(-33.5, 100.0)
    try:
        window_for_bbox([100.0, -33.5, 101.0, -33.0])
        return False
    except ValueError:
        return ok


def test():
    return all([
        test_snap_is_idempotent(),
        test_nearby_points_share_a_cell(),
        test_point_index_roundtrip(),
        test_window_covers_bbox_and_a_point_bbox_is_one_point(),
        test_grid_corners(),
        test_blocks_nest(),
        test_time_axis(),
        test_bounds(),
    ])


if __name__ == '__main__':
    print(test())

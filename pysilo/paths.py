"""Derived on-disk locations of the machine-wide SILO store.

The store is keyed by :class:`troi.Config` (one store per
data root, shared by every request on this machine). Rule of thumb
across the lab's packages: user-settable inputs → Config, derived
locations → Paths. No inheritance — composition only.
"""
from attrs import frozen, field
from troi import Config, config as default_config


@frozen
class Paths:
    """Where the pysilo store lives for a given Config.

    Attributes:
        config: The :class:`troi.Config` supplying the data root (and
            the SILO registration email).
        root: Store directory (``{config.tmp_dir}/silo_store``). Cross-node
            claims live under ``{root}/claims`` (see :mod:`troi.ledger`).
        store: The sparse Zarr store -- one ``(time, y, x)`` array per
            variable on the national 0.05° lattice, plus one ``_source``
            array of SILO's provenance codes beside each.
        ledger: Marker tree of populated point-years:
            ``ledger/{by:03d}_{bx:03d}/{year}.json`` holding, per point
            slot in the block, the last date fetched for that year.

    Example:
        ```python
        from pysilo.paths import Paths

        Paths().store  # '~/Downloads/Troi-Tmp/silo_store/silo.zarr'
        ```
    """

    config: Config = default_config

    root: str = field(init=False)
    store: str = field(init=False)
    ledger: str = field(init=False)

    root.default(lambda s: f'{s.config.tmp_dir}/silo_store')
    store.default(lambda s: f'{s.root}/silo.zarr')
    ledger.default(lambda s: f'{s.root}/ledger')


def test_paths_derive_from_config():
    import tempfile
    tmpdir = tempfile.mkdtemp(prefix='silo_paths_test_')
    cfg = Config(out_dir=tmpdir, tmp_dir=tmpdir)
    paths = Paths(cfg)
    return (
        paths.root == f'{tmpdir}/silo_store'
        and paths.store == f'{tmpdir}/silo_store/silo.zarr'
        and paths.ledger == f'{tmpdir}/silo_store/ledger'
    )


def test():
    return test_paths_derive_from_config()


if __name__ == '__main__':
    print(test())

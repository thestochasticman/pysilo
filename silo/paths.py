"""Derived on-disk location of the machine-wide SILO store.

The store is keyed by :class:`borevitz_lab.config.Config` (one store per
data root, shared by every request on this machine). Rule of thumb
across the lab's packages: user-settable inputs → Config, derived
locations → Paths. No inheritance — composition only.
"""
from attrs import frozen, field
from borevitz_lab.config import Config, config as default_config


@frozen
class Paths:
    """Where the silo store lives for a given Config.

    Attributes:
        config: The :class:`borevitz_lab.config.Config` supplying the data root.
        root: Store directory (``{config.tmp_dir}/silo_store``).
        db: The SQLite database holding every observation and the
            coverage ledger.

    Example:
        ```python
        from silo.paths import Paths

        Paths().db  # '~/Downloads/BorevitzLab-Tmp/silo_store/silo.db'
        ```
    """

    config: Config = default_config

    root: str = field(init=False)
    db: str = field(init=False)

    root.default(lambda s: f'{s.config.tmp_dir}/silo_store')
    db.default(lambda s: f'{s.root}/silo.db')


def test_paths_derive_from_config():
    import tempfile
    tmpdir = tempfile.mkdtemp(prefix='silo_paths_test_')
    cfg = Config(out_dir=tmpdir, tmp_dir=tmpdir)
    paths = Paths(cfg)
    return (
        paths.root == f'{tmpdir}/silo_store'
        and paths.db == f'{tmpdir}/silo_store/silo.db'
    )


def test():
    return test_paths_derive_from_config()


if __name__ == '__main__':
    print(test())

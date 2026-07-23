from attrs import frozen

@frozen
class SILO:
    """Endpoint and variable configuration for SILO DataDrill requests."""

    base_url: str = 'https://www.longpaddock.qld.gov.au/cgi-bin/silo/DataDrillDataset.php'
    # Comment codes requesting every available variable in one call.
    codes: str = 'RXNJVDESCLFTAPWMHG'
    password: str = 'apirequest'
    variables: tuple[str, ...] = (
        'daily_rain',
        'max_temp',
        'min_temp',
        'radiation',
        'vp',
        'vp_deficit',
        'evap_pan',
        'evap_syn',
        'evap_comb',
        'evap_morton_lake',
        'et_short_crop',
        'et_tall_crop',
        'et_morton_actual',
        'et_morton_potential',
        'et_morton_wet',
        'mslp',
        'rh_tmax',
        'rh_tmin',
    )

defaultsilo = SILO()

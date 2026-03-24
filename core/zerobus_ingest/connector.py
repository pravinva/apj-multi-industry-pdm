"""
Wrapper for unified-ot-zerobus-connector.
Run when USE_SIMULATOR=false and OT network access is available.
"""

from core.config.loader import load_config


def start_connector(industry: str, catalog: str, spark):
    config = load_config(industry)
    isa95_levels = [l["key"] for l in config["isa95_hierarchy"]["levels"]]

    from zerobus import ISA95Mapper, ZerobusConnector

    endpoint = dbutils.secrets.get("ot-pdm", "OT_ENDPOINT")  # noqa: F821
    protocol = dbutils.secrets.get("ot-pdm", "OT_PROTOCOL")  # noqa: F821

    mapper = ISA95Mapper(levels=isa95_levels)
    connector = ZerobusConnector(
        endpoint=endpoint,
        protocol=protocol,
        mapper=mapper,
        output_table=f"{catalog}.bronze._zerobus_staging",
        spark=spark,
        quality_codes=True,
    )
    connector.run()

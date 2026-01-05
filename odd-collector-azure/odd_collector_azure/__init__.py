def get_version() -> str:
    try:
        from odd_collector_azure.__version__ import VERSION

        return VERSION
    except Exception:
        return "-"
import polars as pl


def sample_dataframe() -> pl.DataFrame:
    """Return a simple DataFrame for demonstration."""
    return pl.DataFrame({"numbers": [1, 2, 3]})

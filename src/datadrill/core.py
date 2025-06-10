import polars as pl


def sample_dataframe() -> pl.DataFrame:
    """Return a simple DataFrame for demonstration."""
    return pl.DataFrame({"numbers": [1, 2, 3]})


def sample_dataframe_with_modified() -> pl.DataFrame:
    """Return a DataFrame with both unmodified and modified columns."""
    return pl.DataFrame(
        {
            "numbers": [1, 2, 3],
            "modified_numbers": [10, 20, 30],
        }
    )

import polars as pl
import pytest

try:
    from datadrill_rs import sample_dataframe_with_modified_py
except ImportError:
    sample_dataframe_with_modified_py = None


@pytest.mark.skipif(
    sample_dataframe_with_modified_py is None, reason="bindings not compiled"
)
def test_sample_dataframe_from_rust():
    df = sample_dataframe_with_modified_py()
    expected = pl.DataFrame({"numbers": [1, 2, 3], "modified_numbers": [10, 20, 30]})
    assert df.equals(expected)

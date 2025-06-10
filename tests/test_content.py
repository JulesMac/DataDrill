from datadrill.core import sample_dataframe


def test_sample_dataframe_content():
    df = sample_dataframe()
    assert df["numbers"].to_list() == [1, 2, 3]

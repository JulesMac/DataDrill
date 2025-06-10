from datadrill.core import sample_dataframe


def test_sample_dataframe():
    df = sample_dataframe()
    assert df.shape == (3, 1)

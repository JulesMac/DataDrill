from datadrill.core import sample_dataframe


def test_sample_dataframe_dump():
    df = sample_dataframe()
    assert df.write_json() == '[{"numbers":1},{"numbers":2},{"numbers":3}]'

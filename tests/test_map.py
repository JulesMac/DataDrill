from datadrill import (
    Environment,
    Field,
    FieldResolver,
    map,
    map2,
    sample_dataframe_with_modified,
)


def test_map_single_reader():
    df = sample_dataframe_with_modified()
    env = Environment(FieldResolver(df.columns))
    numbers = Field("numbers")

    expr = map(lambda a: a + 1, numbers())
    result = df.select(expr(env))
    assert result.to_series().to_list() == [2, 3, 4]


def test_map2_basic():
    df = sample_dataframe_with_modified()
    env = Environment(FieldResolver(df.columns))
    numbers = Field("numbers")
    modified = Field("modified_numbers")

    expr = map2(lambda a, b: a + b, numbers(), modified())
    result = df.select(expr(env))
    assert result.to_series().to_list() == [11, 22, 33]

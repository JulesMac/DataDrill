from datadrill import (
    Environment,
    Field,
    FieldResolver,
    ask,
    asks,
    pure,
    sample_dataframe_with_modified,
)


def test_ask_returns_environment():
    df = sample_dataframe_with_modified()
    env = Environment(FieldResolver(df.columns))
    assert ask()(env) is env


def test_asks_prefix():
    df = sample_dataframe_with_modified()
    env = Environment(FieldResolver(df.columns, prefix="modified_"))
    expr = asks(lambda e: e.resolver.prefix)
    result = df.select(expr(env))
    assert result.to_series().to_list() == ["modified_"]


def test_pure_constant_addition():
    df = sample_dataframe_with_modified()
    env = Environment(FieldResolver(df.columns))
    numbers = Field("numbers")
    expr = numbers() + pure(1)
    result = df.select(expr(env))
    assert result.to_series().to_list() == [2, 3, 4]

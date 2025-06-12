from datadrill import (
    Environment,
    Field,
    FieldResolver,
    Reader,
    ask,
    asks,
    pure,
    map2,
    field_function,
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


@field_function
def add_offset(a: Reader, offset: Reader) -> Reader:
    return a + offset


def test_field_function_with_asks():
    df = sample_dataframe_with_modified()
    env = Environment(FieldResolver(df.columns, prefix="modified_"))
    numbers = Field("numbers")
    offset = asks(lambda e: len(e.resolver.prefix))
    expr = add_offset(numbers(), offset)
    result = df.select(expr(env))
    assert result.to_series().to_list() == [19, 29, 39]


def test_map2_with_asks():
    df = sample_dataframe_with_modified()
    env = Environment(FieldResolver(df.columns, prefix="modified_"))
    numbers = Field("numbers")
    prefix_len = asks(lambda e: len(e.resolver.prefix))
    expr = map2(lambda a, b: a + b, numbers(), prefix_len)
    result = df.select(expr(env))
    assert result.to_series().to_list() == [19, 29, 39]

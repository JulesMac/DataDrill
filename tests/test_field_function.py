import polars as pl
from datadrill import (
    Environment,
    Field,
    FieldResolver,
    use_prefix,
    sample_dataframe_with_modified,
    field_function,
)


@field_function
def add_and_scale(a: pl.Expr, b: pl.Expr, factor: int) -> pl.Expr:
    return (a + b) * factor


def test_field_function_basic():
    df = sample_dataframe_with_modified()
    env = Environment(FieldResolver(df.columns))
    numbers = Field("numbers")
    modified = Field("modified_numbers")

    result = df.select(add_and_scale(numbers, modified, factor=2)(env))
    assert result.to_series().to_list() == [22, 44, 66]


@field_function
def add_two(a: pl.Expr, b: pl.Expr) -> pl.Expr:
    return a + b


def test_field_function_with_reader_arg():
    df = sample_dataframe_with_modified()
    env = Environment(FieldResolver(df.columns))
    numbers = Field("numbers")

    result = df.select(add_two(numbers, use_prefix("modified_")(numbers()))(env))
    assert result.to_series().to_list() == [11, 22, 33]

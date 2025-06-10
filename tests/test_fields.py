from datadrill import (
    Environment,
    Field,
    FieldResolver,
    use_prefix,
    sample_dataframe_with_modified,
)


def test_field_unmodified():
    df = sample_dataframe_with_modified()
    env = Environment(FieldResolver(df.columns))
    numbers = Field("numbers")

    result = df.select(numbers()(env))
    assert result["numbers"].to_list() == [1, 2, 3]


def test_field_modified():
    df = sample_dataframe_with_modified()
    base_env = Environment(FieldResolver(df.columns))
    env = base_env.with_prefix("modified_")
    numbers = Field("numbers")

    result = df.select(numbers()(env))
    assert result["modified_numbers"].to_list() == [10, 20, 30]


def test_use_prefix_overrides_env():
    df = sample_dataframe_with_modified()
    env = Environment(FieldResolver(df.columns))
    numbers = Field("numbers")

    result = df.select(use_prefix("modified_")(numbers())(env))
    assert result["modified_numbers"].to_list() == [10, 20, 30]

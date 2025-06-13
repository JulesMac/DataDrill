from datadrill import (
    Environment,
    Field,
    FieldResolver,
    get_data,
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


def test_add_two_fields():
    df = sample_dataframe_with_modified()
    env = Environment(FieldResolver(df.columns))
    numbers = Field("numbers")
    modified = Field("modified_numbers")

    result = df.select((numbers() + modified())(env))
    assert result["numbers"].to_list() == [11, 22, 33]


def test_add_field_with_prefix():
    df = sample_dataframe_with_modified()
    env = Environment(FieldResolver(df.columns))
    numbers = Field("numbers")

    result = df.select((numbers() + use_prefix("modified_")(numbers()))(env))
    assert result["numbers"].to_list() == [11, 22, 33]


def test_add_scalar():
    df = sample_dataframe_with_modified()
    env = Environment(FieldResolver(df.columns))
    numbers = Field("numbers")

    result = df.select((numbers() + 1)(env))
    assert result["numbers"].to_list() == [2, 3, 4]


def test_get_data_unmodified():
    df = sample_dataframe_with_modified()
    env = Environment(FieldResolver(df.columns))

    result = df.select(get_data("numbers")(env))
    assert result["numbers"].to_list() == [1, 2, 3]


def test_get_data_modified_prefix():
    df = sample_dataframe_with_modified()
    env = Environment(FieldResolver(df.columns, prefix="modified_"))

    result = df.select(get_data("numbers")(env))
    assert result["modified_numbers"].to_list() == [10, 20, 30]

from datadrill import (
    Environment,
    Field,
    sample_dataframe_with_modified,
    use_modified,
)


def test_field_unmodified():
    df = sample_dataframe_with_modified()
    numbers = Field("numbers", "numbers_modified")
    env = Environment(modified=False)

    result = df.select(numbers()(env))
    assert result["numbers"].to_list() == [1, 2, 3]


def test_field_modified():
    df = sample_dataframe_with_modified()
    numbers = Field("numbers", "numbers_modified")
    env = Environment(modified=True)

    result = df.select(numbers()(env))
    assert result["numbers_modified"].to_list() == [10, 20, 30]


def test_use_modified_overrides_env():
    df = sample_dataframe_with_modified()
    numbers = Field("numbers", "numbers_modified")
    env = Environment(modified=False)

    result = df.select(use_modified(numbers())(env))
    assert result["numbers_modified"].to_list() == [10, 20, 30]

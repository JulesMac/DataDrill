from datadrill import (
    DataFrame,
    Field,
    Environment,
    FieldResolver,
    sample_dataframe_with_modified,
)


def test_filter_then_select():
    base = DataFrame(sample_dataframe_with_modified())
    numbers = Field("numbers")
    result = base.filter(numbers() > 1).select(numbers()).run()
    assert result["numbers"].to_list() == [2, 3]


def test_run_with_prefix_env():
    df = sample_dataframe_with_modified()
    base = DataFrame(df)
    numbers = Field("numbers")
    env = Environment(FieldResolver(df.columns, prefix="modified_"))
    result = base.select(numbers()).run(env)
    assert result["modified_numbers"].to_list() == [10, 20, 30]

# Getting Started

DataDrill provides composable helpers for building queries on top of
[Polars](https://pola.rs). The library supplies small building blocks that you
can combine to describe DataFrame transformations and reuse logic.

## Installation

```bash
poetry install
```

## Creating a DataFrame

```python
from datadrill import sample_dataframe_with_modified

df = sample_dataframe_with_modified()
```

## Working with Fields

Fields represent columns. They resolve to Polars expressions using an
`Environment` and can be combined with normal Python operators.

```python
from datadrill import Environment, Field, FieldResolver

env = Environment(FieldResolver(df.columns))
numbers = Field("numbers")
modified = Field("modified_numbers")

# Select a column
df.select(numbers()(env))

# Combine columns
df.select((numbers() + modified())(env))

# Force the "modified_" prefix
from datadrill import use_prefix
df.select(use_prefix("modified_")(numbers())(env))
```

## Composable DataFrame operations

`DataFrame` wraps a Polars DataFrame and records transformations. Calling
`run()` executes the stored operations.

```python
from datadrill import DataFrame

query = (
    DataFrame(df)
    .filter(numbers() > 1)
    .select(numbers() + modified())
    .sort(numbers(), descending=True)
)

result = query.run(env)
```

## Custom field functions

Turn a regular function into a reusable expression with `@field_function`.

```python
from datadrill import field_function

@field_function
def add_and_scale(a, b, factor):
    return (a + b) * factor

df.select(add_and_scale(numbers(), modified(), factor=2)(env))
# [22, 44, 66]
```

## Series functions

Use `@series_function` to work with `polars.Series` values.

```python
from datadrill import series_function
import polars as pl

@series_function
def add_and_scale_series(a: pl.Series, b: pl.Series, factor: int) -> pl.Series:
    return (a + b) * factor

df.select(add_and_scale_series(numbers(), modified(), factor=2)(env))
```

# DataDrill

DataDrill is a small demonstration project managed with [Poetry](https://python-poetry.org/).
The reference Python implementation now lives under the `python/` directory to
avoid mixing with future Rust code.

## Lines of Code

Statistics generated with [cloc](https://github.com/AlDanial/cloc):

| Language | Files | Blank | Comment | Code |
|----------|------:|------:|--------:|-----:|
| Python   | 7 | 164 | 27 | 412 |
| Markdown | 3 | 61 | 0 | 141 |
| YAML     | 3 | 4 | 0 | 67 |
| TOML     | 2 | 8 | 0 | 32 |
| Rust     | 1 | 3 | 0 | 30 |
| **Total** | 19 | 245 | 27 | 691 |

## Development

Install dependencies using Poetry:

```bash
poetry install
```

### Lint

Run the linter with pre-commit:

```bash
poetry run pre-commit run --all-files
```

### Tests

Run unit tests using pytest:

```bash
poetry run pytest
```

### Rust crate

Build and test the Rust implementation with Cargo:

```bash
cargo test --manifest-path rust/Cargo.toml
```

### Test and Lint Reports

Test logs and a JUnit XML report are stored in the `reports/` directory.
During continuous integration these files are uploaded as an artifact named
`test-report`.


## Field API Example

Fields are callables that resolve Polars expressions based on an environment.
An environment now uses a `FieldResolver` which can prefix column names.

```python
from datadrill import (
    Environment,
    Field,
    FieldResolver,
    get_data,
    use_prefix,
    sample_dataframe_with_modified,
)

df = sample_dataframe_with_modified()
env = Environment(FieldResolver(df.columns))
numbers = Field("numbers")

# Select the unmodified column
df.select(numbers()(env))

# Force the "modified_" prefix
df.select(use_prefix("modified_")(numbers())(env))

# Combine fields with arithmetic operators
modified = Field("modified_numbers")
df.select((numbers() + modified())(env))

# Access a column without creating a Field
df.select(get_data("numbers")(env))
```

## Custom Field Functions

Use ``@field_function`` to create reusable Polars logic that works with
``Field`` parameters and regular arguments.

```python
from datadrill import (
    Environment,
    Field,
    FieldResolver,
    Reader,
    field_function,
    sample_dataframe_with_modified,
)
import polars as pl

@field_function
def add_and_scale(a: Reader, b: Reader, factor: int) -> Reader:
    return (a + b) * factor

df = sample_dataframe_with_modified()
env = Environment(FieldResolver(df.columns))
numbers = Field("numbers")
modified = Field("modified_numbers")

df.select(add_and_scale(numbers(), modified(), factor=2)(env))
# [22, 44, 66]
```

## Series Functions

The ``@series_function`` decorator lets you define custom logic using
``polars.Series`` values. The wrapped function is executed via
``Expr.map_batches`` so it integrates with lazy expressions.

```python
from datadrill import series_function

@series_function
def add_and_scale_series(a: pl.Series, b: pl.Series, factor: int) -> pl.Series:
    return (a + b) * factor

df.select(add_and_scale_series(numbers(), modified(), factor=2)(env))
# [22, 44, 66]
```

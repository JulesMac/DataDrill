# DataDrill

DataDrill is a small demonstration project managed with [Poetry](https://python-poetry.org/).

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
```

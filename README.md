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

Fields are callables that resolve Polars expressions based on an environment. A simple environment contains a `modified` flag which chooses between column namespaces.

```python
from datadrill import Environment, Field, use_modified, sample_dataframe_with_modified

numbers = Field("numbers", "numbers_modified")
env = Environment(modified=False)

# Select the unmodified column
sample_dataframe_with_modified().select(numbers()(env))

# Force the modified namespace
sample_dataframe_with_modified().select(use_modified(numbers())(env))
```

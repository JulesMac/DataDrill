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

Sample output from running pre-commit and pytest is stored in the `reports/`
directory. These logs are generated in CI and committed for reference.


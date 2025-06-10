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


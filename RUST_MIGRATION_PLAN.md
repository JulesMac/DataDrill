# Rust Migration Plan

This document outlines the steps required to convert the current DataDrill Python project into a Rust-based implementation.

## 1. Prepare the Repository

1. Initialize a new Cargo crate in a `rust` directory at the project root.
2. Set up CI and tooling similar to the existing Python workflow (formatting, linting, testing).
3. Mirror the current license and project metadata in `Cargo.toml`.

## 2. Select Dependencies

- Use the [`polars`](https://crates.io/crates/polars) crate for DataFrame operations.
- Add [`pyo3`](https://crates.io/crates/pyo3) and [`maturin`](https://crates.io/crates/maturin) if Python bindings are required.

## 3. Port Core Types

1. Translate `FieldResolver` and `Environment` into Rust structs.
2. Create a `Reader` trait or struct that represents deferred evaluation of `polars::Expr`.
3. Implement `Field` as a thin wrapper around column names.
4. Re-create helper functions such as `use_prefix`, `field_function`, and `series_function` with idiomatic Rust equivalents.

## 4. Recreate Functionality

- Replicate arithmetic and logical operator support for `Reader` using Rust traits (`Add`, `Sub`, etc.).
- Implement mapping helpers (`map`, `map2`) as generic functions returning `Reader` instances.
- Provide an `ask`, `asks`, and `pure` API similar to the Python version.

## 5. Testing Strategy

1. Translate the existing pytest suite into Rust unit tests under `rust/tests`.
2. Ensure parity by comparing DataFrame outputs between the Python and Rust versions.
3. Use `cargo test` to run the suite during development and CI.

## 6. Optional Python Bindings

- If a Python interface is still desired, expose the Rust API using PyO3 and build wheels with Maturin.
- Update `pyproject.toml` to depend on the generated Python package.

## 7. Gradual Migration

1. Begin by reimplementing the simple helper `sample_dataframe_with_modified` in Rust.
2. Incrementally port modules (`core`, `field`) while keeping Python code operational until each section is complete.
3. Maintain compatibility by writing integration tests that compare outputs from both implementations.

## 8. Documentation and Examples

- Update the README with instructions for building and testing the Rust crate.
- Provide usage examples in Rust and, if bindings are built, in Python as well.

## 9. Cleanup

- Remove the old Python implementation once feature parity is confirmed.
- Update CI to only build and test the Rust crate.


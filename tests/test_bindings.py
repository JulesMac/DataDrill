"""Tests for the optional Rust bindings."""

from __future__ import annotations

import importlib
import subprocess
from pathlib import Path

import polars as pl
import pytest


@pytest.fixture(scope="session", autouse=True)
def build_bindings() -> None:
    """Build the Rust extension with maturin if it isn't already available."""
    try:  # pragma: no cover - import check
        import datadrill_rs.datadrill_rs  # noqa: F401
    except ImportError:  # pragma: no cover - build occurs outside coverage
        repo_root = Path(__file__).resolve().parents[1]
        # Build in debug mode to keep compilation fast during testing
        subprocess.run(
            [
                "maturin",
                "develop",
                "--manifest-path",
                str(repo_root / "rust" / "Cargo.toml"),
                "-F",
                "pybindings",
            ],
            check=True,
        )
        importlib.invalidate_caches()


def test_sample_dataframe_from_rust() -> None:
    from datadrill_rs import sample_dataframe_with_modified_py

    df = sample_dataframe_with_modified_py()
    expected = pl.DataFrame({"numbers": [1, 2, 3], "modified_numbers": [10, 20, 30]})
    assert df.equals(expected)

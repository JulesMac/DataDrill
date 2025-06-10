from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import polars as pl


@dataclass(frozen=True)
class Environment:
    """Context used when resolving fields."""

    modified: bool = False

    def with_modified(self, value: bool = True) -> "Environment":
        """Return a copy of the environment with ``modified`` set to ``value``."""
        return Environment(modified=value)


Reader = Callable[[Environment], pl.Expr]


@dataclass(frozen=True)
class Field:
    """Representation of a DataFrame column in two namespaces."""

    name: str
    modified_name: str | None = None

    def __call__(self) -> Reader:
        """Return a reader that resolves the correct column based on the environment."""

        def reader(env: Environment) -> pl.Expr:
            column = self.modified_name if env.modified else self.name
            return pl.col(column)

        return reader


def use_modified(reader: Reader) -> Reader:
    """Force a reader to resolve using the ``modified`` namespace."""

    def wrapper(env: Environment) -> pl.Expr:
        return reader(env.with_modified())

    return wrapper

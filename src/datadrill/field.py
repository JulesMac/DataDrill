from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Sequence

import polars as pl


@dataclass(frozen=True)
class FieldResolver:
    """Resolve column names based on an optional prefix."""

    schema: Sequence[str]
    prefix: str = ""

    def with_prefix(self, value: str = "") -> "FieldResolver":
        """Return a copy of the resolver with ``prefix`` set to ``value``."""
        return FieldResolver(self.schema, value)

    def clear_prefix(self) -> "FieldResolver":
        """Return a copy of the resolver without a prefix."""
        return FieldResolver(self.schema)

    def resolve(self, name: str) -> str:
        """Return the column name taking the prefix into account."""
        column = f"{self.prefix}{name}"
        if column not in self.schema:
            raise KeyError(f"{column} not in schema")
        return column


@dataclass(frozen=True)
class Environment:
    """Context used when resolving fields."""

    resolver: FieldResolver

    def with_prefix(self, value: str = "") -> "Environment":
        """Return a copy of the environment with ``prefix`` set to ``value``."""
        return Environment(self.resolver.with_prefix(value))

    def clear_prefix(self) -> "Environment":
        """Return a copy of the environment without a prefix."""
        return Environment(self.resolver.clear_prefix())


Reader = Callable[[Environment], pl.Expr]


@dataclass(frozen=True)
class Field:
    """Representation of a DataFrame column."""

    name: str

    def __call__(self) -> Reader:
        """Return a reader that resolves the correct column based on the environment."""

        def reader(env: Environment) -> pl.Expr:
            column = env.resolver.resolve(self.name)
            return pl.col(column)

        return reader


def use_prefix(prefix: str) -> Callable[[Reader], Reader]:
    """Force a reader to resolve using ``prefix``."""

    def decorator(reader: Reader) -> Reader:
        def wrapper(env: Environment) -> pl.Expr:
            return reader(env.with_prefix(prefix))

        return wrapper

    return decorator


def get_data(name: str) -> Reader:
    """Return a reader resolving ``name`` using the environment's resolver."""

    def reader(env: Environment) -> pl.Expr:
        column = env.resolver.resolve(name)
        return pl.col(column)

    return reader

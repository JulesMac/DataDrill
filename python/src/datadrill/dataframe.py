from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List

import polars as pl

from .field import Environment, FieldResolver, Reader, Field

ExprSource = Reader | Field | pl.Expr | int | float


@dataclass(frozen=True)
class DataFrame:
    """Composable DataFrame operations."""

    df: pl.DataFrame
    _ops: List[Callable[[pl.DataFrame, Environment], pl.DataFrame]] = field(
        default_factory=list
    )

    def filter(self, predicate: ExprSource) -> DataFrame:
        """Return a new DataFrame with ``predicate`` applied."""

        def op(df: pl.DataFrame, env: Environment) -> pl.DataFrame:
            expr = Reader._expr_from(predicate, env)
            return df.filter(expr)

        return DataFrame(self.df, [*self._ops, op])

    def select(self, *exprs: ExprSource) -> DataFrame:
        """Return a new DataFrame selecting ``exprs``."""

        def op(df: pl.DataFrame, env: Environment) -> pl.DataFrame:
            columns = [Reader._expr_from(e, env) for e in exprs]
            return df.select(columns)

        return DataFrame(self.df, [*self._ops, op])

    def run(self, env: Environment | None = None) -> pl.DataFrame:
        """Execute stored operations using ``env`` if provided."""
        if env is None:
            env = Environment(FieldResolver(self.df.columns))

        df = self.df
        for op in self._ops:
            df = op(df, env)
        return df

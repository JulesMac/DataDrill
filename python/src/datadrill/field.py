from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Sequence, Any, TypeAlias

import polars as pl


@dataclass(frozen=True)
class FieldResolver:
    """Resolve column names based on an optional prefix."""

    schema: Sequence[str]
    prefix: str = ""

    def with_prefix(self, value: str = "") -> FieldResolver:
        """Return a copy of the resolver with ``prefix`` set to ``value``."""
        return FieldResolver(self.schema, value)

    def clear_prefix(self) -> FieldResolver:
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

    def with_prefix(self, value: str = "") -> Environment:
        """Return a copy of the environment with ``prefix`` set to ``value``."""
        return Environment(self.resolver.with_prefix(value))

    def clear_prefix(self) -> Environment:
        """Return a copy of the environment without a prefix."""
        return Environment(self.resolver.clear_prefix())


ReaderFunc = Callable[[Environment], Any]


class Reader:
    """Callable wrapper supporting expression operators."""

    def __init__(self, func: ReaderFunc):
        self._func = func

    def __call__(self, env: Environment) -> Any:
        return self._func(env)

    @staticmethod
    def _expr_from(value: ExprLike, env: Environment) -> pl.Expr:
        if isinstance(value, Reader):
            return value(env)
        if isinstance(value, Field):
            return value()(env)
        if isinstance(value, pl.Expr):
            return value
        return pl.lit(value)

    def _binary_op(
        self,
        other: ExprLike,
        op: Callable[[pl.Expr, pl.Expr], pl.Expr],
        *,
        reverse: bool = False,
    ) -> Reader:
        def wrapper(env: Environment) -> pl.Expr:
            left = self._expr_from(self, env)
            right = self._expr_from(other, env)
            if reverse:
                left, right = right, left
            return op(left, right)

        return Reader(wrapper)

    def __add__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a + b)

    def __radd__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a + b, reverse=True)

    def __sub__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a - b)

    def __rsub__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a - b, reverse=True)

    def __mul__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a * b)

    def __rmul__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a * b, reverse=True)

    def __truediv__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a / b)

    def __rtruediv__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a / b, reverse=True)

    def __floordiv__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a // b)

    def __rfloordiv__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a // b, reverse=True)

    def __mod__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a % b)

    def __rmod__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a % b, reverse=True)

    def __pow__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a**b)

    def __rpow__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a**b, reverse=True)

    def __and__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a & b)

    def __rand__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a & b, reverse=True)

    def __or__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a | b)

    def __ror__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a | b, reverse=True)

    def __xor__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a ^ b)

    def __rxor__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a ^ b, reverse=True)

    def __lt__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a < b)

    def __le__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a <= b)

    def __gt__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a > b)

    def __ge__(self, other: ExprLike) -> Reader:
        return self._binary_op(other, lambda a, b: a >= b)

    def __eq__(self, other: object) -> Reader:  # type: ignore[override]
        return self._binary_op(other, lambda a, b: a == b)

    def __ne__(self, other: object) -> Reader:  # type: ignore[override]
        return self._binary_op(other, lambda a, b: a != b)

    def __neg__(self) -> Reader:
        def wrapper(env: Environment) -> pl.Expr:
            return -self(env)

        return Reader(wrapper)

    def __pos__(self) -> Reader:
        def wrapper(env: Environment) -> pl.Expr:
            return +self(env)

        return Reader(wrapper)

    def __invert__(self) -> Reader:
        def wrapper(env: Environment) -> pl.Expr:
            return ~self(env)

        return Reader(wrapper)


@dataclass(frozen=True)
class Field:
    """Representation of a DataFrame column."""

    name: str

    def __call__(self) -> Reader:
        """Return a reader that resolves the correct column based on the environment."""

        def reader(env: Environment) -> pl.Expr:
            column = env.resolver.resolve(self.name)
            return pl.col(column)

        return Reader(reader)


# Inputs accepted by Reader._expr_from and map helpers
ExprLike: TypeAlias = Reader | Field | pl.Expr | int | float


def use_prefix(prefix: str) -> Callable[[Reader], Reader]:
    """Force a reader to resolve using ``prefix``."""

    def decorator(reader: Reader) -> Reader:
        def wrapper(env: Environment) -> pl.Expr:
            return reader(env.with_prefix(prefix))

        return Reader(wrapper)

    return decorator


def get_data(name: str) -> Reader:
    """Return a reader resolving ``name`` using the environment's resolver."""

    def reader(env: Environment) -> pl.Expr:
        column = env.resolver.resolve(name)
        return pl.col(column)

    return Reader(reader)


def field_function(func: Callable[..., Any]) -> Callable[..., Reader]:
    """Wrap ``func`` so the return value becomes a :class:`Reader`.

    The wrapped ``func`` is executed when the resulting reader runs. All
    arguments are converted to :class:`polars.Expr` using ``Reader._expr_from``
    first. This ensures user code always operates on resolved expressions
    rather than ``Reader`` instances.
    """

    def factory(*args: Any, **kwargs: Any) -> Reader:
        def reader(env: Environment) -> pl.Expr:
            call_args = [Reader._expr_from(arg, env) for arg in args]
            call_kwargs = {
                key: Reader._expr_from(value, env) for key, value in kwargs.items()
            }
            result = func(*call_args, **call_kwargs)
            return Reader._expr_from(result, env)

        return Reader(reader)

    return factory


def series_function(func: Callable[..., pl.Series]) -> Callable[..., Reader]:
    """Wrap ``func`` so it operates on :class:`polars.Series` values."""

    def factory(*args: Any, **kwargs: Any) -> Reader:
        def reader(env: Environment) -> pl.Expr:
            exprs = []
            constants = {}

            for i, value in enumerate(args):
                name = f"_{i}"
                if isinstance(value, (Reader, Field)) or isinstance(value, pl.Expr):
                    exprs.append(Reader._expr_from(value, env).alias(name))
                else:
                    constants[name] = value

            for name, value in kwargs.items():
                if isinstance(value, (Reader, Field)) or isinstance(value, pl.Expr):
                    exprs.append(Reader._expr_from(value, env).alias(name))
                else:
                    constants[name] = value

            struct_expr = pl.struct(exprs)

            def map_fn(struct: pl.Series) -> pl.Series:
                call_args = []
                for i in range(len(args)):
                    name = f"_{i}"
                    if name in constants:
                        call_args.append(constants[name])
                    else:
                        call_args.append(struct.struct.field(name))

                call_kwargs = {}
                for key in kwargs.keys():
                    if key in constants:
                        call_kwargs[key] = constants[key]
                    else:
                        call_kwargs[key] = struct.struct.field(key)

                return func(*call_args, **call_kwargs)

            return struct_expr.map_batches(map_fn)

        return Reader(reader)

    return factory


def map(
    func: Callable[[pl.Expr], pl.Expr | int | float],
    reader: ExprLike,
) -> Reader:
    """Apply ``func`` to ``reader`` using the execution environment."""

    def wrapper(env: Environment) -> pl.Expr:
        value = Reader._expr_from(reader, env)
        result = func(value)
        return Reader._expr_from(result, env)

    return Reader(wrapper)


def map2(
    func: Callable[[pl.Expr, pl.Expr], pl.Expr | int | float],
    reader1: ExprLike,
    reader2: ExprLike,
) -> Reader:
    """Apply ``func`` to ``reader1`` and ``reader2`` using the environment."""

    def wrapper(env: Environment) -> pl.Expr:
        value1 = Reader._expr_from(reader1, env)
        value2 = Reader._expr_from(reader2, env)
        result = func(value1, value2)
        return Reader._expr_from(result, env)

    return Reader(wrapper)


def ask() -> Reader:
    """Return the current :class:`Environment`."""

    def reader(env: Environment) -> Environment:
        return env

    return Reader(reader)


def asks(func: Callable[[Environment], ExprLike]) -> Reader:
    """Transform the environment into an expression using ``func``."""

    def reader(env: Environment) -> pl.Expr:
        value = func(env)
        return Reader._expr_from(value, env)

    return Reader(reader)


def pure(value: ExprLike) -> Reader:
    """Return a reader that always yields ``value``."""

    def reader(env: Environment) -> pl.Expr:
        return Reader._expr_from(value, env)

    return Reader(reader)

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Sequence, Any, get_type_hints
import inspect

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


ReaderFunc = Callable[[Environment], pl.Expr]


class Reader:
    """Callable wrapper supporting expression operators."""

    def __init__(self, func: ReaderFunc):
        self._func = func

    def __call__(self, env: Environment) -> pl.Expr:
        return self._func(env)

    @staticmethod
    def _expr_from(
        value: Reader | Field | ReaderFunc | pl.Expr | int | float,
        env: Environment,
    ) -> pl.Expr:
        if isinstance(value, Reader):
            return value(env)
        if isinstance(value, Field):
            return value()(env)
        if callable(value):  # treat as ReaderFunc
            return value(env)
        if isinstance(value, pl.Expr):
            return value
        return pl.lit(value)

    def _binary_op(
        self,
        other: Reader | Field | ReaderFunc | pl.Expr | int | float,
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

    def __add__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a + b)

    def __radd__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a + b, reverse=True)

    def __sub__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a - b)

    def __rsub__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a - b, reverse=True)

    def __mul__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a * b)

    def __rmul__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a * b, reverse=True)

    def __truediv__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a / b)

    def __rtruediv__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a / b, reverse=True)

    def __floordiv__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a // b)

    def __rfloordiv__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a // b, reverse=True)

    def __mod__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a % b)

    def __rmod__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a % b, reverse=True)

    def __pow__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a**b)

    def __rpow__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a**b, reverse=True)

    def __and__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a & b)

    def __rand__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a & b, reverse=True)

    def __or__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a | b)

    def __ror__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a | b, reverse=True)

    def __xor__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a ^ b)

    def __rxor__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a ^ b, reverse=True)

    def __lt__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a < b)

    def __le__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a <= b)

    def __gt__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
        return self._binary_op(other, lambda a, b: a > b)

    def __ge__(
        self, other: Reader | Field | ReaderFunc | pl.Expr | int | float
    ) -> Reader:
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


def field_function(func: Callable[..., pl.Expr]) -> Callable[..., Reader]:
    """Wrap ``func`` so it produces a :class:`Reader` when called."""

    # Inspect the target function so we can bind its parameters dynamically
    sig = inspect.signature(func)
    hints = get_type_hints(func)

    # Parameters annotated as ``Field``/``Reader``/``Expr`` should be resolved
    # when the returned ``Reader`` executes.
    dynamic: set[str] = set()
    for name, param in sig.parameters.items():
        ann = hints.get(name, param.annotation)
        if ann in {Field, Reader, pl.Expr} or ann is inspect._empty:
            dynamic.add(name)

    def factory(*args: Any, **kwargs: Any) -> Reader:
        # Partially bind provided arguments; remaining values will come from
        # the execution environment when the ``Reader`` is evaluated.
        bound = sig.bind_partial(*args, **kwargs)
        bound.apply_defaults()

        def reader(env: Environment) -> pl.Expr:
            # Resolve arguments to a mix of Polars expressions and constants
            call_args = []
            call_kwargs = {}
            for name, param in sig.parameters.items():
                value = bound.arguments.get(name, param.default)
                if name in dynamic:
                    value = Reader._expr_from(value, env)
                if param.kind in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                ):
                    call_args.append(value)
                elif param.kind == inspect.Parameter.KEYWORD_ONLY:
                    call_kwargs[name] = value
                else:
                    raise TypeError("varargs are not supported")

            result = func(*call_args, **call_kwargs)
            # The result might be a scalar or an expression; normalize it
            return Reader._expr_from(result, env)

        return Reader(reader)

    return factory


def series_function(func: Callable[..., pl.Series]) -> Callable[..., Reader]:
    """Wrap ``func`` so it operates on :class:`polars.Series` values."""

    # Discover how to call ``func`` so we can construct a Polars expression
    sig = inspect.signature(func)
    hints = get_type_hints(func)

    # Determine which parameters should be resolved from the environment when
    # the resulting ``Reader`` is executed.
    dynamic: set[str] = set()
    for name, param in sig.parameters.items():
        ann = hints.get(name, param.annotation)
        if ann in {Field, Reader, pl.Expr, pl.Series} or ann is inspect._empty:
            dynamic.add(name)

    def factory(*args: Any, **kwargs: Any) -> Reader:
        # Bind any provided arguments now and defer the rest until execution
        bound = sig.bind_partial(*args, **kwargs)
        bound.apply_defaults()

        def reader(env: Environment) -> pl.Expr:
            # Build a struct expression containing all dynamic arguments
            exprs = []
            constants = {}
            for name, param in sig.parameters.items():
                value = bound.arguments.get(name, param.default)
                if name in dynamic:
                    expr = Reader._expr_from(value, env).alias(name)
                    exprs.append(expr)
                else:
                    constants[name] = value

            struct_expr = pl.struct(exprs)

            def map_fn(struct: pl.Series) -> pl.Series:
                # Unpack the struct back into arguments for ``func``
                call_args = []
                call_kwargs = {}
                for name, param in sig.parameters.items():
                    if name in dynamic:
                        value = struct.struct.field(name)
                    else:
                        value = constants[name]
                    if param.kind in (
                        inspect.Parameter.POSITIONAL_ONLY,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    ):
                        call_args.append(value)
                    elif param.kind == inspect.Parameter.KEYWORD_ONLY:
                        call_kwargs[name] = value
                    else:
                        raise TypeError("varargs are not supported")
                return func(*call_args, **call_kwargs)

            return struct_expr.map_batches(map_fn)

        return Reader(reader)

    return factory


def map(
    func: Callable[[pl.Expr], pl.Expr | int | float],
    reader: Reader | Field | ReaderFunc | pl.Expr | int | float,
) -> Reader:
    """Apply ``func`` to ``reader`` using the execution environment."""

    def wrapper(env: Environment) -> pl.Expr:
        value = Reader._expr_from(reader, env)
        result = func(value)
        return Reader._expr_from(result, env)

    return Reader(wrapper)


def map2(
    func: Callable[[pl.Expr, pl.Expr], pl.Expr | int | float],
    reader1: Reader | Field | ReaderFunc | pl.Expr | int | float,
    reader2: Reader | Field | ReaderFunc | pl.Expr | int | float,
) -> Reader:
    """Apply ``func`` to ``reader1`` and ``reader2`` using the environment."""

    def wrapper(env: Environment) -> pl.Expr:
        value1 = Reader._expr_from(reader1, env)
        value2 = Reader._expr_from(reader2, env)
        result = func(value1, value2)
        return Reader._expr_from(result, env)

    return Reader(wrapper)

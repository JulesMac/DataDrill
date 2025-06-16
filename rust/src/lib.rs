use polars::prelude::*;
use std::ops::{Add, BitAnd, BitOr, BitXor, Div, Mul, Neg, Not, Rem, Sub};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub struct FieldResolver {
    schema: Vec<String>,
    prefix: String,
}

impl FieldResolver {
    pub fn new<S: Into<String>>(schema: Vec<S>) -> Self {
        Self {
            schema: schema.into_iter().map(Into::into).collect(),
            prefix: String::new(),
        }
    }

    pub fn with_prefix(&self, value: &str) -> Self {
        Self {
            schema: self.schema.clone(),
            prefix: value.to_string(),
        }
    }

    pub fn clear_prefix(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            prefix: String::new(),
        }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn resolve(&self, name: &str) -> Result<String, String> {
        let column = format!("{}{}", self.prefix, name);
        if self.schema.iter().any(|c| c == &column) {
            Ok(column)
        } else {
            Err(format!("{column} not in schema"))
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Environment {
    resolver: FieldResolver,
}

impl Environment {
    pub fn new(resolver: FieldResolver) -> Self {
        Self { resolver }
    }

    pub fn with_prefix(&self, value: &str) -> Self {
        Self {
            resolver: self.resolver.with_prefix(value),
        }
    }

    pub fn clear_prefix(&self) -> Self {
        Self {
            resolver: self.resolver.clear_prefix(),
        }
    }

    pub fn resolver(&self) -> &FieldResolver {
        &self.resolver
    }
}

#[derive(Clone)]
pub struct Reader<T>(Arc<dyn Fn(&Environment) -> T + Send + Sync>);

impl<T> Reader<T> {
    pub fn new<F>(func: F) -> Self
    where
        F: Fn(&Environment) -> T + Send + Sync + 'static,
    {
        Self(Arc::new(func))
    }

    pub fn run(&self, env: &Environment) -> T {
        (self.0)(env)
    }
}

impl Reader<Expr> {
    pub fn alias(self, name: &str) -> Self {
        let name = name.to_string();
        Reader::new(move |env| self.run(env).alias(&name))
    }

    pub fn floor_div<R>(self, rhs: R) -> Self
    where
        R: IntoReader + Clone + Send + Sync + 'static,
    {
        Reader::new(move |env| {
            let rhs_expr = rhs.clone().into_reader().run(env);
            self.run(env).floor_div(rhs_expr)
        })
    }

    pub fn pow<R>(self, rhs: R) -> Self
    where
        R: IntoReader + Clone + Send + Sync + 'static,
    {
        Reader::new(move |env| {
            let rhs_expr = rhs.clone().into_reader().run(env);
            self.run(env).pow(rhs_expr)
        })
    }

    pub fn gt<R>(self, rhs: R) -> Self
    where
        R: IntoReader + Clone + Send + Sync + 'static,
    {
        Reader::new(move |env| {
            let rhs_expr = rhs.clone().into_reader().run(env);
            self.run(env).gt(rhs_expr)
        })
    }

    pub fn gt_eq<R>(self, rhs: R) -> Self
    where
        R: IntoReader + Clone + Send + Sync + 'static,
    {
        Reader::new(move |env| {
            let rhs_expr = rhs.clone().into_reader().run(env);
            self.run(env).gt_eq(rhs_expr)
        })
    }

    pub fn lt<R>(self, rhs: R) -> Self
    where
        R: IntoReader + Clone + Send + Sync + 'static,
    {
        Reader::new(move |env| {
            let rhs_expr = rhs.clone().into_reader().run(env);
            self.run(env).lt(rhs_expr)
        })
    }

    pub fn lt_eq<R>(self, rhs: R) -> Self
    where
        R: IntoReader + Clone + Send + Sync + 'static,
    {
        Reader::new(move |env| {
            let rhs_expr = rhs.clone().into_reader().run(env);
            self.run(env).lt_eq(rhs_expr)
        })
    }

    pub fn eq_to<R>(self, rhs: R) -> Self
    where
        R: IntoReader + Clone + Send + Sync + 'static,
    {
        Reader::new(move |env| {
            let rhs_expr = rhs.clone().into_reader().run(env);
            self.run(env).eq(rhs_expr)
        })
    }

    pub fn ne_to<R>(self, rhs: R) -> Self
    where
        R: IntoReader + Clone + Send + Sync + 'static,
    {
        Reader::new(move |env| {
            let rhs_expr = rhs.clone().into_reader().run(env);
            self.run(env).neq(rhs_expr)
        })
    }
}

macro_rules! impl_expr_op {
    ($trait:ident, $method:ident, $op:tt) => {
        impl $trait for Reader<Expr> {
            type Output = Reader<Expr>;

            fn $method(self, rhs: Reader<Expr>) -> Self::Output {
                Reader::new(move |env| self.run(env) $op rhs.run(env))
            }
        }

        impl $trait<i32> for Reader<Expr> {
            type Output = Reader<Expr>;

            fn $method(self, rhs: i32) -> Self::Output {
                Reader::new(move |env| self.run(env) $op lit(rhs).cast(DataType::Int32))
            }
        }

        impl $trait<Reader<Expr>> for i32 {
            type Output = Reader<Expr>;

            fn $method(self, rhs: Reader<Expr>) -> Self::Output {
                Reader::new(move |env| lit(self).cast(DataType::Int32) $op rhs.run(env))
            }
        }
    };
}

macro_rules! impl_expr_op_method {
    ($trait:ident, $method:ident, $func:ident) => {
        impl $trait for Reader<Expr> {
            type Output = Reader<Expr>;

            fn $method(self, rhs: Reader<Expr>) -> Self::Output {
                Reader::new(move |env| self.run(env).$func(rhs.run(env)))
            }
        }

        impl $trait<i32> for Reader<Expr> {
            type Output = Reader<Expr>;

            fn $method(self, rhs: i32) -> Self::Output {
                Reader::new(move |env| self.run(env).$func(lit(rhs).cast(DataType::Int32)))
            }
        }

        impl $trait<Reader<Expr>> for i32 {
            type Output = Reader<Expr>;

            fn $method(self, rhs: Reader<Expr>) -> Self::Output {
                Reader::new(move |env| lit(self).cast(DataType::Int32).$func(rhs.run(env)))
            }
        }
    };
}

impl_expr_op!(Add, add, +);
impl_expr_op!(Sub, sub, -);
impl_expr_op!(Mul, mul, *);
impl_expr_op!(Div, div, /);
impl_expr_op!(Rem, rem, %);
impl_expr_op_method!(BitAnd, bitand, and);
impl_expr_op_method!(BitOr, bitor, or);
impl_expr_op_method!(BitXor, bitxor, xor);

impl Neg for Reader<Expr> {
    type Output = Reader<Expr>;

    fn neg(self) -> Self::Output {
        Reader::new(move |env| -self.run(env))
    }
}

impl Not for Reader<Expr> {
    type Output = Reader<Expr>;

    fn not(self) -> Self::Output {
        Reader::new(move |env| self.run(env).not())
    }
}

#[derive(Clone, Debug)]
pub struct Field {
    name: String,
}

impl Field {
    pub fn new<S: Into<String>>(name: S) -> Self {
        Self { name: name.into() }
    }

    pub fn reader(&self) -> Reader<Expr> {
        let name = self.name.clone();
        Reader::new(move |env| {
            let column = env.resolver().resolve(&name).expect("column not in schema");
            col(&column)
        })
    }
}

pub fn use_prefix(prefix: &str, reader: Reader<Expr>) -> Reader<Expr> {
    let prefix = prefix.to_string();
    Reader::new(move |env| reader.run(&env.with_prefix(&prefix)))
}

pub fn get_data(name: &str) -> Reader<Expr> {
    let name = name.to_string();
    Reader::new(move |env| {
        let column = env.resolver().resolve(&name).expect("column not in schema");
        col(&column)
    })
}

pub fn map<F>(func: F, reader: Reader<Expr>) -> Reader<Expr>
where
    F: Fn(Expr) -> Expr + Send + Sync + 'static,
{
    Reader::new(move |env| {
        let expr = reader.run(env);
        func(expr)
    })
}

pub fn map2<F>(func: F, reader1: Reader<Expr>, reader2: Reader<Expr>) -> Reader<Expr>
where
    F: Fn(Expr, Expr) -> Expr + Send + Sync + 'static,
{
    Reader::new(move |env| {
        let expr1 = reader1.run(env);
        let expr2 = reader2.run(env);
        func(expr1, expr2)
    })
}

pub fn ask() -> Reader<Environment> {
    Reader::new(|env| env.clone())
}

pub fn asks<F, R>(func: F) -> Reader<Expr>
where
    F: Fn(&Environment) -> R + Send + Sync + 'static,
    R: Literal + Clone + 'static,
{
    Reader::new(move |env| func(env).clone().lit())
}

pub fn pure<T>(value: T) -> Reader<Expr>
where
    T: Literal + Clone + Send + Sync + 'static,
{
    Reader::new(move |_env| value.clone().lit())
}

pub trait IntoReader {
    fn into_reader(self) -> Reader<Expr>;
}

impl IntoReader for Reader<Expr> {
    fn into_reader(self) -> Reader<Expr> {
        self
    }
}

impl IntoReader for Field {
    fn into_reader(self) -> Reader<Expr> {
        self.reader()
    }
}

impl<'a> IntoReader for &'a Field {
    fn into_reader(self) -> Reader<Expr> {
        self.reader()
    }
}

impl<T> IntoReader for T
where
    T: Literal + Clone + Send + Sync + 'static,
{
    fn into_reader(self) -> Reader<Expr> {
        Reader::new(move |_env| self.clone().lit())
    }
}

pub fn field_function2<A, B, F>(func: F, a: A, b: B) -> Reader<Expr>
where
    A: IntoReader + Clone + Send + Sync + 'static,
    B: IntoReader + Clone + Send + Sync + 'static,
    F: Fn(Expr, Expr) -> Expr + Send + Sync + 'static,
{
    Reader::new(move |env| {
        let expr_a = a.clone().into_reader().run(env);
        let expr_b = b.clone().into_reader().run(env);
        func(expr_a, expr_b)
    })
}

pub fn field_function3<A, B, C, F>(func: F, a: A, b: B, c: C) -> Reader<Expr>
where
    A: IntoReader + Clone + Send + Sync + 'static,
    B: IntoReader + Clone + Send + Sync + 'static,
    C: IntoReader + Clone + Send + Sync + 'static,
    F: Fn(Expr, Expr, Expr) -> Expr + Send + Sync + 'static,
{
    Reader::new(move |env| {
        let expr_a = a.clone().into_reader().run(env);
        let expr_b = b.clone().into_reader().run(env);
        let expr_c = c.clone().into_reader().run(env);
        func(expr_a, expr_b, expr_c)
    })
}

pub fn series_function3<A, B, C, F>(func: F, a: A, b: B, c: C) -> Reader<Expr>
where
    A: IntoReader + Clone + Send + Sync + 'static,
    B: IntoReader + Clone + Send + Sync + 'static,
    C: IntoReader + Clone + Send + Sync + 'static,
    F: Fn(Series, Series, Series) -> Series + Send + Sync + 'static,
{
    let func = Arc::new(func);
    Reader::new(move |env| {
        let expr_a = a.clone().into_reader().run(env);
        let expr_b = b.clone().into_reader().run(env);
        let expr_c = c.clone().into_reader().run(env);
        let func = Arc::clone(&func);
        expr_a.map_many(
            move |cols: &mut [Column]| {
                let a = std::mem::take(&mut cols[0]).take_materialized_series();
                let b = std::mem::take(&mut cols[1]).take_materialized_series();
                let c = std::mem::take(&mut cols[2]).take_materialized_series();
                Ok(Some(func(a, b, c).into()))
            },
            &[expr_b, expr_c],
            GetOutput::first(),
        )
    })
}

pub fn sample_dataframe_with_modified() -> DataFrame {
    df! {
        "numbers" => &[1i32, 2, 3],
        "modified_numbers" => &[10i32, 20, 30]
    }
    .unwrap()
}

pub struct DataFrameOps {
    df: DataFrame,
    ops: Vec<Box<dyn Fn(DataFrame, &Environment) -> PolarsResult<DataFrame> + Send + Sync>>,
}

impl DataFrameOps {
    pub fn new(df: DataFrame) -> Self {
        Self {
            df,
            ops: Vec::new(),
        }
    }

    pub fn filter<R>(mut self, predicate: R) -> Self
    where
        R: IntoReader + Clone + Send + Sync + 'static,
    {
        self.ops.push(Box::new(move |df, env| {
            let expr = predicate.clone().into_reader().run(env);
            df.lazy().filter(expr).collect()
        }));
        self
    }

    pub fn select<I, R>(mut self, exprs: I) -> Self
    where
        I: IntoIterator<Item = R> + Clone + Send + Sync + 'static,
        R: IntoReader + Clone + Send + Sync + 'static,
    {
        self.ops.push(Box::new(move |df, env| {
            let columns: Vec<Expr> = exprs
                .clone()
                .into_iter()
                .map(|e| e.into_reader().run(env))
                .collect();
            df.lazy().select(columns).collect()
        }));
        self
    }

    pub fn run(self, env: Option<Environment>) -> PolarsResult<DataFrame> {
        let env = env.unwrap_or_else(|| {
            Environment::new(FieldResolver::new(self.df.get_column_names_str()))
        });
        let mut df = self.df;
        for op in self.ops {
            df = op(df, &env)?;
        }
        Ok(df)
    }
}

// ---- Python bindings ----
#[cfg(feature = "pybindings")]
mod py {
    use super::*;
    use pyo3::prelude::*;
    use pyo3_polars::PyDataFrame;

    #[pyfunction]
    fn sample_dataframe_with_modified_py() -> PyResult<PyDataFrame> {
        Ok(PyDataFrame(sample_dataframe_with_modified()))
    }

    #[pymodule]
    fn datadrill_rs(_py: Python<'_>, m: &Bound<PyModule>) -> PyResult<()> {
        m.add_function(wrap_pyfunction!(sample_dataframe_with_modified_py, m)?)?;
        Ok(())
    }
}

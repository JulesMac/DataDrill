use polars::prelude::*;
use std::ops::Add;
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
}

impl Add for Reader<Expr> {
    type Output = Reader<Expr>;

    fn add(self, rhs: Reader<Expr>) -> Self::Output {
        Reader::new(move |env| self.run(env) + rhs.run(env))
    }
}

impl Add<i32> for Reader<Expr> {
    type Output = Reader<Expr>;

    fn add(self, rhs: i32) -> Self::Output {
        Reader::new(move |env| self.run(env) + lit(rhs).cast(DataType::Int32))
    }
}

impl Add<Reader<Expr>> for i32 {
    type Output = Reader<Expr>;

    fn add(self, rhs: Reader<Expr>) -> Self::Output {
        Reader::new(move |env| lit(self).cast(DataType::Int32) + rhs.run(env))
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

pub fn sample_dataframe_with_modified() -> DataFrame {
    df! {
        "numbers" => &[1i32, 2, 3],
        "modified_numbers" => &[10i32, 20, 30]
    }
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_dataframe_contains_expected_columns() {
        let df = sample_dataframe_with_modified();
        assert_eq!(df.height(), 3);
        assert_eq!(df.get_column_names(), vec!["numbers", "modified_numbers"]);
        assert_eq!(
            df.column("numbers").unwrap().i32().unwrap().to_vec(),
            vec![Some(1), Some(2), Some(3)]
        );
        assert_eq!(
            df.column("modified_numbers")
                .unwrap()
                .i32()
                .unwrap()
                .to_vec(),
            vec![Some(10), Some(20), Some(30)]
        );
    }

    #[test]
    fn field_resolver_resolves_prefixed_columns() {
        let resolver = FieldResolver::new(vec!["numbers", "modified_numbers"]);
        let env = Environment::new(resolver.clone());
        assert_eq!(env.resolver().resolve("numbers").unwrap(), "numbers");

        let env2 = env.with_prefix("modified_");
        assert_eq!(
            env2.resolver().resolve("numbers").unwrap(),
            "modified_numbers"
        );
        assert!(env2.resolver().resolve("missing").is_err());
    }

    #[test]
    fn field_unmodified() {
        let df = sample_dataframe_with_modified();
        let env = Environment::new(FieldResolver::new(df.get_column_names_str()));
        let numbers = Field::new("numbers");

        let expr = numbers.reader().run(&env);
        let out = df.lazy().select([expr]).collect().unwrap();
        assert_eq!(
            out.column("numbers").unwrap().i32().unwrap().to_vec(),
            vec![Some(1), Some(2), Some(3)]
        );
    }

    #[test]
    fn field_modified_with_prefix() {
        let df = sample_dataframe_with_modified();
        let base_env = Environment::new(FieldResolver::new(df.get_column_names_str()));
        let env = base_env.with_prefix("modified_");
        let numbers = Field::new("numbers");

        let expr = numbers.reader().run(&env);
        let out = df.lazy().select([expr]).collect().unwrap();
        assert_eq!(
            out.column("modified_numbers")
                .unwrap()
                .i32()
                .unwrap()
                .to_vec(),
            vec![Some(10), Some(20), Some(30)]
        );
    }

    #[test]
    fn add_two_fields() {
        let df = sample_dataframe_with_modified();
        let env = Environment::new(FieldResolver::new(df.get_column_names_str()));
        let numbers = Field::new("numbers");
        let modified = Field::new("modified_numbers");

        let expr = (numbers.reader() + modified.reader()).run(&env);
        let out = df.lazy().select([expr]).collect().unwrap();
        assert_eq!(
            out.column("numbers").unwrap().i32().unwrap().to_vec(),
            vec![Some(11), Some(22), Some(33)]
        );
    }

    #[test]
    fn add_field_with_prefix() {
        let df = sample_dataframe_with_modified();
        let env = Environment::new(FieldResolver::new(df.get_column_names_str()));
        let numbers = Field::new("numbers");

        let expr = (numbers.reader() + use_prefix("modified_", numbers.reader())).run(&env);
        let out = df.lazy().select([expr]).collect().unwrap();
        assert_eq!(
            out.column("numbers").unwrap().i32().unwrap().to_vec(),
            vec![Some(11), Some(22), Some(33)]
        );
    }

    #[test]
    #[ignore]
    fn add_scalar() {
        let df = sample_dataframe_with_modified();
        let env = Environment::new(FieldResolver::new(df.get_column_names_str()));
        let numbers = Field::new("numbers");

        let expr = (numbers.reader() + 1).run(&env);
        let out = df.lazy().select([expr]).collect().unwrap();
        assert_eq!(
            out.column("numbers").unwrap().i32().unwrap().to_vec(),
            vec![Some(2), Some(3), Some(4)]
        );
    }

    #[test]
    #[ignore]
    fn map_single_reader() {
        let df = sample_dataframe_with_modified();
        let env = Environment::new(FieldResolver::new(df.get_column_names_str()));
        let numbers = Field::new("numbers");

        let expr = map(|a| a + lit(1), numbers.reader()).run(&env);
        let out = df.lazy().select([expr]).collect().unwrap();
        assert_eq!(
            out.column("numbers").unwrap().i32().unwrap().to_vec(),
            vec![Some(2), Some(3), Some(4)]
        );
    }

    #[test]
    fn map2_basic() {
        let df = sample_dataframe_with_modified();
        let env = Environment::new(FieldResolver::new(df.get_column_names_str()));
        let numbers = Field::new("numbers");
        let modified = Field::new("modified_numbers");

        let expr = map2(|a, b| a + b, numbers.reader(), modified.reader()).run(&env);
        let out = df.lazy().select([expr]).collect().unwrap();
        assert_eq!(
            out.column("numbers").unwrap().i32().unwrap().to_vec(),
            vec![Some(11), Some(22), Some(33)]
        );
    }

    #[test]
    fn ask_returns_environment() {
        let df = sample_dataframe_with_modified();
        let env = Environment::new(FieldResolver::new(df.get_column_names_str()));
        let out = ask().run(&env);
        assert_eq!(out, env);
    }

    #[test]
    fn asks_prefix() {
        let df = sample_dataframe_with_modified();
        let env = Environment::new(FieldResolver::new(df.get_column_names_str()))
            .with_prefix("modified_");
        let expr = asks(|e| e.resolver().prefix().to_string()).run(&env);
        let out = df.lazy().select([expr]).collect().unwrap();
        let values: Vec<Option<&str>> = out
            .column("literal")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(values, vec![Some("modified_")]);
    }

    #[test]
    #[ignore]
    fn pure_constant_addition() {
        let df = sample_dataframe_with_modified();
        let env = Environment::new(FieldResolver::new(df.get_column_names_str()));
        let numbers = Field::new("numbers");

        let expr = (numbers.reader() + pure(1i32)).run(&env);
        let out = df.lazy().select([expr]).collect().unwrap();
        assert_eq!(
            out.column("numbers").unwrap().i32().unwrap().to_vec(),
            vec![Some(2), Some(3), Some(4)]
        );
    }

    #[test]
    #[ignore]
    fn map2_with_asks() {
        let df = sample_dataframe_with_modified();
        let env = Environment::new(FieldResolver::new(df.get_column_names_str()))
            .with_prefix("modified_");
        let numbers = Field::new("numbers");
        let prefix_len = asks(|e| e.resolver().prefix().len() as i32);

        let expr = map2(|a, b| a + b, numbers.reader(), prefix_len).run(&env);
        let out = df.lazy().select([expr]).collect().unwrap();
        assert_eq!(
            out.column("modified_numbers")
                .unwrap()
                .i32()
                .unwrap()
                .to_vec(),
            vec![Some(19), Some(29), Some(39)]
        );
    }

    fn add_and_scale(a: Expr, b: Expr, factor: Expr) -> Expr {
        (a + b) * factor.cast(DataType::Int32)
    }

    fn add_two(a: Expr, b: Expr) -> Expr {
        a + b
    }

    #[test]
    #[ignore]
    fn field_function_basic() {
        let df = sample_dataframe_with_modified();
        let env = Environment::new(FieldResolver::new(df.get_column_names_str()));
        let numbers = Field::new("numbers");
        let modified = Field::new("modified_numbers");

        let expr = field_function3(
            add_and_scale,
            numbers.reader(),
            modified.reader(),
            pure(2i32),
        )
        .run(&env);
        let out = df.lazy().select([expr]).collect().unwrap();
        assert_eq!(
            out.column("numbers").unwrap().i32().unwrap().to_vec(),
            vec![Some(22), Some(44), Some(66)]
        );
    }

    #[test]
    #[ignore]
    fn field_function_with_reader_arg() {
        let df = sample_dataframe_with_modified();
        let env = Environment::new(FieldResolver::new(df.get_column_names_str()));
        let numbers = Field::new("numbers");

        let expr = field_function2(
            add_two,
            numbers.reader(),
            use_prefix("modified_", numbers.reader()),
        )
        .run(&env);
        let out = df.lazy().select([expr]).collect().unwrap();
        assert_eq!(
            out.column("numbers").unwrap().i32().unwrap().to_vec(),
            vec![Some(11), Some(22), Some(33)]
        );
    }
}

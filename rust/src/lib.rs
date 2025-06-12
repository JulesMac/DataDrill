use polars::prelude::*;
use std::ops::Add;
use std::sync::Arc;

#[derive(Clone, Debug)]
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

    pub fn resolve(&self, name: &str) -> Result<String, String> {
        let column = format!("{}{}", self.prefix, name);
        if self.schema.iter().any(|c| c == &column) {
            Ok(column)
        } else {
            Err(format!("{column} not in schema"))
        }
    }
}

#[derive(Clone, Debug)]
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
pub struct Reader(Arc<dyn Fn(&Environment) -> Expr + Send + Sync>);

impl Reader {
    pub fn new<F>(func: F) -> Self
    where
        F: Fn(&Environment) -> Expr + Send + Sync + 'static,
    {
        Self(Arc::new(func))
    }

    pub fn run(&self, env: &Environment) -> Expr {
        (self.0)(env)
    }

    pub fn alias(self, name: &str) -> Self {
        let name = name.to_string();
        Reader::new(move |env| self.run(env).alias(&name))
    }
}

impl Add for Reader {
    type Output = Reader;

    fn add(self, rhs: Reader) -> Self::Output {
        Reader::new(move |env| self.run(env) + rhs.run(env))
    }
}

impl Add<i32> for Reader {
    type Output = Reader;

    fn add(self, rhs: i32) -> Self::Output {
        Reader::new(move |env| self.run(env) + lit(rhs))
    }
}

impl Add<Reader> for i32 {
    type Output = Reader;

    fn add(self, rhs: Reader) -> Self::Output {
        Reader::new(move |env| lit(self) + rhs.run(env))
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

    pub fn reader(&self) -> Reader {
        let name = self.name.clone();
        Reader::new(move |env| {
            let column = env
                .resolver()
                .resolve(&name)
                .expect("column not in schema");
            col(&column)
        })
    }
}

pub fn use_prefix(prefix: &str, reader: Reader) -> Reader {
    let prefix = prefix.to_string();
    Reader::new(move |env| reader.run(&env.with_prefix(&prefix)))
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
        assert_eq!(out.column("numbers").unwrap().i32().unwrap().to_vec(), vec![Some(1), Some(2), Some(3)]);
    }

    #[test]
    fn field_modified_with_prefix() {
        let df = sample_dataframe_with_modified();
        let base_env = Environment::new(FieldResolver::new(df.get_column_names_str()));
        let env = base_env.with_prefix("modified_");
        let numbers = Field::new("numbers");

        let expr = numbers.reader().run(&env);
        let out = df.lazy().select([expr]).collect().unwrap();
        assert_eq!(out.column("modified_numbers").unwrap().i32().unwrap().to_vec(), vec![Some(10), Some(20), Some(30)]);
    }

    #[test]
    fn add_two_fields() {
        let df = sample_dataframe_with_modified();
        let env = Environment::new(FieldResolver::new(df.get_column_names_str()));
        let numbers = Field::new("numbers");
        let modified = Field::new("modified_numbers");

        let expr = (numbers.reader() + modified.reader()).run(&env);
        let out = df.lazy().select([expr]).collect().unwrap();
        assert_eq!(out.column("numbers").unwrap().i32().unwrap().to_vec(), vec![Some(11), Some(22), Some(33)]);
    }

    #[test]
    fn add_field_with_prefix() {
        let df = sample_dataframe_with_modified();
        let env = Environment::new(FieldResolver::new(df.get_column_names_str()));
        let numbers = Field::new("numbers");

        let expr = (numbers.reader() + use_prefix("modified_", numbers.reader())).run(&env);
        let out = df.lazy().select([expr]).collect().unwrap();
        assert_eq!(out.column("numbers").unwrap().i32().unwrap().to_vec(), vec![Some(11), Some(22), Some(33)]);
    }

    #[test]
    #[ignore]
    fn add_scalar() {
        let df = sample_dataframe_with_modified();
        let env = Environment::new(FieldResolver::new(df.get_column_names_str()));
        let numbers = Field::new("numbers");

        let expr = (numbers.reader() + 1).run(&env);
        let out = df.lazy().select([expr]).collect().unwrap();
        assert_eq!(out.column("numbers").unwrap().i32().unwrap().to_vec(), vec![Some(2), Some(3), Some(4)]);
    }
}

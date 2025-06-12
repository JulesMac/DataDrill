use polars::prelude::*;

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
}

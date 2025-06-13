use datadrill::*;
use polars::prelude::{DataType, Expr, IntoLazy, lit};

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
fn get_data_unmodified() {
    let df = sample_dataframe_with_modified();
    let env = Environment::new(FieldResolver::new(df.get_column_names_str()));

    let expr = get_data("numbers").run(&env);
    let out = df.lazy().select([expr]).collect().unwrap();
    assert_eq!(
        out.column("numbers").unwrap().i32().unwrap().to_vec(),
        vec![Some(1), Some(2), Some(3)]
    );
}

#[test]
fn get_data_modified_prefix() {
    let df = sample_dataframe_with_modified();
    let env =
        Environment::new(FieldResolver::new(df.get_column_names_str())).with_prefix("modified_");

    let expr = get_data("numbers").run(&env);
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
    let env =
        Environment::new(FieldResolver::new(df.get_column_names_str())).with_prefix("modified_");
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
fn map2_with_asks() {
    let df = sample_dataframe_with_modified();
    let env =
        Environment::new(FieldResolver::new(df.get_column_names_str())).with_prefix("modified_");
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

#[test]
fn sub_two_fields() {
    let df = sample_dataframe_with_modified();
    let env = Environment::new(FieldResolver::new(df.get_column_names_str()));
    let numbers = Field::new("numbers");
    let modified = Field::new("modified_numbers");

    let expr = (numbers.reader() - modified.reader()).run(&env);
    let out = df.lazy().select([expr]).collect().unwrap();
    assert_eq!(
        out.column("numbers").unwrap().i32().unwrap().to_vec(),
        vec![Some(-9), Some(-18), Some(-27)]
    );
}

#[test]
fn multiply_field_with_scalar() {
    let df = sample_dataframe_with_modified();
    let env = Environment::new(FieldResolver::new(df.get_column_names_str()));
    let numbers = Field::new("numbers");

    let expr = (numbers.reader() * 2i32).run(&env);
    let out = df.lazy().select([expr]).collect().unwrap();
    assert_eq!(
        out.column("numbers").unwrap().i32().unwrap().to_vec(),
        vec![Some(2), Some(4), Some(6)]
    );
}

#[test]
fn modulo_field_scalar() {
    let df = sample_dataframe_with_modified();
    let env = Environment::new(FieldResolver::new(df.get_column_names_str()));
    let numbers = Field::new("numbers");

    let expr = (numbers.reader() % 2i32).run(&env);
    let out = df.lazy().select([expr]).collect().unwrap();
    assert_eq!(
        out.column("numbers").unwrap().i32().unwrap().to_vec(),
        vec![Some(1), Some(0), Some(1)]
    );
}

#[test]
fn neg_field() {
    let df = sample_dataframe_with_modified();
    let env = Environment::new(FieldResolver::new(df.get_column_names_str()));
    let numbers = Field::new("numbers");

    let expr = (-numbers.reader()).run(&env);
    let out = df.lazy().select([expr]).collect().unwrap();
    assert_eq!(
        out.column("numbers").unwrap().i32().unwrap().to_vec(),
        vec![Some(-1), Some(-2), Some(-3)]
    );
}

#[test]
fn not_boolean_expression() {
    let df = sample_dataframe_with_modified();
    let env = Environment::new(FieldResolver::new(df.get_column_names_str()));
    let numbers = Field::new("numbers");

    let greater_than_one = map(|a| a.gt(lit(1)), numbers.reader());
    let expr = (!greater_than_one).alias("result").run(&env);
    let out = df.lazy().select([expr]).collect().unwrap();
    let values: Vec<Option<bool>> = out
        .column("result")
        .unwrap()
        .bool()
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(values, vec![Some(true), Some(false), Some(false)]);
}

fn add_and_scale(a: Expr, b: Expr, factor: Expr) -> Expr {
    (a + b) * factor.cast(DataType::Int32)
}

fn add_two(a: Expr, b: Expr) -> Expr {
    a + b
}

#[test]
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

#[test]
fn series_function_basic() {
    let df = sample_dataframe_with_modified();
    let env = Environment::new(FieldResolver::new(df.get_column_names_str()));
    let numbers = Field::new("numbers");
    let modified = Field::new("modified_numbers");

    let expr = series_function3(
        |a, b, factor| {
            let sum = (&a + &b).unwrap();
            let f = factor.cast(&DataType::Int32).unwrap();
            (&sum * &f).unwrap()
        },
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

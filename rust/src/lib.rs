use polars::prelude::*;

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
}

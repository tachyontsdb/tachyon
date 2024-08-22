#[cfg(test)]
mod tests {
    use crate::{utils::test::set_up_dirs, Connection, Inserter, Timestamp, ValueType};
    use std::{borrow::Borrow, collections::HashSet, iter::zip, path::PathBuf};

    fn create_stream_helper(conn: &mut Connection, stream: &str) -> Inserter {
        if !conn.check_stream_exists(stream) {
            conn.create_stream(stream, ValueType::UInteger64);
        }

        conn.prepare_insert(stream)
    }

    fn e2e_vector_test(
        root_dir: PathBuf,
        start: u64,
        end: u64,
        first_i: usize,
        expected_count: usize,
    ) {
        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];
        let values = [45u64, 47, 23, 48];

        let mut inserter =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "web"}"#);

        // Insert dummy data
        for (t, v) in zip(timestamps, values) {
            inserter.insert(t, v.into());
        }

        inserter.flush();

        // Prepare test query
        let query = r#"http_requests_total{service = "web"}"#;
        let mut stmt = conn.prepare_query(query, Some(start), Some(end));

        // Process results
        let mut i = first_i;
        let mut count = 0;
        loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }

            let res = res.unwrap();
            assert_eq!(timestamps[i], res.timestamp);
            assert_eq!(values[i], res.value.get_uinteger64());
            i += 1;

            count += 1;
        }

        assert_eq!(count, expected_count);
    }

    #[test]
    fn test_e2e_vector_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_vector_test(root_dir, 23, 51, 0, 4);
    }

    #[test]
    fn test_e2e_vector_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_vector_test(root_dir, 29, 40, 1, 2);
    }

    #[test]
    fn test_e2e_multiple_streams() {
        set_up_dirs!(dirs, "db");

        let root_dir = dirs[0].clone();
        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];
        let values = [45, 47, 23, 48];

        let mut inserter1 =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "web"}"#);

        for (t, v) in zip(timestamps, values) {
            inserter1.insert(t, v.into());
        }

        inserter1.flush();

        let timestamps_2 = [12, 15, 30, 67];
        let values_2 = [1, 5, 40, 20];

        let mut inserter2 =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "cool"}"#);

        for (t, v) in zip(timestamps_2, values_2) {
            inserter2.insert(t, v.into());
        }

        inserter2.flush();

        let mut stmt = conn.prepare_query(
            r#"http_requests_total{service = "web"}"#,
            Some(23),
            Some(51),
        );

        let mut i = 0;

        loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }
            let res = res.unwrap();
            assert_eq!(timestamps[i], res.timestamp);
            assert_eq!(values[i], res.value.get_uinteger64());
            i += 1;
        }

        assert_eq!(i, 4);

        let mut stmt = conn.prepare_query(
            r#"http_requests_total{service = "cool"}"#,
            Some(12),
            Some(67),
        );

        let mut i = 0;
        loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }
            let res = res.unwrap();
            assert_eq!(timestamps_2[i], res.timestamp);
            assert_eq!(values_2[i], res.value.get_uinteger64());
            i += 1;
        }

        assert_eq!(i, 4);
    }

    fn e2e_scalar_aggregate_test(
        root_dir: PathBuf,
        operation: &str,
        start: u64,
        end: u64,
        expected_val: u64,
    ) {
        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];
        let values = [45u64, 47, 23, 48];

        let mut inserter =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "web"}"#);

        // Insert dummy data
        for (t, v) in zip(timestamps, values) {
            inserter.insert(t, v.into());
        }

        inserter.flush();

        // Prepare test query
        let query = format!(r#"{}(http_requests_total{{service = "web"}})"#, operation);
        let mut stmt = conn.prepare_query(&query, Some(start), Some(end));

        // Process results
        let actual_val = stmt.next_scalar().unwrap();
        assert_eq!(actual_val.get_uinteger64(), expected_val);
        assert!(stmt.next_scalar().is_none());
    }

    #[test]
    fn test_e2e_sum_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "sum", 23, 51, 163)
    }

    #[test]
    fn test_e2e_sum_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "sum", 29, 40, 70)
    }

    #[test]
    fn test_e2e_count_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "count", 23, 51, 4)
    }

    #[test]
    fn test_e2e_count_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "count", 29, 40, 2)
    }

    #[test]
    fn test_e2e_avg_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "avg", 23, 51, 40)
    }

    #[test]
    fn test_e2e_avg_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "avg", 29, 40, 35)
    }

    #[test]
    fn test_e2e_min_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "min", 23, 51, 23)
    }

    #[test]
    fn test_e2e_min_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "min", 29, 40, 23)
    }

    #[test]
    fn test_e2e_max_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "max", 23, 51, 48)
    }

    #[test]
    fn test_e2e_max_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "max", 29, 40, 47)
    }

    fn e2e_scalars_aggregate_test(
        root_dir: PathBuf,
        operation: &str,
        param: u64,
        start: u64,
        end: u64,
        expected_val: Vec<u64>,
    ) {
        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 25, 29, 40, 44, 51];
        let values = [27u64, 31, 47, 23, 31, 48];

        let mut inserter =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "web"}"#);

        // Insert dummy data
        for (t, v) in zip(timestamps, values) {
            inserter.insert(t, v.into());
        }

        inserter.flush();

        // Prepare test query
        let query = format!(
            r#"{}({}, http_requests_total{{service = "web"}})"#,
            operation, param
        );
        let mut stmt = conn.prepare_query(&query, Some(start), Some(end));

        // Process results
        let mut actual_val: Vec<u64> = Vec::new();
        loop {
            let res = stmt.next_scalar();
            if res.is_none() {
                break;
            }
            actual_val.push(res.unwrap().get_uinteger64());
        }

        assert_eq!(actual_val, expected_val);
    }

    #[test]
    fn test_e2e_bottomk() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalars_aggregate_test(root_dir, "bottomk", 2, 23, 51, [23, 27].to_vec())
    }

    #[test]
    fn test_e2e_bottomk_zero_k() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalars_aggregate_test(root_dir, "bottomk", 0, 23, 51, [].to_vec())
    }

    #[test]
    fn test_e2e_bottomk_large_k() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalars_aggregate_test(
            root_dir,
            "bottomk",
            10000,
            23,
            51,
            [23, 27, 31, 31, 47, 48].to_vec(),
        )
    }

    #[test]
    fn test_e2e_topk() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalars_aggregate_test(root_dir, "topk", 2, 23, 51, [48, 47].to_vec())
    }

    #[test]
    fn test_e2e_topk_zero_k() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalars_aggregate_test(root_dir, "topk", 0, 23, 51, [].to_vec())
    }

    #[test]
    fn test_e2e_topk_large_k() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalars_aggregate_test(
            root_dir,
            "topk",
            10000,
            23,
            51,
            [48, 47, 31, 31, 27, 23].to_vec(),
        )
    }

    #[test]
    fn test_vector_to_vector_no_interpolation() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();

        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];

        let values_a = [45, 47, 23, 48];
        let values_b = [9, 18, 0, 100];

        let mut inserter1 =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "web"}"#);

        // Insert dummy data
        for (t, v) in zip(timestamps, values_a) {
            inserter1.insert(t, v.into());
        }

        inserter1.flush();

        let mut inserter2 =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "mobile"}"#);

        for (t, v) in zip(timestamps, values_b) {
            inserter2.insert(t, v.into());
        }

        inserter2.flush();

        // Prepare test query
        let query =
            r#"http_requests_total{service = "web"} * http_requests_total{service = "mobile"}"#;
        let mut stmt = conn.prepare_query(query, Some(0), Some(100));

        // Process results
        let mut i = 0;
        loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }

            let res = res.unwrap();
            assert_eq!(timestamps[i], res.timestamp);
            assert_eq!(values_a[i] * values_b[i], res.value.get_uinteger64());
            i += 1;
        }
    }

    fn vec_union<T: Ord + Eq + std::hash::Hash + Clone>(v1: &Vec<T>, v2: &Vec<T>) -> Vec<T> {
        let mut set = HashSet::<T>::new();

        for e in v1 {
            set.insert(e.clone());
        }

        for e in v2 {
            set.insert(e.clone());
        }

        let mut vec: Vec<T> = set.into_iter().collect();
        vec.sort();

        vec
    }

    fn e2e_vector_to_vector_test(
        root_dir: PathBuf,
        timestamps_a: Vec<Timestamp>,
        values_a: Vec<u64>,
        timestamps_b: Vec<Timestamp>,
        values_b: Vec<u64>,
        expected_timestamps: Vec<Timestamp>,
        expected_values: Vec<u64>,
    ) {
        let mut conn = Connection::new(root_dir);

        let mut inserter1 =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "web"}"#);

        // Insert dummy data
        for (t, v) in zip(timestamps_a, values_a) {
            inserter1.insert(t, v.into());
        }

        inserter1.flush();

        let mut inserter2 =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "mobile"}"#);

        for (t, v) in zip(timestamps_b, values_b) {
            inserter2.insert(t, v.into());
        }

        inserter2.flush();

        // Prepare test query
        let query =
            r#"http_requests_total{service = "web"} + http_requests_total{service = "mobile"}"#;
        let mut stmt = conn.prepare_query(query, Some(0), Some(100));

        // Process results
        let mut i = 0;
        loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }

            let res = res.unwrap();
            assert_eq!(
                expected_values[i],
                res.value.get_uinteger64(),
                "Comparison failed at time {} with expected {} and actual {}",
                expected_timestamps[i],
                expected_values[i],
                res.value.get_uinteger64()
            );
            assert_eq!(expected_timestamps[i], res.timestamp);
            i += 1;
        }
    }

    #[test]
    fn test_vector_to_vector_basic_interpolation_1() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();

        let timestamps_a = vec![10, 20, 30, 40];
        let values_a = vec![0, 20, 0, 20];

        let timestamps_b = vec![5, 15, 25, 35, 45];
        let values_b = vec![10, 10, 10, 10, 10];

        let expected_values = vec![10, 10, 20, 30, 20, 10, 20, 30, 30];
        let expected_timestamps = vec_union(timestamps_a.borrow(), timestamps_b.borrow());

        e2e_vector_to_vector_test(
            root_dir,
            timestamps_a,
            values_a,
            timestamps_b,
            values_b,
            expected_timestamps,
            expected_values,
        )
    }

    #[test]
    fn test_vector_to_vector_basic_interpolation_2() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();

        let timestamps_a = vec![5, 15, 25, 35, 45];
        let values_a = vec![10, 10, 10, 10, 10];

        let timestamps_b = vec![10, 20, 30, 40];
        let values_b = vec![0, 20, 0, 20];

        let expected_values = vec![10, 10, 20, 30, 20, 10, 20, 30, 30];
        let expected_timestamps = vec_union(timestamps_a.borrow(), timestamps_b.borrow());

        e2e_vector_to_vector_test(
            root_dir,
            timestamps_a,
            values_a,
            timestamps_b,
            values_b,
            expected_timestamps,
            expected_values,
        )
    }

    #[test]
    fn test_vector_to_vector_complex_interpolation() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();

        let timestamps_a = vec![1, 2, 4, 6, 10, 12, 13, 14, 15, 16];
        let values_a = vec![10, 20, 30, 20, 20, 10, 15, 20, 80, 100];

        let timestamps_b = vec![3, 5, 7, 8, 9, 11, 16];
        let values_b = vec![30, 30, 10, 20, 20, 10, 10];

        let expected_values = vec![
            40, 50, 55, 60, 55, 40, 30, 40, 40, 35, 25, 20, 25, 30, 90, 110,
        ];
        let expected_timestamps = vec_union(timestamps_a.borrow(), timestamps_b.borrow());

        e2e_vector_to_vector_test(
            root_dir,
            timestamps_a,
            values_a,
            timestamps_b,
            values_b,
            expected_timestamps,
            expected_values,
        )
    }

    #[test]
    fn test_vector_to_scalar() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();

        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];

        let values_a = [45, 47, 23, 48];
        let values_b = [9, 18, 0, 100];

        let mut inserter1 =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "web"}"#);

        // Insert dummy data
        for (t, v) in zip(timestamps, values_a) {
            inserter1.insert(t, v.into());
        }

        inserter1.flush();

        let mut inserter2 =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "mobile"}"#);

        for (t, v) in zip(timestamps, values_b) {
            inserter2.insert(t, v.into());
        }

        inserter2.flush();

        // Prepare test query
        let query = r#"http_requests_total{service = "web"} + sum(http_requests_total{service = "mobile"})"#;
        let mut stmt = conn.prepare_query(query, Some(0), Some(100));

        // Process results
        let mut i = 0;
        let sum_values_b = values_b.iter().sum::<u64>();
        loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }

            let res = res.unwrap();
            assert_eq!(timestamps[i], res.timestamp);
            assert_eq!(values_a[i] + sum_values_b, res.value.get_uinteger64());
            i += 1;
        }
    }

    #[test]
    fn test_scalar_to_scalar() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();

        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];

        let values_a = [45, 47, 23, 48];
        let values_b = [9, 18, 0, 100];

        let mut inserter1 =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "web"}"#);

        // Insert dummy data
        for (t, v) in zip(timestamps, values_a) {
            inserter1.insert(t, v.into());
        }

        inserter1.flush();

        let mut inserter2 =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "mobile"}"#);

        for (t, v) in zip(timestamps, values_b) {
            inserter2.insert(t, v.into());
        }

        inserter2.flush();

        // Prepare test query
        let query = r#"sum(http_requests_total{service = "web"}) / sum(http_requests_total{service = "mobile"})"#;
        let mut stmt = conn.prepare_query(query, Some(0), Some(100));

        // Process results
        let sum_values_a = values_a.iter().sum::<u64>();
        let sum_values_b = values_b.iter().sum::<u64>();

        loop {
            let res = stmt.next_scalar();
            if res.is_none() {
                break;
            }

            let res = res.unwrap();
            assert_eq!(sum_values_a / sum_values_b, res.get_uinteger64());
        }
    }
}

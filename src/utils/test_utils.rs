use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

macro_rules! set_up_files {
        ($path_var:ident, $($x:expr),+ $(,)?) => {
            let _name = {
                fn f() {}
                fn type_name_of<T>(_: T) -> &'static str {
                    std::any::type_name::<T>()
                }
                let name = type_name_of(f);

                // Find and cut the rest of the path
                match &name[..name.len() - 3].rfind(':') {
                    Some(pos) => &name[pos + 1..name.len() - 3],
                    None => &name[..name.len() - 3],
                }
            };
            let _paths = vec![$($x.to_string()),+];
            let _name = _name.to_string() + "$";
            let mut _new_paths = vec![_name; _paths.len()];
            for (i, path) in _paths.iter().enumerate() {
                _new_paths[i] = _new_paths[i].clone() + &path;
            }

            let _tmp = TestFile::new(&_new_paths);
            let $path_var = _tmp.paths.clone();
        };
    }

pub(crate) use set_up_files;

use crate::{
    common::{Timestamp, Value},
    storage::file::TimeDataFile,
};

pub const TEST_DIR: &str = "tmp";

// Will create and drop a file with a given path
pub struct TestFile {
    pub paths: Vec<PathBuf>,
}

impl TestFile {
    pub fn new<S: AsRef<str>>(paths: &[S]) -> Self {
        if (!Path::new(TEST_DIR).exists()) {
            std::fs::create_dir_all(TEST_DIR);
        }
        let mut new_paths: Vec<PathBuf> = Vec::new();

        for path in paths {
            let new_path = Path::new(TEST_DIR).join(path.as_ref());
            new_paths.push(new_path);
        }
        println!("Test Paths: {:#?}", new_paths);
        Self { paths: new_paths }
    }

    pub fn new_and_create<S: AsRef<str>>(paths: &[S]) -> Self {
        if (!Path::new(TEST_DIR).exists()) {
            std::fs::create_dir_all(TEST_DIR);
        }
        for path in paths {
            let new_path = Path::new(TEST_DIR).join(path.as_ref());
            std::fs::File::create(&new_path);
        }

        Self::new(paths)
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        for path in &self.paths {
            std::fs::remove_file(path);
        }
    }
}

pub fn generate_ty_file(path: PathBuf, timestamps: &[Timestamp], values: &[Value]) {
    assert!(timestamps.len() == values.len());
    let mut model = TimeDataFile::new();

    for i in 0..timestamps.len() {
        model.write_data_to_file_in_mem(timestamps[i], values[i])
    }
    model.write(path);
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::str::FromStr;

    use super::set_up_files;
    use super::TestFile;
    use super::TEST_DIR;

    #[test]
    fn test_file_no_creation() {
        let path = "test_file_no_creation.ty";
        let file = TestFile::new(&[path]);
        let expected_path = TEST_DIR.to_string() + "/test_file_no_creation.ty";
        assert!(!Path::new(&expected_path.as_str()).exists());

        std::fs::File::create(&expected_path);
        drop(file);
        assert!(!Path::new(&expected_path.as_str()).exists());
    }

    #[test]
    fn test_file_creation() {
        let path = "test_file_creation.ty";
        let file = TestFile::new_and_create(&[path]);
        let expected_path = TEST_DIR.to_string() + "/test_file_creation.ty";
        assert!(Path::new(&expected_path.as_str()).exists());
        drop(file);
        assert!(!Path::new(&expected_path.as_str()).exists());
    }
}

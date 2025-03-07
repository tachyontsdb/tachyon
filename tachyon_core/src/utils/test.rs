use crate::storage::file::TimeDataFile;
use crate::{StreamId, Timestamp, Value, ValueType, Version};
use std::fs;
use std::path::{Path, PathBuf};

pub const TEST_DIR: &str = "../tmp";

macro_rules! set_up_dirs {
    ($dir_var:ident, $($x:expr),+ $(,)? ) => {
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
        let _dirs = vec![$($x.to_string()),+];
        let _name = _name.to_string() + "$";
        let mut _new_dirs = vec![_name; _dirs.len()];
        for (i, dir) in _dirs.iter().enumerate() {
            _new_dirs[i] = _new_dirs[i].clone() + &dir;
        }

        let _tmp = crate::utils::test::TestDir::new_and_create(&_new_dirs);
        let $dir_var = _tmp.dirs.clone();
    };
}

pub(crate) use set_up_dirs;

pub struct TestDir {
    pub dirs: Vec<PathBuf>,
}

impl TestDir {
    pub fn new_and_create(dirs: &[impl AsRef<str>]) -> Self {
        if !Path::new(TEST_DIR).exists() {
            fs::create_dir_all(TEST_DIR).unwrap();
        }
        let mut new_dirs: Vec<PathBuf> = Vec::new();
        for dir in dirs {
            let new_path = Path::new(TEST_DIR).join(dir.as_ref());
            fs::create_dir_all(&new_path).unwrap();
            new_dirs.push(new_path);
        }
        println!("Test Paths: {:#?}", new_dirs);

        Self { dirs: new_dirs }
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        for dir in &self.dirs {
            fs::remove_dir_all(dir).unwrap();
        }
    }
}

/**
   A common use case is to create files, and you want to drop those files (even if the test fails. To do so, we have to use RAII in Rust).

   `set_up_files!` macro does this for you. It will create files in a tmp directory (created if it doesn't exist), generate a name from
   the temp directory & test name, and assign the variables to a variable.

   @param First parameter is the name of the variable
   @param The next args are all path strings.

   e.g. set_up_files!(paths, "cool.ty", "nice.ty")

   The files specified by the paths will be dropped when the scope of which the macro is called is done (RAII).
*/
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

        let _tmp = crate::utils::test::TestFile::new(&_new_paths);
        let $path_var = _tmp.paths.clone();
    };
}

pub(crate) use set_up_files;

// Will create and drop a file with a given path
pub struct TestFile {
    pub paths: Vec<PathBuf>,
}

impl TestFile {
    pub fn new(paths: &[impl AsRef<str>]) -> Self {
        if !Path::new(TEST_DIR).exists() {
            fs::create_dir_all(TEST_DIR).unwrap();
        }
        let mut new_paths: Vec<PathBuf> = Vec::new();
        for path in paths {
            let new_path = Path::new(TEST_DIR).join(path.as_ref());
            new_paths.push(new_path);
        }
        println!("Test Paths: {:#?}", new_paths);

        Self { paths: new_paths }
    }

    pub fn new_and_create(paths: &[impl AsRef<str>]) -> Self {
        if !Path::new(TEST_DIR).exists() {
            fs::create_dir_all(TEST_DIR).unwrap();
        }
        for path in paths {
            let new_path = Path::new(TEST_DIR).join(path.as_ref());
            fs::File::create(&new_path).unwrap();
        }

        Self::new(paths)
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        for path in &self.paths {
            fs::remove_file(path).unwrap();
        }
    }
}

pub fn generate_ty_file(path: PathBuf, timestamps: &[Timestamp], values: &[Value]) {
    assert!(timestamps.len() == values.len());
    let mut model = TimeDataFile::new(Version(0), StreamId(0), ValueType::UInteger64);

    for i in 0..timestamps.len() {
        model.write_data_to_file_in_mem(timestamps[i], values[i])
    }
    model.write(path);
}

#[cfg(test)]
mod tests {
    use crate::utils::test::{TestDir, TestFile, TEST_DIR};
    use std::fs::File;
    use std::path::Path;

    #[test]
    fn test_file_no_creation() {
        let path = "test_file_no_creation.ty";
        let file = TestFile::new(&[path]);
        let expected_path = TEST_DIR.to_string() + "/test_file_no_creation.ty";
        assert!(!Path::new(&expected_path.as_str()).exists());

        File::create(&expected_path).unwrap();
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

    #[test]
    fn test_dir_creation() {
        let dir_name: &str = "test_dir";
        let dir = TestDir::new_and_create(&[dir_name]);
        let expected_dir = TEST_DIR.to_string() + "/test_dir";
        assert!(Path::new(&expected_dir.as_str()).exists());
        drop(dir);
        assert!(!Path::new(&expected_dir.as_str()).exists());
    }
}

#[cfg(test)]
pub mod test;

macro_rules! static_assert {
    ($($tt: tt)*) => {
        const _: () = assert!($($tt)*);
    }
}

pub(crate) use static_assert;

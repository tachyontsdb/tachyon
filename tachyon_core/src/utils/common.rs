// https://internals.rust-lang.org/t/nicer-static-assertions/15986

macro_rules! static_assert {
    ($($tt:tt)*) => {
        const _: () = assert!($($tt)*);
    }
}
pub(crate) use static_assert;

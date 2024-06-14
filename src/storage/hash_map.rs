use std::ptr::{null, null_mut};

// not safe to clone unless option is None
#[derive(Clone)]
struct Node<V> {
    key: u64,
    val: V,
    next: *mut Node<V>,
}

pub(crate) struct IDLookup<V: Sized + Copy> {
    table: Vec<*mut Node<V>>,
    size: usize,
}

impl<V: Copy> IDLookup<V> {
    pub fn new_with_size(size: usize) -> Self {
        IDLookup {
            table: vec![null_mut(); size],
            size,
        }
    }

    pub fn get(&self, key: u64) -> Option<V> {
        let idx = (key as usize) % self.table.len();
        let mut cur = self.table[idx];
        unsafe {
            while (!cur.is_null() && (*cur).key != key) {
                cur = (*cur).next;
            }
        }
        if cur.is_null() {
            return None;
        }
        let val = unsafe { (*cur).val };
        Some(val)
    }

    pub fn insert(&mut self, key: u64, value: V) {
        let idx = (key as usize) % self.table.len();
        let mut cur = self.table[idx];
        unsafe {
            while (!cur.is_null() && (*cur).key != key) {
                cur = (*cur).next;
            }
        }
        if cur.is_null() {
            self.table[idx] = Box::into_raw(Box::new(Node {
                key,
                val: value,
                next: self.table[idx],
            }));
        } else {
            unsafe {
                (*cur).val = value;
            }
        }
        self.size += 1;
    }
    pub fn remove(&mut self, key: u64) {
        let idx = (key as usize) % self.table.len();
        let mut cur = self.table[idx];
        let mut prev = null_mut();
        unsafe {
            while (!cur.is_null() && (*cur).key != key) {
                prev = cur;
                cur = (*cur).next;
            }
        }
        if cur != null_mut() {
            if prev.is_null() {
                self.table[idx] = null_mut();
            } else {
                unsafe {
                    (*prev).next = (*cur).next;
                }
            }

            unsafe {
                drop(Box::from_raw(cur));
            }
        }
        self.size -= 1;
    }

    pub fn rehash(&mut self) {}
}

impl<V> Drop for Node<V> {
    fn drop(&mut self) {
        if !self.next.is_null() {
            unsafe {
                drop(Box::from_raw(self.next));
            }
        }
    }
}

impl<V: Copy> Drop for IDLookup<V> {
    fn drop(&mut self) {
        while (!self.table.is_empty()) {
            let item = self.table.pop().unwrap();
            if !item.is_null() {
                unsafe { drop(Box::from_raw(item)) };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get() {
        let mut hm = IDLookup::<u64>::new_with_size(10);
        assert_eq!(hm.get(4), None);

        hm.insert(45, 234);
        hm.insert(15, 123);
        hm.insert(25, 23);
        hm.insert(45, 1);
        hm.insert(43, 22);
        assert_eq!(hm.get(25), Some(23));
        assert_eq!(hm.get(45), Some(1));
        assert_eq!(hm.get(43), Some(22));

        hm.remove(45);
        assert_eq!(hm.get(45), None);
        assert_eq!(hm.get(15), Some(123));
    }
}

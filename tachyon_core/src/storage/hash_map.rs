use std::ptr;

/// Not safe to clone unless next is null
#[derive(Clone)]
struct Node<V> {
    key: u64,
    val: V,
    next: *mut Node<V>,
}

pub struct IDLookup<V: Copy> {
    size: usize,
    table: Vec<*mut Node<V>>,
}

impl<V: Copy> IDLookup<V> {
    pub fn new_with_capacity(capacity: usize) -> Self {
        IDLookup {
            size: 0,
            table: vec![ptr::null_mut(); capacity],
        }
    }

    pub fn get(&self, key: &u64) -> Option<V> {
        let idx = (*key as usize) % self.table.len();
        let mut cur = self.table[idx];
        unsafe {
            while !cur.is_null() && (*cur).key != *key {
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
            while !cur.is_null() && (*cur).key != key {
                cur = (*cur).next;
            }
        }
        if cur.is_null() {
            self.table[idx] = Box::into_raw(Box::new(Node {
                key,
                val: value,
                next: self.table[idx],
            }));
            self.size += 1;
        } else {
            unsafe {
                (*cur).val = value;
            }
        }
    }

    pub fn remove(&mut self, key: &u64) {
        let idx = (*key as usize) % self.table.len();
        let mut cur = self.table[idx];
        let mut prev = ptr::null_mut();
        unsafe {
            while !cur.is_null() && (*cur).key != *key {
                prev = cur;
                cur = (*cur).next;
            }
        }

        if !cur.is_null() {
            if prev.is_null() {
                self.table[idx] = unsafe { (*cur).next };
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
}

impl<V: Copy> Drop for IDLookup<V> {
    fn drop(&mut self) {
        while let Some(mut item) = self.table.pop() {
            while !item.is_null() {
                let next = unsafe { (*item).next };
                unsafe { drop(Box::from_raw(item)) };
                item = next;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get() {
        let mut hm = IDLookup::<u64>::new_with_capacity(10);
        assert_eq!(hm.get(&4), None);

        hm.insert(45, 234);
        hm.insert(15, 123);
        hm.insert(25, 23);
        hm.insert(45, 1);
        hm.insert(43, 22);
        assert_eq!(hm.get(&25), Some(23));
        assert_eq!(hm.get(&45), Some(1));
        assert_eq!(hm.get(&43), Some(22));
        assert_eq!(hm.size, 4);

        hm.remove(&45);
        assert_eq!(hm.size, 3);
        assert_eq!(hm.get(&45), None);
        assert_eq!(hm.get(&15), Some(123));
    }

    #[test]
    fn test_insert_remove() {
        let mut hm = IDLookup::<u64>::new_with_capacity(10);
        hm.insert(4, 5);

        // will all get inserted into same bucket
        hm.insert(14, 3);
        hm.insert(24, 5);
        hm.insert(34, 8);
        hm.insert(44, 9);

        assert_eq!(hm.get(&24), Some(5));
        assert_eq!(hm.get(&44), Some(9));
        assert_eq!(hm.size, 5);

        // remove head
        hm.remove(&4);
        assert_eq!(hm.get(&4), None);
        assert_eq!(hm.get(&14), Some(3));

        // remove tail
        hm.remove(&44);
        assert_eq!(hm.get(&44), None);
        assert_eq!(hm.get(&14), Some(3));
        assert_eq!(hm.get(&34), Some(8));

        // remove middle
        hm.remove(&24);
        assert_eq!(hm.get(&24), None);
        assert_eq!(hm.get(&14), Some(3));
        assert_eq!(hm.get(&34), Some(8));

        // remove the rest
        hm.remove(&14);
        hm.remove(&34);

        assert_eq!(hm.size, 0);

        assert_eq!(hm.get(&34), None);
        assert_eq!(hm.get(&14), None);

        // test update
        hm.insert(4, 23);
        assert_eq!(hm.get(&4), Some(23));

        hm.insert(4, 24);
        assert_eq!(hm.get(&4), Some(24));

        hm.remove(&4);
        assert_eq!(hm.get(&4), None);

        assert_eq!(hm.size, 0);
    }
}

use std::ptr::null_mut;

const SHIFT: u64 = 8;
const FANOUT: usize = 1 << SHIFT;
const MASK: u64 = (1 << SHIFT) - 1;

struct RNode {
    shift: u8,
    offset: u8,
    count: u8,
    labels: [u64; FANOUT],

    // either stores pointer to child or value
    children: [*mut RNode; FANOUT],
    parent: *mut RNode,
    value_bitmap: u64,
}

impl RNode {
    fn get(&self, key: u64) -> Option<u64> {
        let idx = ((key >> self.shift) & MASK) as usize;
        let child = self.children[idx];

        if self.value_bitmap & (1 << idx) != 0 {
            if key == self.labels[idx] {
                return Some(child as u64);
            }
            return None;
        }

        if child.is_null() {
            return None;
        }

        unsafe { (*child).get(key) }
    }

    fn first_differing(&self, a: u64, b: u64) -> u8 {
        for i in (0..(self.shift >> 3)).rev() {
            if ((a >> (i << 3)) & MASK) != ((b >> (i << 3)) & MASK) {
                return i << 3;
            }
        }
        // should not happen
        panic!("Equal keys compared")
    }

    fn insert_node(&mut self, label: u64, node: *mut RNode) {
        // let new_shift = self.first_differing(a, b);
        todo!()
    }

    fn insert(&mut self, key: u64, value: u64) {
        let idx = ((key >> self.shift) & MASK) as usize;
        let child: *mut RNode = self.children[idx];

        // if singular value is already stored in position
        if self.value_bitmap & (1 << idx) != 0 {
            if self.labels[idx] == key {
                self.children[idx] = value as *mut RNode;
            } else {
                let child_key = child as u64;
                let new_shift = self.first_differing(child_key, key);

                let mut new_node = RNode {
                    shift: new_shift,
                    offset: idx as u8,
                    count: 0,
                    labels: [0; FANOUT],
                    children: [null_mut(); FANOUT],
                    parent: self,
                    value_bitmap: 0,
                };
                new_node.insert(self.labels[idx], child as u64);
                new_node.insert(key, value);

                self.value_bitmap ^= (1 << idx);
                self.labels[idx] = key & !((1 << (new_shift + 8)) - 1);
                self.children[idx] = Box::into_raw(Box::new(new_node));
            }
            return;
        }

        if child.is_null() {
            self.value_bitmap |= (1 << idx);
            self.labels[idx] = key;
            self.children[idx] = value as *mut RNode;
            return;
        }

        // easy case
        if (key & self.labels[idx] == self.labels[idx]) {
            unsafe {
                (*child).insert(key, value);
            }
            return;
        }
        let new_shift = self.first_differing(key, self.labels[idx]);
        let mut new_node = RNode {
            shift: new_shift,
            offset: idx as u8,
            count: 0,
            labels: [0; FANOUT],
            children: [null_mut(); FANOUT],
            parent: self,
            value_bitmap: 0,
        };
        new_node.insert(key, value);
        new_node.insert_node(self.labels[idx], self.children[idx]);
        self.labels[idx] = key & !((1 << (new_shift + 8)) - 1);
        self.children[idx] = Box::into_raw(Box::new(new_node));
    }

    fn remove(&mut self, key: u64) {}
}

struct RadixTree {
    root: RNode,
}

impl RadixTree {
    pub fn new() -> Self {
        Self {
            root: RNode {
                shift: ((SHIFT - 1) << 3) as u8,
                offset: 0,
                count: 0,
                labels: [0; FANOUT],
                children: [null_mut(); FANOUT],
                parent: null_mut(),
                value_bitmap: 0,
            },
        }
    }
    pub fn get(&self, key: u64) -> Option<u64> {
        self.root.get(key)
    }
    pub fn insert(&mut self, key: u64, value: u64) {
        self.root.insert(key, value);
    }
}

#[cfg(test)]
mod tests {
    use super::RadixTree;

    #[test]
    fn test_insert() {
        let mut tree = RadixTree::new();

        let res = tree.get(23);
        assert!(res.is_none());

        tree.insert(23, 34);
        let res = tree.get(23);
        assert!(res.is_some());
        assert_eq!(res.unwrap(), 34);

        tree.insert(45, 23);
        let res: Option<u64> = tree.get(45);
        assert!(res.is_some());
        assert_eq!(res.unwrap(), 23);

        tree.insert(54, 2);
        let res: Option<u64> = tree.get(54);
        assert!(res.is_some());
        assert_eq!(res.unwrap(), 2);
    }
}

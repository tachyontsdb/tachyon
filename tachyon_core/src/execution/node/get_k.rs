use crate::{Connection, ReturnType, Value, ValueType, Vector};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use super::{ExecutorNode, TNode};

struct TypeValuePair(ValueType, Value);

impl PartialEq for TypeValuePair {
    fn eq(&self, other: &Self) -> bool {
        self.1.eq(self.0, &other.1, other.0)
    }
}

impl Eq for TypeValuePair {}

impl PartialOrd for TypeValuePair {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TypeValuePair {
    fn cmp(&self, other: &Self) -> Ordering {
        self.1.partial_cmp(self.0, &other.1, other.0).unwrap()
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum GetKType {
    Bottom,
    Top,
}

pub struct GetKNode {
    getk_type: GetKType,
    child: Box<TNode>,
    param: Box<TNode>,

    k: Option<usize>,

    ix: usize,
    ks: Vec<Value>,
}

impl GetKNode {
    pub fn new(
        _: &mut Connection,
        getk_type: GetKType,
        child: Box<TNode>,
        param: Box<TNode>,
    ) -> Self {
        Self {
            getk_type,
            child,
            param,
            k: None,
            ks: Vec::new(),
            ix: 0,
        }
    }
}

impl ExecutorNode for GetKNode {
    fn value_type(&self) -> ValueType {
        self.child.value_type()
    }

    fn return_type(&self) -> ReturnType {
        ReturnType::Scalar
    }

    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        if self.k.is_none() {
            // Generate heaps during the first call

            let k = ((self.param.next_scalar(conn).unwrap())
                .convert_into_u64(self.param.value_type())) as usize;
            self.k = Some(k);

            self.ks = if k == 0 {
                Vec::new()
            } else {
                let child_value_type = self.child.value_type();

                // Newer values overwrite older values in case of ties

                if self.getk_type == GetKType::Bottom {
                    let mut maxheap = BinaryHeap::<TypeValuePair>::new();
                    while let Some(Vector { value, .. }) = self.child.next_vector(conn) {
                        if maxheap.len() < k {
                            maxheap.push(TypeValuePair(child_value_type, value));
                        } else {
                            let ordering = value
                                .partial_cmp_same(child_value_type, &maxheap.peek().unwrap().1)
                                .unwrap();
                            if ordering.is_le() {
                                maxheap.pop();
                                maxheap.push(TypeValuePair(child_value_type, value));
                            }
                        }
                    }
                    maxheap
                        .into_sorted_vec()
                        .into_iter()
                        .map(|pair| pair.1)
                        .collect()
                } else {
                    let mut minheap = BinaryHeap::<Reverse<TypeValuePair>>::new();
                    while let Some(Vector { value, .. }) = self.child.next_vector(conn) {
                        if minheap.len() < k {
                            minheap.push(Reverse(TypeValuePair(child_value_type, value)));
                        } else {
                            let ordering = value
                                .partial_cmp_same(child_value_type, &minheap.peek().unwrap().0 .1)
                                .unwrap();
                            if ordering.is_ge() {
                                minheap.pop();
                                minheap.push(Reverse(TypeValuePair(child_value_type, value)));
                            }
                        }
                    }
                    minheap
                        .into_sorted_vec()
                        .into_iter()
                        .map(|rev_pair| rev_pair.0 .1)
                        .collect()
                }
            };
        }

        if self.ix >= self.ks.len() {
            None
        } else {
            let value = self.ks[self.ix];
            self.ix += 1;
            Some(value)
        }
    }
}

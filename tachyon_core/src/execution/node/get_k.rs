use crate::{Connection, ReturnType, ValueType, Vector};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use super::{ExecutorNode, TNode};

struct TypeVectorPair(ValueType, Vector);

impl PartialEq for TypeVectorPair {
    fn eq(&self, other: &Self) -> bool {
        self.1.value.eq(self.0, &other.1.value, other.0)
    }
}

impl Eq for TypeVectorPair {}

impl PartialOrd for TypeVectorPair {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TypeVectorPair {
    fn cmp(&self, other: &Self) -> Ordering {
        self.1
            .value
            .partial_cmp(self.0, &other.1.value, other.0)
            .unwrap()
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
    ks: Vec<Vector>,
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
        ReturnType::Vector
    }

    fn next_vector(&mut self, conn: &mut Connection) -> Option<Vector> {
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
                    let mut maxheap = BinaryHeap::<TypeVectorPair>::new();
                    while let Some(vector) = self.child.next_vector(conn) {
                        if maxheap.len() < k {
                            maxheap.push(TypeVectorPair(child_value_type, vector));
                        } else {
                            let ordering = vector
                                .value
                                .partial_cmp_same(
                                    child_value_type,
                                    &maxheap.peek().unwrap().1.value,
                                )
                                .unwrap();
                            if ordering.is_le() {
                                maxheap.pop();
                                maxheap.push(TypeVectorPair(child_value_type, vector));
                            }
                        }
                    }
                    maxheap.into_iter().map(|pair| pair.1).collect()
                } else {
                    let mut minheap = BinaryHeap::<Reverse<TypeVectorPair>>::new();
                    while let Some(vector) = self.child.next_vector(conn) {
                        if minheap.len() < k {
                            minheap.push(Reverse(TypeVectorPair(child_value_type, vector)));
                        } else {
                            let ordering = vector
                                .value
                                .partial_cmp_same(
                                    child_value_type,
                                    &minheap.peek().unwrap().0 .1.value,
                                )
                                .unwrap();
                            if ordering.is_ge() {
                                minheap.pop();
                                minheap.push(Reverse(TypeVectorPair(child_value_type, vector)));
                            }
                        }
                    }
                    minheap.into_iter().map(|rev_pair| rev_pair.0 .1).collect()
                }
            };

            // Return values in order of timestamp
            self.ks.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        }

        if self.ix >= self.ks.len() {
            None
        } else {
            let vector = self.ks[self.ix];
            self.ix += 1;
            Some(vector)
        }
    }
}

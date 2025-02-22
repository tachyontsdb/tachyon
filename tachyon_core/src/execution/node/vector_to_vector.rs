use crate::{Connection, ReturnType, Timestamp, Value, ValueType, Vector};

use std::collections::VecDeque;

use super::{BinaryOp, ExecutorNode, TNode};

pub struct VectorToVectorNode {
    op: BinaryOp,
    lhs: Box<TNode>,
    rhs: Box<TNode>,
    lhs_range: VecDeque<Vector>,
    rhs_range: VecDeque<Vector>,
    value_opt: Option<(Vector, VectorToVectorStream)>,
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum VectorToVectorStream {
    Lhs = 0,
    Rhs = 1,
}

impl VectorToVectorNode {
    pub fn new(op: BinaryOp, lhs: Box<TNode>, rhs: Box<TNode>) -> Self {
        Self {
            op,
            lhs,
            rhs,
            lhs_range: VecDeque::new(),
            rhs_range: VecDeque::new(),
            value_opt: None,
        }
    }

    fn calculate_value_with_linear_interpolation(
        &self,
        ts: Timestamp,
        stream: VectorToVectorStream,
    ) -> Value {
        let range = match stream {
            VectorToVectorStream::Lhs => &self.lhs_range,
            VectorToVectorStream::Rhs => &self.rhs_range,
        };

        if range.len() == 1 {
            range[0].value
        } else if range.len() == 2 {
            let (v1, v2) = (range[1].value, range[0].value);
            let (t1, t2) = (range[1].timestamp, range[0].timestamp);

            let rhs_value_type = self.rhs.value_type();

            let slope = (v2.convert_into_f64(self.lhs.value_type())
                - v1.convert_into_f64(rhs_value_type))
                / (t2 as f64 - t1 as f64);
            let res =
                ((ts as f64 - t1 as f64) * slope + v1.convert_into_f64(rhs_value_type)).round();

            // TODO: Allow floats
            (res as u64).into()
        } else {
            panic!("No values in range for interpolation.")
        }
    }

    fn next_child_vector(
        &mut self,
        conn: &mut Connection,
        stream: VectorToVectorStream,
    ) -> Option<Vector> {
        let (node, range) = match stream {
            VectorToVectorStream::Lhs => (&mut self.lhs, &mut self.lhs_range),
            VectorToVectorStream::Rhs => (&mut self.rhs, &mut self.rhs_range),
        };

        let vec_opt = node.next_vector(conn);
        if let Some(vec) = vec_opt {
            range.push_front(vec);

            if range.len() > 2 {
                range.pop_back();
            }
        }

        vec_opt
    }
}

impl ExecutorNode for VectorToVectorNode {
    fn value_type(&self) -> ValueType {
        ValueType::get_applied_value_type(self.lhs.value_type(), self.rhs.value_type())
    }

    fn return_type(&self) -> ReturnType {
        ReturnType::Vector
    }

    fn next_vector(&mut self, conn: &mut Connection) -> Option<Vector> {
        match self.op {
            BinaryOp::Arithmetic(_) => {
                // Initial case
                if self.lhs_range.is_empty()
                    && self.rhs_range.is_empty()
                    && self.value_opt.is_none()
                {
                    let lhs_vector_opt = self.next_child_vector(conn, VectorToVectorStream::Lhs);
                    let rhs_vector_opt = self.next_child_vector(conn, VectorToVectorStream::Rhs);

                    match (lhs_vector_opt, rhs_vector_opt) {
                        (Some(lhs), Some(rhs)) => {
                            let Vector {
                                timestamp: lhs_ts,
                                value: lhs_val,
                            } = lhs;
                            let Vector {
                                timestamp: rhs_ts,
                                value: rhs_val,
                            } = rhs;
                            match lhs_ts.cmp(&rhs_ts) {
                                std::cmp::Ordering::Less => {
                                    // Store this value
                                    self.value_opt = Some((rhs, VectorToVectorStream::Rhs));
                                }
                                std::cmp::Ordering::Greater => {
                                    // Store this value
                                    self.value_opt = Some((lhs, VectorToVectorStream::Lhs));
                                }
                                std::cmp::Ordering::Equal => {}
                            }

                            Some(Vector {
                                timestamp: Timestamp::min(lhs_ts, rhs_ts),
                                value: self.op.apply(
                                    lhs_val,
                                    self.lhs.value_type(),
                                    rhs_val,
                                    self.rhs.value_type(),
                                ), // These are the first values of the stream thus no interpolation is necessary
                            })
                        }
                        _ => None, // One of the streams is empty. Then do nothing.
                    }
                } else if self.value_opt.is_some() {
                    // There is a residual value from one stream
                    let (val, last_stream) = self.value_opt.unwrap();
                    let Vector {
                        timestamp: value_ts,
                        value: value_val,
                    } = val;
                    match last_stream {
                        VectorToVectorStream::Lhs => {
                            let rhs_vec_opt =
                                self.next_child_vector(conn, VectorToVectorStream::Rhs); // Fetch value from opposing stream
                            match rhs_vec_opt {
                                Some(Vector {
                                    timestamp: rhs_ts,
                                    value: rhs_val,
                                }) => {
                                    match rhs_ts.cmp(&value_ts) {
                                        // If the new vector is less than the residual we must interpolate the residual value relative to the new vector
                                        std::cmp::Ordering::Less => {
                                            let lhs_interpolated = self
                                                .calculate_value_with_linear_interpolation(
                                                    rhs_ts,
                                                    VectorToVectorStream::Lhs,
                                                );
                                            Some(Vector {
                                                timestamp: rhs_ts,
                                                value: self.op.apply(
                                                    lhs_interpolated,
                                                    ValueType::UInteger64, // TODO: Fix this
                                                    rhs_val,
                                                    self.rhs.value_type(),
                                                ),
                                            })
                                        }
                                        // If the new vector is greater than the residual we must store it, and then interpolate it relative to the residual
                                        std::cmp::Ordering::Greater => {
                                            self.value_opt = Some((
                                                rhs_vec_opt.unwrap(),
                                                VectorToVectorStream::Rhs,
                                            ));
                                            let rhs_interpolated = self
                                                .calculate_value_with_linear_interpolation(
                                                    value_ts,
                                                    VectorToVectorStream::Rhs,
                                                );
                                            Some(Vector {
                                                timestamp: value_ts,
                                                value: self.op.apply(
                                                    value_val,
                                                    self.value_type(),
                                                    rhs_interpolated,
                                                    ValueType::UInteger64,
                                                ),
                                            })
                                        }
                                        // If they are equal then no interpolation needed, we can discard residual value
                                        std::cmp::Ordering::Equal => {
                                            self.value_opt = None;
                                            Some(Vector {
                                                timestamp: value_ts,
                                                value: self.op.apply(
                                                    value_val,
                                                    self.lhs.value_type(),
                                                    rhs_val,
                                                    self.rhs.value_type(),
                                                ),
                                            })
                                        }
                                    }
                                }
                                _ => {
                                    let rhs_interpolated = self.rhs_range[0].value; // If there is no value from the RHS stream, then we must interpolate based on the last value
                                    self.value_opt = None;
                                    Some(Vector {
                                        timestamp: value_ts,
                                        value: self.op.apply(
                                            value_val,
                                            self.lhs.value_type(),
                                            rhs_interpolated,
                                            self.rhs.value_type(),
                                        ),
                                    })
                                }
                            }
                        }
                        VectorToVectorStream::Rhs => {
                            let lhs_vec_opt =
                                self.next_child_vector(conn, VectorToVectorStream::Lhs); // Fetch value from opposing stream
                            match lhs_vec_opt {
                                Some(Vector {
                                    timestamp: lhs_ts,
                                    value: lhs_val,
                                }) => match lhs_ts.cmp(&value_ts) {
                                    // If the new vector is less than the residual we must interpolate the residual value relative to the new vector
                                    std::cmp::Ordering::Less => {
                                        let rhs_interpolated = self
                                            .calculate_value_with_linear_interpolation(
                                                lhs_ts,
                                                VectorToVectorStream::Rhs,
                                            );
                                        Some(Vector {
                                            timestamp: lhs_ts,
                                            value: self.op.apply(
                                                lhs_val,
                                                self.lhs.value_type(),
                                                rhs_interpolated,
                                                ValueType::UInteger64,
                                            ), // TODO: Fix this
                                        })
                                    }
                                    // If the new vector is greater than the residual we must store it, and then interpolate it relative to the residual
                                    std::cmp::Ordering::Greater => {
                                        self.value_opt =
                                            Some((lhs_vec_opt.unwrap(), VectorToVectorStream::Lhs));
                                        let lhs_interpolated = self
                                            .calculate_value_with_linear_interpolation(
                                                value_ts,
                                                VectorToVectorStream::Lhs,
                                            );
                                        Some(Vector {
                                            timestamp: value_ts,
                                            value: self.op.apply(
                                                lhs_interpolated,
                                                ValueType::UInteger64,
                                                value_val,
                                                self.rhs.value_type(),
                                            ),
                                        })
                                    }
                                    // If they are equal then no interpolation needed, we can discard residual value
                                    std::cmp::Ordering::Equal => {
                                        self.value_opt = None;
                                        Some(Vector {
                                            timestamp: value_ts,
                                            value: self.op.apply(
                                                lhs_val,
                                                self.lhs.value_type(),
                                                value_val,
                                                self.rhs.value_type(),
                                            ),
                                        })
                                    }
                                },
                                _ => {
                                    let lhs_interpolated = self.lhs_range[0].value; // If there is no value from the LHS stream, then we must interpolate based on the last value
                                    self.value_opt = None;
                                    Some(Vector {
                                        timestamp: value_ts,
                                        value: self.op.apply(
                                            lhs_interpolated,
                                            ValueType::UInteger64, // TODO: Fix this
                                            value_val,
                                            self.rhs.value_type(),
                                        ),
                                    })
                                }
                            }
                        }
                    }
                } else {
                    // There is no residual value present
                    let lhs_vector_opt = self.next_child_vector(conn, VectorToVectorStream::Lhs);
                    let rhs_vector_opt = self.next_child_vector(conn, VectorToVectorStream::Rhs);

                    match (lhs_vector_opt, rhs_vector_opt) {
                        (
                            Some(Vector {
                                timestamp: lhs_ts,
                                value: lhs_val,
                            }),
                            Some(Vector {
                                timestamp: rhs_ts,
                                value: rhs_val,
                            }),
                        ) => match lhs_ts.cmp(&rhs_ts) {
                            // If the LHS is less than the RHS we must store the RHS and interpolate it relative to the LHS
                            std::cmp::Ordering::Less => {
                                self.value_opt =
                                    Some((rhs_vector_opt.unwrap(), VectorToVectorStream::Rhs));
                                let rhs_interpolated = self
                                    .calculate_value_with_linear_interpolation(
                                        lhs_ts,
                                        VectorToVectorStream::Rhs,
                                    );
                                Some(Vector {
                                    timestamp: lhs_ts,
                                    value: self.op.apply(
                                        lhs_val,
                                        self.lhs.value_type(),
                                        rhs_interpolated,
                                        ValueType::UInteger64, // TODO: Fix this
                                    ),
                                })
                            }
                            // If the LHS is greater than the RHS we must store the LHS and interpolate it relative to the RHS
                            std::cmp::Ordering::Greater => {
                                self.value_opt =
                                    Some((lhs_vector_opt.unwrap(), VectorToVectorStream::Lhs));
                                let lhs_interpolated = self
                                    .calculate_value_with_linear_interpolation(
                                        rhs_ts,
                                        VectorToVectorStream::Lhs,
                                    );
                                Some(Vector {
                                    timestamp: rhs_ts,
                                    value: self.op.apply(
                                        lhs_interpolated,
                                        ValueType::UInteger64,
                                        rhs_val,
                                        self.rhs.value_type(),
                                    ),
                                })
                            }
                            // If equal then no interpolation necessary
                            std::cmp::Ordering::Equal => Some(Vector {
                                timestamp: lhs_ts,
                                value: self.op.apply(
                                    lhs_val,
                                    self.lhs.value_type(),
                                    rhs_val,
                                    self.value_type(),
                                ),
                            }),
                        },
                        // If no RHS value then we interpolate it based on the last one
                        (
                            Some(Vector {
                                timestamp: lhs_ts,
                                value: lhs_val,
                            }),
                            None,
                        ) => {
                            let rhs_interpolated = self.rhs_range[0].value;
                            Some(Vector {
                                timestamp: lhs_ts,
                                value: self.op.apply(
                                    lhs_val,
                                    self.value_type(),
                                    rhs_interpolated,
                                    ValueType::UInteger64,
                                ), // TODO: Fix this for floats
                            })
                        }
                        // If no LHS value then we interpolate it based on the last one
                        (
                            None,
                            Some(Vector {
                                timestamp: rhs_ts,
                                value: rhs_val,
                            }),
                        ) => {
                            let lhs_interpolated = self.lhs_range[0].value;
                            Some(Vector {
                                timestamp: rhs_ts,
                                value: self.op.apply(
                                    lhs_interpolated,
                                    ValueType::UInteger64,
                                    rhs_val,
                                    self.rhs.value_type(),
                                ),
                            })
                        }
                        _ => None, // Both of the streams are empty.
                    }
                }
            }
            BinaryOp::Comparison(_) => {
                panic!("Comparison operator not allowed between scalar and scalar!")
            }
        }
    }
}

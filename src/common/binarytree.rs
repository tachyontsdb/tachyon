#[cfg(test)]
mod test{
    pub struct BinaryTreeNode {
        value: u16,
        left: Option<Box<Self>>,
        right: Option<Box<Self>>,
    }

    impl BinaryTreeNode {
        fn print(&self) {
            println!("{}", self.value);

            match &self.left {
                Some(left) => {
                    left.as_ref().print();
                }
                _ => {}
            }

            match &self.right {
                Some(right) => {
                    right.as_ref().print();
                }
                _ => {}
            }
        }

        fn insert(&mut self, node: Self) {
            if let Some(existing) = if node.value > self.value { &mut self.left } else { &mut self.right } {
                existing.insert(node);
            } else {
                if node.value > self.value {
                    self.left = Some(Box::new(node));
                } else {
                    self.right = Some(Box::new(node));
                }
            }
        }
    }

    pub struct BinaryTree {
        root: BinaryTreeNode,
    }

    impl BinaryTree {
        fn add_val(&mut self, val: u16) {
            let new_node = BinaryTreeNode {
                value: val,
                left: None,
                right: None
            };

            self.root.insert(new_node);
        }
    }

    fn test_binary_tree() {
        let mut tree = BinaryTree{
            root: BinaryTreeNode{
                value: 4,
                left: None,
                right: None
            }
        };

        tree.add_val(5);

        tree.root.print()
    }
}

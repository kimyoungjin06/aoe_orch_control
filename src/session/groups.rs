//! Group tree management

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::Instance;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Group {
    pub name: String,
    pub path: String,
    #[serde(default)]
    pub collapsed: bool,
    #[serde(skip)]
    pub children: Vec<Group>,
}

impl Group {
    pub fn new(name: &str, path: &str) -> Self {
        Self {
            name: name.to_string(),
            path: path.to_string(),
            collapsed: false,
            children: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct GroupTree {
    roots: Vec<Group>,
    groups_by_path: HashMap<String, Group>,
}

impl GroupTree {
    pub fn new_with_groups(instances: &[Instance], existing_groups: &[Group]) -> Self {
        let mut tree = Self {
            roots: Vec::new(),
            groups_by_path: HashMap::new(),
        };

        // Add existing groups
        for group in existing_groups {
            tree.groups_by_path
                .insert(group.path.clone(), group.clone());
        }

        // Ensure all instance groups exist
        for inst in instances {
            if !inst.group_path.is_empty() {
                tree.ensure_group_exists(&inst.group_path);
            }
        }

        // Build tree structure
        tree.rebuild_tree();

        tree
    }

    fn ensure_group_exists(&mut self, path: &str) {
        if self.groups_by_path.contains_key(path) {
            return;
        }

        // Create all parent groups
        let parts: Vec<&str> = path.split('/').collect();
        let mut current_path = String::new();

        for (i, part) in parts.iter().enumerate() {
            if i > 0 {
                current_path.push('/');
            }
            current_path.push_str(part);

            if !self.groups_by_path.contains_key(&current_path) {
                let group = Group::new(part, &current_path);
                self.groups_by_path.insert(current_path.clone(), group);
            }
        }
    }

    fn rebuild_tree(&mut self) {
        self.roots.clear();

        // Find root groups (no '/' in path)
        let mut root_groups: Vec<Group> = self
            .groups_by_path
            .values()
            .filter(|g| !g.path.contains('/'))
            .cloned()
            .collect();

        root_groups.sort_by(|a, b| a.name.cmp(&b.name));

        // Build children recursively
        for root in &mut root_groups {
            self.build_children(root);
        }

        self.roots = root_groups;
    }

    fn build_children(&self, parent: &mut Group) {
        let prefix = format!("{}/", parent.path);

        let mut children: Vec<Group> = self
            .groups_by_path
            .values()
            .filter(|g| g.path.starts_with(&prefix) && !g.path[prefix.len()..].contains('/'))
            .cloned()
            .collect();

        children.sort_by(|a, b| a.name.cmp(&b.name));

        for child in &mut children {
            self.build_children(child);
        }

        parent.children = children;
    }

    pub fn create_group(&mut self, path: &str) {
        self.ensure_group_exists(path);
        self.rebuild_tree();
    }

    pub fn delete_group(&mut self, path: &str) {
        // Remove group and all children
        let prefix = format!("{}/", path);
        let to_remove: Vec<String> = self
            .groups_by_path
            .keys()
            .filter(|p| *p == path || p.starts_with(&prefix))
            .cloned()
            .collect();

        for p in to_remove {
            self.groups_by_path.remove(&p);
        }

        self.rebuild_tree();
    }

    pub fn group_exists(&self, path: &str) -> bool {
        self.groups_by_path.contains_key(path)
    }

    pub fn get_all_groups(&self) -> Vec<Group> {
        let mut groups: Vec<Group> = self.groups_by_path.values().cloned().collect();
        groups.sort_by(|a, b| a.path.cmp(&b.path));
        groups
    }

    pub fn get_roots(&self) -> &[Group] {
        &self.roots
    }

    pub fn toggle_collapsed(&mut self, path: &str) {
        if let Some(group) = self.groups_by_path.get_mut(path) {
            group.collapsed = !group.collapsed;
            self.rebuild_tree();
        }
    }
}

/// Item represents either a group or an instance in the flattened tree view
#[derive(Debug, Clone)]
pub enum Item {
    Group {
        path: String,
        name: String,
        depth: usize,
        collapsed: bool,
        session_count: usize,
    },
    Session {
        id: String,
        depth: usize,
    },
}

impl Item {
    pub fn depth(&self) -> usize {
        match self {
            Item::Group { depth, .. } => *depth,
            Item::Session { depth, .. } => *depth,
        }
    }
}

pub fn flatten_tree(group_tree: &GroupTree, instances: &[Instance]) -> Vec<Item> {
    let mut items = Vec::new();

    // Add ungrouped sessions first
    let ungrouped: Vec<&Instance> = instances
        .iter()
        .filter(|i| i.group_path.is_empty())
        .collect();

    for inst in ungrouped {
        items.push(Item::Session {
            id: inst.id.clone(),
            depth: 0,
        });
    }

    // Add groups and their sessions
    for root in group_tree.get_roots() {
        flatten_group(root, instances, &mut items, 0, group_tree);
    }

    items
}

fn flatten_group(
    group: &Group,
    instances: &[Instance],
    items: &mut Vec<Item>,
    depth: usize,
    tree: &GroupTree,
) {
    let session_count = count_sessions_in_group(&group.path, instances, tree);

    items.push(Item::Group {
        path: group.path.clone(),
        name: group.name.clone(),
        depth,
        collapsed: group.collapsed,
        session_count,
    });

    if group.collapsed {
        return;
    }

    // Add sessions in this group (direct children only)
    let group_sessions: Vec<&Instance> = instances
        .iter()
        .filter(|i| i.group_path == group.path)
        .collect();

    for inst in group_sessions {
        items.push(Item::Session {
            id: inst.id.clone(),
            depth: depth + 1,
        });
    }

    // Recursively add child groups
    for child in &group.children {
        flatten_group(child, instances, items, depth + 1, tree);
    }
}

fn count_sessions_in_group(path: &str, instances: &[Instance], _tree: &GroupTree) -> usize {
    let prefix = format!("{}/", path);
    instances
        .iter()
        .filter(|i| i.group_path == path || i.group_path.starts_with(&prefix))
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_tree_creation() {
        let mut inst1 = Instance::new("test1", "/tmp/1");
        inst1.group_path = "work".to_string();
        let mut inst2 = Instance::new("test2", "/tmp/2");
        inst2.group_path = "work/frontend".to_string();
        let mut inst3 = Instance::new("test3", "/tmp/3");
        inst3.group_path = "personal".to_string();

        let instances = vec![inst1, inst2, inst3];
        let tree = GroupTree::new_with_groups(&instances, &[]);

        assert!(tree.group_exists("work"));
        assert!(tree.group_exists("work/frontend"));
        assert!(tree.group_exists("personal"));
        assert!(!tree.group_exists("nonexistent"));
    }

    #[test]
    fn test_flatten_tree() {
        let ungrouped = Instance::new("ungrouped", "/tmp/u");
        let mut inst1 = Instance::new("test1", "/tmp/1");
        inst1.group_path = "work".to_string();
        let mut inst2 = Instance::new("test2", "/tmp/2");
        inst2.group_path = "work".to_string();

        let instances = vec![ungrouped, inst1, inst2];
        let tree = GroupTree::new_with_groups(&instances, &[]);
        let items = flatten_tree(&tree, &instances);

        assert!(!items.is_empty());

        // First item should be ungrouped session
        matches!(items[0], Item::Session { .. });
    }
}

package lib

// NodeAccess tells whether the permission of the tree applies to the node itself, to its children,
// and to the nodes two or more levels below it.
type NodeAccess struct {
	Object               bool `json:"object"`
	ImmediateDescendants bool `json:"immediate_descendants"`
	OtherDescendants     bool `json:"other_descendants"`
}

// ACLTree is a node of the ACL tree of a user; the root of the tree is the Cypress root. A node without Access
// gets ImmediateDescendants of its nearest ancestor with Access if it is a child of that ancestor, and
// OtherDescendants otherwise.
type ACLTree struct {
	Access   *NodeAccess         `json:"access,omitempty"`
	Children map[string]*ACLTree `json:"children,omitempty"`
}

type UserACLTreeResponse struct {
	Path          string   `json:"path"`
	ACLSnapshotID string   `json:"acl_snapshot_id"`
	ACLTree       *ACLTree `json:"acl_tree"`
}

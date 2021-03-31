#include <stdlib.h>
#include <stdio.h>

#include "bst.h"

// Local functions
static int place_bst_node( struct BST_Node *parent, struct BST_Node *node );
static struct BST_Node *make_bst_node( int key, void *data );

// Root's pointer is passed because root can get modified for the first node
int bst_add_node( struct BST_Node **root, int key, void *data )
{
	struct BST_Node *newnode = NULL;
	struct BST_Node *parent = NULL;
	struct BST_Node *retnode = NULL;
	int status = 0;

	newnode = make_bst_node( key, data);
	if( *root == NULL ){
		*root = newnode;
		status = BST_SUCCESS;
	}
	else{
		status = place_bst_node( *root, newnode );
	}
	return status;
}

// Helper function to find a minimum node in the given binary tree with root node.
struct BST_Node * minValueNode(struct BST_Node* node) 
{ 
    struct BST_Node* current = node; 
  
    // Going the leftmost part of the tree
    while (current && current->left_child != NULL) 
        current = current->left_child; 
  
    return current; 
} 

// Helper to delete the node in the given binary tree with root node as parent
struct BST_Node* deleteNode(struct BST_Node* parent, int key) 
{ 
    // base case when parent is null
    if (parent == NULL) 
	{
		return parent;
	} 
    // If the key to be deleted is smaller than the parent's key , then it lies in left subtree 
    if (key < parent->key)
	{ 
        parent->left_child = deleteNode(parent->left_child, key); 
	}
    // If the key to be deleted is greater than the parent's key, then it lies in right subtree 
    else if (key > parent->key) 
	{
        parent->right_child = deleteNode(parent->right_child, key); 
	}
    // if key value is same as parent's key
    else
    { 
        // node with only one child or no child 
        if (parent->left_child == NULL) 
        { 
            struct BST_Node *temp = parent->right_child; 
            free(parent); 
            return temp; 
        } 
        else if (parent->right_child == NULL) 
        { 
            struct BST_Node *temp = parent->left_child; 
            free(parent); 
            return temp; 
        } 
  
        // node with two children. In this case we get inorder successor of the node.
        struct BST_Node* temp = minValueNode(parent->right_child); 
  
        // Copy the inorder successor's content to this node 
        parent->key = temp->key; 
  
        // Delete the inorder successor 
        parent->right_child = deleteNode(parent->right_child, temp->key); 
    } 
    return parent; 
} 


int bst_del_node( struct BST_Node **root, int key )
{
	int status=BST_SUCCESS;
	// Internal function to delete the node with given key in BST
	struct BST_Node* newroot=deleteNode(*root,key);
	// root is updated to new root obtained after deleeing the node
	*root=newroot;
	status= BST_SUCCESS;
	return status;
}

struct BST_Node *bst_search( struct BST_Node *root, int key )
{
	struct BST_Node *retval = NULL;

	if( root == NULL ){
		return NULL;
	}
	else if( root->key == key )
		return root;
	else if( key < root->key )
		return bst_search( root->left_child, key );
	else if( key > root->key )
		return bst_search( root->right_child, key );
}
void bst_print( struct BST_Node *root )
{
	if( root == NULL )
		return;
	else{
		printf("%d ", root->key);
		bst_print( root->left_child );
		bst_print( root->right_child );
	}
}

void bst_free( struct BST_Node *root )
{
	if( root == NULL )
		return;
	else{
		bst_free( root->left_child );
		bst_free( root->right_child );
		free(root);
	}
}

void bst_destroy( struct BST_Node *root )
{
	if( root == NULL )
		return;
	else{
		bst_free( root->left_child );
		bst_free( root->right_child );
		free(root->data);
		free(root);
	}
}

static int place_bst_node( struct BST_Node *parent, struct BST_Node *node )
{
	int retstatus;

	if( parent == NULL ){
		return BST_NULL;
	}
	else if( node->key == parent->key ){
		return BST_DUP_KEY;
	}
	else if( node->key < parent->key ){
		if( parent->left_child == NULL ){
			parent->left_child = node;
			return BST_SUCCESS;
		}
		else{
			return place_bst_node( parent->left_child, node );
		}
	}
	else if( node->key > parent->key ){
		if( parent->right_child == NULL ){
			parent->right_child = node;
			return BST_SUCCESS;
		}
		else{
			return place_bst_node( parent->right_child, node );
		}
	}
}

static struct BST_Node *make_bst_node( int key, void *data )
{
	struct BST_Node *newnode;
	newnode = (struct BST_Node *) malloc(sizeof(struct BST_Node));
	newnode->key = key;
	newnode->data = data;
	newnode->left_child = NULL;
	newnode->right_child = NULL;

	return newnode;
}
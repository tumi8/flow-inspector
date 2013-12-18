#ifndef __sds_lpm_tree_
#define __sds_lpm_tree_

#include <stdint.h>

struct lpm_tree;

/* lpm_destroy:
 * Frees the entire tree structure.
 */
void lpm_destroy(struct lpm_tree* tree);

/* lpm_init:
 * Constructs a fresh tree ready for use by the other functions.
 */
struct lpm_tree* lpm_init();

/* lpm_insert:
 * Insert a new prefix ('ip_string' and 'netmask') into the tree. If
 * 'ip_string' does not contain a valid IPv4 address, or the netmask
 * is clearly invalid, the tree is not modified and the function.
 * The network_owner is another id that can point to the identity 
 * that owns the subnet.
 * returns 0. Successful insertion returns 1.
 */
int lpm_insert(struct lpm_tree* tree, const char* ip_string, uint32_t netmask, uint32_t network_owner);

/* lpm_lookup_string:
 * Perform a lookup. Given a string 'ip_string' convert to the
 * best-matching prefix if the string is a valid IPv4 address
 * (according to inet_pton), and store it in 'output' and return 1. If
 * no match is found, store the string "NF" in 'output' and return
 * 1. If 'ip_string' is not a valid IPv4 address, return 0, and
 * 'output' is not modified.
 */
int lpm_lookup_string(struct lpm_tree* tree, char* ip_string, char* output, uint32_t* network_owner);

/* lpm_lookup_string:
 * Perform a lookup. Given a uint32_t  'ip_address' convert to the
 * best-matching prefix and store it in 'output' and return 1. If
 * no match is found, store the string "NF" in 'output' and return
 * 1. If 'ip_string' is not a valid IPv4 address, return 0, and
 * 'output' is not modified.
 */
int lpm_lookup(struct lpm_tree* tree, uint32_t address, char* output, uint32_t* network_owner);


/* lpm_debug_print:
 * Traverses the tree and prints out node status, starting from the root.
 */
void lpm_debug_print(struct lpm_tree* tree);



/* Gubbins. */

/* 
 * The tree features internal nodes (which hold no data; only point to
 * children) and data nodes (which _do_ hold data). I hold a 'type'
 *  field at the front of both of these types to differentiate.
 */
#define DAT_NODE 1
#define INT_NODE 0

#define MAX_BITS 32



struct lpm_tree {
	struct internal_node* head;
};

struct internal_node {
	uint8_t type;

	struct internal_node* l;
	struct internal_node* r;
};

struct data_node {
	uint8_t type;

	struct internal_node* l;
	struct internal_node* r;

	uint32_t prefix;
	uint8_t netmask;

	uint32_t network_owner;
};


#endif

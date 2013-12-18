/**
 * For licensing, see ./LICENSE
 * 
 * Currently, this code allows insertions and lookups, but not
 * deletions. The data structure is a simple trie, which provides an
 * O(log n) bound on insertions and lookups. Deletions wouldn't be
 * tricky to add.
 * 
 * When performing a lookup, bit comparisons decide left/right
 * traversal from the head of the tree, and the prefix length defines
 * a maximum depth when inserting. The lookup function will traverse
 * the tree until it determines that no more specific match than the
 * best already found is possible. The code will replace all valid IP
 * addresses (according to inet_pton()) with the matching prefix, or
 * "NF" if there was no match. It will not attempt to match tokens
 * that are not prefixes, but will print them out in the output.
 * 
 * The code reads the lines to convert from standard input; it reads a
 * list of prefixes from a file, specified by the "-f" parameter. The
 * prefix file should contain one prefix per line, with the prefix and
 * the netmask separated by a space. All output is sent to standard
 * output.
 */

#include "tree.h"

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>

#include <math.h>
#include <string.h>
#include <fcntl.h>

#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/stat.h>

int debug = 0;

/*#define DEBUG(fmt, ...) do { if (debug) fprintf(stderr, fmt, __VA_ARGS__); } while (0)*/
#undef DEBUG
#define DEBUG(...) do { if (debug) fprintf(stdout, __VA_ARGS__); } while (0)

/*
 * This creates an data (value-holding) node which points nowhere.
 */
struct data_node* create_data_node(uint32_t prefix, uint8_t netmask, uint32_t network_owner)
{
	struct data_node* node = (struct data_node*)malloc(sizeof(struct data_node));
	DEBUG("## Created data node %p for %d\n", (void*)node, prefix);
	node->type  = DAT_NODE;
	node->prefix = prefix;
	node->netmask  = netmask;
	node->network_owner = network_owner;
	node->l = NULL;
	node->r = NULL;

	return node;
}

/*
 * This creates an internal node that points nowhere.
 */
struct internal_node* create_internal_node()
{
	struct internal_node* tmp = (struct internal_node*)malloc(sizeof(struct internal_node));
	DEBUG("## Created internal node %p\n", (void*)tmp);
	tmp->type = INT_NODE;
	tmp->l = NULL;
	tmp->r = NULL;

	return tmp;
}

/*
 * This function used internally; see lpm_insert().
 */
void insert(uint32_t prefix, uint32_t nm, struct internal_node* n, uint32_t network_owner)
{
	uint8_t b = MAX_BITS;
	uint8_t depth = 0;
	struct internal_node* parent;
	struct internal_node* next = n;

	/* First, find the correct location for the prefix. Burrow down to
	   the correct depth, potentially creating internal nodes as I
	   go. */
	do {
		n = next;
		b--;
		depth++;

		parent = (struct internal_node*)n;
		uint32_t v_bit = prefix & ((uint32_t)pow(2, b));

		/* Determine which direction to descend. */
		if (v_bit) {
			if (n->r == NULL) {
				n->r = create_internal_node();
			}
			next = n->r;
		}
		else {
			if (n->l == NULL) {
				n->l = create_internal_node();
			}
			next = n->l;
		}
	} while (depth < nm);

	if (next == NULL) {
		/* The easy case. */
		struct data_node* node = create_data_node(prefix, nm, network_owner);
		uint32_t v_bit = prefix & ((uint32_t)pow(2, b));
		if (v_bit) {
			parent->r = (struct internal_node*)node;
		}
		else {
			parent->l = (struct internal_node*)node;
		}
	}
	else if (next->type == INT_NODE) {
		/* In this case, we've descended as far as we can. Attach the
		   prefix here. */
		uint32_t v_bit = prefix & ((uint32_t)pow(2, b));
		struct data_node* newnode = create_data_node(prefix, nm, network_owner);
		newnode->l = next->l;
		newnode->r = next->r;

		if (v_bit) {
			n->r = (struct internal_node*)newnode;
		}
		else {
			n->l = (struct internal_node*)newnode;
		}

		DEBUG("## Freeing %p\n", (void*)n);
		free(next);
	}
}

/* destroy:
 * Recursively destroys nodes.
 */
void destroy(struct internal_node* node)
{
	if (node == NULL) return;

	if (node->l != NULL) {
		destroy(node->l);
	}
	if (node->r != NULL) {
		destroy(node->r);
	}

	free(node);
}


/* lpm_destroy:
 * Frees the entire tree structure.
 */
void lpm_destroy(struct lpm_tree* tree)
{
	if (tree == NULL) return;
	destroy(tree->head);
	free(tree);
}


/* lpm_init:
 * Constructs a fresh tree ready for use by the other functions.
 */
struct lpm_tree* lpm_init()
{
	/* Build empty internal node, and attach it to new tree. */
	struct internal_node* node = create_internal_node();

	struct lpm_tree* tree = (struct lpm_tree*)malloc(sizeof(struct lpm_tree));
	DEBUG("## Created tree %p\n", (void*)tree);
	tree->head = node;

	return tree;
}

/* lpm_insert:
 * Insert a new prefix ('ip_string' and 'netmask') into the tree. If
 * 'ip_string' does not contain a valid IPv4 address, or the netmask
 * is clearly invalid, the tree is not modified and the function
 * returns 0. Successful insertion returns 1.
 */
int lpm_insert(struct lpm_tree* tree, const char* ip_string, uint32_t netmask, uint32_t network_owner)
{
	uint32_t ip;
	if (!inet_pton(AF_INET, ip_string, &ip) || netmask > 32) {
		return 0;
	}
	ip = ntohl(ip);

	DEBUG(">> Inserting %s/%d===================================================\n", ip_string, netmask);

	insert(ip, netmask, tree->head, network_owner);
	DEBUG(">> Done inserting %s/%d =============================================\n", ip_string, netmask);

	return 1;
}

/*
 * Internal function; called by lpm_lookup()
 */
void lookup(uint32_t address, char* output, struct internal_node* n, uint32_t* network_owner)
{
	uint32_t b = MAX_BITS;
	struct internal_node* parent;
	struct internal_node* next = n;

	uint32_t best_prefix = 0;
	uint8_t  best_netmask = 0;

	char addr_string[16];
	uint32_t tmp = htonl(address);
	inet_ntop(AF_INET, &tmp, addr_string, 16);

	do {
		n = next;
		b--;

		parent = (struct internal_node*)n;
		uint32_t v_bit = address & ((uint32_t)pow(2, b));

		/* If we've found an internal node, determine which
		   direction to descend. */
		if (v_bit) {
			next = n->r;
		}
		else {
			next = n->l;
		}

		if (n->type == DAT_NODE) {
			struct data_node* node = (struct data_node*)n;

			char prefix[16];
			tmp = htonl(node->prefix);
			inet_ntop(AF_INET, &tmp, prefix, 16);

			uint32_t mask = 0xFFFFFFFF;

			mask = mask - ((uint32_t)pow(2, 32 - node->netmask) - 1);

			if ((address & mask) == node->prefix) {
				best_prefix = node->prefix;
				best_netmask = node->netmask;
				*network_owner = node->network_owner;
			}
			else {
				break;
			}
		}
	} while (next != NULL);

	if (!best_prefix) {
		sprintf(output, "NF");
		*network_owner = 0;
	}
	else {
		char prefix[16];
		tmp = htonl(best_prefix);
		inet_ntop(AF_INET, &tmp, prefix, 16);

		sprintf(output, "%s/%d", prefix, best_netmask);
	}
}

/* lpm_lookup:
 * Perform a lookup. Given a string 'ip_string' convert to the
 * best-matching prefix if the string is a valid IPv4 address
 * (according to inet_pton), and store it in 'output' and return 1. If
 * no match is found, store the string "NF" in 'output' and return
 * 1. If 'ip_string' is not a valid IPv4 address, return 0, and
 * 'output' is not modified.
 */
int lpm_lookup_string(struct lpm_tree* tree, char* ip_string, char* output, uint32_t* network_owner)
{
	uint32_t tmp;
	int rt;
	rt = inet_pton(AF_INET, ip_string, &tmp);
	if (!rt) {
		return 0;
	}
	uint32_t ip = ntohl(tmp);
	lookup(ip, output, tree->head, network_owner);
	return 1;
}

int lpm_lookup(struct lpm_tree* tree, uint32_t address, char* output, uint32_t* network_owner)
{
	lookup(address, output, tree->head, network_owner);
	return 1;
}




/* debug_print:
 * Prints out the current node's parent, the current node's value (if
 * it has one), and recurses down to the left then right children. The
 * 'left' parameter should indicate the direction of the hop to the
 * current node (1 if the left child was used, 0 if the right child
 * was used; -1 is used to indicate the root of the tree.)
 */
void debug_print(struct internal_node* parent, int left, int depth, struct internal_node* n)
{
	printf("parent:%p", (void*)parent);
	if (left == 1) {
		printf("->L");
	}
	else if (left == 0) {
		printf("->R");
	}
	else {
		printf("---");
	}

	if (n == NULL) {
		printf(" Reached a null bottom %p\n", (void*)n);
		return;
	}
	else if (n->type == INT_NODE) {
		printf(" Internal node %p.\n", (void*)n);
	}
	else {
		struct data_node* node = (struct data_node*)n;

		uint32_t tmp = htonl(node->prefix);
		char output[16];
		inet_ntop(AF_INET, &tmp, output, 16);

		printf(" External node: %p, %s/%d\n", (void*)n, output, node->netmask);
	}

	debug_print(n, 1, depth+1, n->l);
	debug_print(n, 0, depth+1, n->r);
}

/* lpm_debug_print:
 * Traverses the tree and prints out node status, starting from the root.
 */
void lpm_debug_print(struct lpm_tree* tree)
{
	if (debug) {
		debug_print((struct internal_node*)tree, -1, 0, tree->head);
	}
}

/*
 * Educate the user via standard error.
 */
void print_usage(char* name)
{
	fprintf(stderr, "%s will replace any IPv4 address on standard input with the\n", name);
	fprintf(stderr, "\tmatching prefix in prefix_file, or 'NF'.\n");
	fprintf(stderr, "Usage: %s -f prefix_file [-d]\n\n", name);
}


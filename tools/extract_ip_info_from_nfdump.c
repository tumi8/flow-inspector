#include <tsdb_api.h>
#include "longest-prefix/tree.h"

#include <unistd.h>
#include <string.h>
#include <search.h>
#include <sys/wait.h>
#include <arpa/inet.h>

#define LINE_SIZE 100000
#define MAX_NETWORKS 50000

typedef struct network_stats {
	uint32_t id;

	uint64_t in_pkts;
	uint64_t in_tcp_pkts;
	uint64_t in_udp_pkts;
	uint64_t in_icmp_pkts;
	uint64_t in_other_pkts;
	
	uint64_t in_bytes;
	uint64_t in_tcp_bytes;
	uint64_t in_udp_bytes;
	uint64_t in_icmp_bytes;
	uint64_t in_other_bytes;

	uint64_t out_pkts;
	uint64_t out_tcp_pkts;
	uint64_t out_udp_pkts;
	uint64_t out_icmp_pkts;
	uint64_t out_other_pkts;

	uint64_t out_bytes;
	uint64_t out_tcp_bytes;
	uint64_t out_udp_bytes;
	uint64_t out_icmp_bytes;
	uint64_t out_other_bytes;
} network_stats_t;

void* network_tree = NULL;
tsdb_handler db;
int counter = 0;

/***********************************  Methods *************************/


static int compare_networks(const void *a, const void *b)
{
	const network_stats_t* na = (const network_stats_t*)a;
	const network_stats_t* nb = (const network_stats_t*)b;

	return (na->id > nb->id) - (na->id < nb->id);
}

static struct lpm_tree* read_prefix_file(const char* prefix_file)
{
	struct lpm_tree* tree = lpm_init(); 
	if (tree == NULL){
		fprintf(stderr, "ERROR: Could not allocate lpm_tree()\n");
		return NULL;
	}
	FILE* f = fopen(prefix_file, "r");
	if (f == NULL) {
		fprintf(stderr, "ERROR: coult not open prefix file %s\n",
			prefix_file);
		goto out1;
	}

	char line[LINE_SIZE];
	char tmp_line[LINE_SIZE];
	char delimiter[] = " ";
	while (fgets(line, LINE_SIZE, f)) {
		// tokenize the subnet list. The expected format is
		// <NET_ID> <subnet1> <subnet2> ...

		// remove trailing \n
		if (line[strlen(line) - 1] == '\n') {
			line[strlen(line) - 1] = 0;
		}

		// duplicate string for parsing. we need the original
		// line for error messages
		strcpy(tmp_line, line);

		char* ptr = strtok(tmp_line, delimiter);
		uint32_t id;
		int is_first = 1;
		while (ptr) {
			if (is_first) {
				id = atoi(ptr);
				is_first = 0;
			} else {
				// extract the ip and the subnet string
				// expected format: ip/subnetmask
				char* slash_pos = strchr(ptr, '/');
				char* netmask;
				if (slash_pos == NULL) {
					fprintf(stderr, "ERROR parsing line "
					"\"%s\". Could not find / in token "
					"%s\n", line, ptr);
					// there is an error in the file format
					// we do the only thing we can and drop
					// out
					goto out2;
				}
				if (strlen(slash_pos) == 1) {
					// there is no subnetmask behind the
					// slash. drop out
					fprintf(stderr, "ERROR parsing line "
					"\"%s\". Found / in token %s but no "
					"following subnet mask!\n", line, ptr);
					goto out2;
				}
				*slash_pos = 0;
				netmask = slash_pos + 1;
				if (0 == lpm_insert(tree, ptr, atoi(netmask),
							id))
				{
					fprintf(stderr, "ERROR inserting %s "
					"into the subnet tree!\n", ptr);
					goto out2;
				}
			}
			ptr = strtok(NULL, delimiter);
		}
	}

	// We are done without errors. 
	goto out1;
out2:
	lpm_destroy(tree);
	tree = NULL;
out1:
	fclose(f);
out:
	return tree;
}

int consume_flow(const char* line, struct lpm_tree* tree)
{
        // create a copy of the line for strtok
        char tmp_line[LINE_SIZE];
        strcpy(tmp_line, line);
        char delimiter[] = "|";
        char* ptr;

	uint32_t sip;
	uint32_t dip;
	uint32_t packets;
	uint32_t bytes;
	uint16_t dport;
	uint16_t sport;
	uint8_t proto;
        int ret;

	// the input line is a | separated list of mulitiple fields (counting field numbers starts at 0)
	// field1 = first-seen second
	// field2 = first-seen milliseconds
	// field3 = last-seen seconds
	// field4 = last-seen milliseconds
	// field5 = protocol
	// field9 = source ip
	// field10 = source port
	// field14 = destination ip
	// field15 = destination port
	// field20 = flags
	// field22 = packet
	// field23 = bytes
        // We parse a token. If we have a key, we construct the final
        // key from key_prefix_key (which results in IP_interface_KEY1, IP_interface_KEY2, ...)
        ptr = strtok(tmp_line, delimiter);
	uint8_t field_ctr = 0;
        while (ptr) {
		switch (field_ctr) {
		case 1: // first-seen second
			break;
		case 2: // first seen milliseconds
			break;
		case 3: // last seen seconds
			break;
		case 4: // last seen milliseconds
			break;
		case 5: // protocol
			proto = (uint8_t)atoi(ptr);
			break;
		case 9: // source ip
			sip = (uint32_t)atoi(ptr);
			break;
		case 10: // source port
			sport = (uint16_t)atoi(ptr);
			break;
		case 14: // destination ip
			dip = (uint32_t)atoi(ptr);
			break;
		case 15: // destination port
			dport = (uint32_t)atoi(ptr);
			break;
		case 20: // flags
			break;
		case 22: // packets
			packets = (uint32_t)atoi(ptr);
			break;
		case 23: // bytes
			bytes = (uint32_t)atoi(ptr);
			break;
		default:
			break;
		}
		field_ctr++;
                ptr = strtok(NULL, delimiter);
        }

	uint32_t network_owner_source, network_owner_dest;
	char network_source[LINE_SIZE];
	char network_dest[LINE_SIZE];
	lpm_lookup(tree, sip, network_source, &network_owner_source);
	lpm_lookup(tree, dip, network_dest, &network_owner_dest);

	network_stats_t *s_net_search = (network_stats_t*)malloc(sizeof(network_stats_t));
	if (NULL == s_net_search) {
		perror("ERROR: Could not allocate s_net_search");
		exit(1);
	}
	bzero(s_net_search, sizeof(network_stats_t));
	network_stats_t *d_net_search = (network_stats_t*)malloc(sizeof(network_stats_t));
	if (NULL == d_net_search) {
		perror("ERROR: Could not allocate d_net_search");
		exit(1);
	}
	bzero(d_net_search, sizeof(network_stats_t));
	s_net_search->id = network_owner_source;
	d_net_search->id = network_owner_dest;

	network_stats_t* s_net = *(network_stats_t**)tsearch(s_net_search, &network_tree, compare_networks);
	
	if (s_net != s_net_search) {
		// element is already in tree. delete the search element
		free(s_net_search);
	}
	network_stats_t* d_net = *(network_stats_t**)tsearch(d_net_search, &network_tree, compare_networks);
	if (d_net != d_net_search) {
		// element is already in tree. delete the search element
		free(d_net_search);
	}

	s_net->out_pkts  += packets;
	s_net->out_bytes += bytes;

	d_net->in_pkts  += packets;
	d_net->in_bytes += bytes;

	switch (proto) {
	case 1: // ICMP
		s_net->out_icmp_pkts  += packets;
		s_net->out_icmp_bytes += bytes;

		d_net->in_icmp_pkts  += packets;
		d_net->in_icmp_bytes += bytes;
		break;
	case 6: // TCP
		s_net->out_tcp_pkts  += packets;
		s_net->out_tcp_bytes += bytes;

		d_net->in_tcp_pkts  += packets;
		d_net->in_tcp_bytes += bytes;
		break;
	case 17: // UDP 
		s_net->out_udp_pkts  += packets;
		s_net->out_udp_bytes += bytes;

		d_net->in_udp_pkts  += packets;
		d_net->in_udp_bytes += bytes;
		break;
	default:
		s_net->out_other_pkts  += packets;
		s_net->out_other_bytes += bytes;

		d_net->in_other_pkts  += packets;
		d_net->in_other_bytes += bytes;
		break;
	}

	return 0;
}


int read_nfdump_files(const char* profile_dir, const char* capfile, struct lpm_tree* tree)
			
{
	int pipefd[2];
	pid_t nfdump_pid;
	int w;
	
	pipe(pipefd);
	
	// let nfdump parse the nfdump files
	if ((nfdump_pid = fork()) == 0) {
		// attach stdout to the left side of pipe
		// and inherit stdin and stdout from parent
		dup2(pipefd[1],STDOUT_FILENO);
		close(pipefd[0]);              // not using the right side
		execlp("nfdump", "nfdump","-M", profile_dir, "-r", capfile, "-o", "pipe", NULL);
		perror("exec of nfdump failed");
		exit(-2);
	} 

	close(pipefd[1]);
	FILE* pipe_input = fdopen(pipefd[0], "r");
	if (!pipe_input) {
		perror("ERROR: Could not open pipe as stream!");
		wait(&w);
		exit(1);;
	}
	char line[LINE_SIZE];
	while (fgets(line, LINE_SIZE, pipe_input)) {
		if (line[strlen(line) - 1] == '\n') {
			line[strlen(line) - 1] = 0;
		}
		consume_flow(line, tree);
	}

	fclose(pipe_input);
	wait(&w);
	return 0;
}

void store_records_in_db(const void *nodep, VISIT value, int level)
{
	char key[LINE_SIZE];
	if (value == leaf) {
		network_stats_t* n = *(network_stats_t**)nodep;
		//printf("Value: %u %llu %llu %llu %llu\n", n->id, n->in_pkts,
		//	n->in_bytes, n->out_pkts, n->out_bytes);	
		snprintf(key, LINE_SIZE, "%u_in_pkts", n->id);
		tsdb_set(&db, key, &n->in_pkts);
		snprintf(key, LINE_SIZE, "%u_in_tcp_pkts", n->id);
		tsdb_set(&db, key, &n->in_tcp_pkts);
		snprintf(key, LINE_SIZE, "%u_in_udp_pkts", n->id);
		tsdb_set(&db, key, &n->in_udp_pkts);
		snprintf(key, LINE_SIZE, "%u_in_icmp_pkts", n->id);
		tsdb_set(&db, key, &n->in_icmp_pkts);
		snprintf(key, LINE_SIZE, "%u_in_other_pkts", n->id);
		tsdb_set(&db, key, &n->in_other_pkts);
		snprintf(key, LINE_SIZE, "%u_in_bytes", n->id);
		tsdb_set(&db, key, &n->in_bytes);
		snprintf(key, LINE_SIZE, "%u_in_tcp_bytes", n->id);
		tsdb_set(&db, key, &n->in_tcp_bytes);
		snprintf(key, LINE_SIZE, "%u_in_udp_bytes", n->id);
		tsdb_set(&db, key, &n->in_udp_bytes);
		snprintf(key, LINE_SIZE, "%u_in_icmp_bytes", n->id);
		tsdb_set(&db, key, &n->in_icmp_bytes);
		snprintf(key, LINE_SIZE, "%u_in_other_bytes", n->id);
		tsdb_set(&db, key, &n->in_other_bytes);

		snprintf(key, LINE_SIZE, "%u_out_pkts", n->id);
		tsdb_set(&db, key, &n->out_pkts);
		snprintf(key, LINE_SIZE, "%u_out_tcp_pkts", n->id);
		tsdb_set(&db, key, &n->out_tcp_pkts);
		snprintf(key, LINE_SIZE, "%u_out_udp_pkts", n->id);
		tsdb_set(&db, key, &n->out_udp_pkts);
		snprintf(key, LINE_SIZE, "%u_out_icmp_pkts", n->id);
		tsdb_set(&db, key, &n->out_icmp_pkts);
		snprintf(key, LINE_SIZE, "%u_out_other_pkts", n->id);
		tsdb_set(&db, key, &n->out_other_pkts);
		snprintf(key, LINE_SIZE, "%u_out_bytes", n->id);
		tsdb_set(&db, key, &n->out_bytes);
		snprintf(key, LINE_SIZE, "%u_out_tcp_bytes", n->id);
		tsdb_set(&db, key, &n->out_tcp_bytes);
		snprintf(key, LINE_SIZE, "%u_out_udp_bytes", n->id);
		tsdb_set(&db, key, &n->out_udp_bytes);
		snprintf(key, LINE_SIZE, "%u_out_icmp_bytes", n->id);
		tsdb_set(&db, key, &n->out_icmp_bytes);
		snprintf(key, LINE_SIZE, "%u_out_other_bytes", n->id);
		tsdb_set(&db, key, &n->out_other_bytes);

		printf("%u\n", n->id);
		if (counter % 100 == 0) {
			//fprintf(stderr, "Dumped %u entries ...\n", counter);
		}
		counter++;
	}
}


int main(int argc, char** argv) 
{
	if (argc != 6) {
		fprintf(stderr, "Usage: %s <prefix-file> <tsdb-file> " 
			"<profile_dir> <capfile> <unix-timestamp>\n", argv[0]);
		exit(1);
	}
	char* prefix_file = argv[1];
	char* tsdb_file = argv[2];
	char* profile_dir = argv[3];
	char* capfile = argv[4];
	time_t epoch = atoi(argv[5]);

        uint16_t vals_per_entry = 1; // defines how many entries we can have
                                     // we currently store one value per entry.
                                     // this means we do not group by IP_interface
                                     // but have a value vor IP_interface_ifInOctets, ...
                                     // maybe we want to change that
        uint16_t slot_seconds = 300; // defines measurment interval


	struct lpm_tree* tree = read_prefix_file(prefix_file);
	if (!tree) {
		fprintf(stderr, "ERROR: Could not create subnet tree!\n");
		exit(1);
	}

	if (0 != tsdb_open(tsdb_file, &db, &vals_per_entry, slot_seconds, 0)) {
		fprintf(stderr, "ERROR: Could not open tsdb database!\n");
		exit(1);
	}

	if (0 != tsdb_goto_epoch(&db, epoch, 0, 1)) {
		fprintf(stderr, "ERROR: Could not set epoch to %u\n", epoch);
		exit(1);
	}

	fprintf(stderr, "reading nfdump files ...\n");
	read_nfdump_files(profile_dir, capfile, tree);
	fprintf(stderr, "Dumping to tsdb ... \n");
	twalk(network_tree, &store_records_in_db);

	fprintf(stderr, "Cleaning up!\n");
	lpm_destroy(tree);
	tsdb_flush(&db);
	tsdb_close(&db);

	return 0;
}

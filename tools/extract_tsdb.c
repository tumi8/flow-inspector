#include <tsdb_api.h>

#include <unistd.h>

int main(int argc, char** argv)
{
	if (argc != 5) {
		fprintf(stderr, "Usage: %s <tsdbfile> <key_pref> <start_time> <end_time>\n", argv[0]);
		exit(1);
	}
	char* tsdb_file = argv[1];
	char* key_pref = argv[2];
	uint32_t start = atoi(argv[3]);
	uint32_t end   = atoi(argv[4]);
	uint32_t interval_size = 300;
	uint16_t vals_per_entry = 1;

	if (start > end) {
		fprintf(stderr, "ERROR: Start (%u) is larger than end (%u)\n", start, end);
		exit(1);
	}

	int ret;
	tsdb_handler db;

	ret = tsdb_open(tsdb_file, &db, &vals_per_entry, interval_size, 1);

	uint32_t i = start;
	uint32_t* read_val;
	char* key = "interface_130.197.2.87_5001_ifHCOutMulticastPkts";
	while (i <  end) {
		ret = tsdb_goto_epoch(&db, i, 0, 1);
		if (ret != 0) {
			fprintf(stderr, "ERROR: Cannot go to interval %u!", i);
			goto increment;
		}
		ret = tsdb_get_by_key(&db, key, &read_val);
		if (ret != 0) {
			// no value for epoch in tsdb. do not print any
		} else {
			printf("%u %u\n", i, *read_val);
		}

increment:
		i += interval_size;
	}

	tsdb_close(&db);
}

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <dirent.h>

#include <rrd.h>
#include <tsdb_api.h>

tsdb_handler db;
#define LINE_SIZE 4096

void init_tsdb(const char* filename)
{
	int ret;

	uint16_t vals_per_entry = 1; // defines how many entries we can have
				     // we currently store one value per entry.
				     // this means we do not group by IP_interface
				     // but have a value vor IP_interface_ifInOctets, ...
				     // maybe we want to change that
	uint16_t slot_seconds = 300; // defines measurment interval

	
	ret = tsdb_open(filename, &db, &vals_per_entry, slot_seconds, 0);
	if (ret != 0) {
		fprintf(stderr, "ERROR: Creating tsdb file!\n");
		exit(-1);
	}
}

void update_tsdb(const char* key_prefix, const char* line)
{
	// create a copy of the line for strtok
	char* tmp_line = (char*) malloc(sizeof(char) * (strlen(line) + 1));
	if (!tmp_line) {
		fprintf(stderr, "ERROR: malloc of tmp_line: %s\n", strerror(errno));
		exit(-1);
	}

	strcpy(tmp_line, line);
	char delimiter[] = " :";
	char* ptr;
	char key[LINE_SIZE];
	int ret; 

	// the input line format is KEY1:VALUE KEY2:VALUE2 KEY3:VALUE3 ...
	// We parse a token. If we have a key, we construct the final
	// key from key_prefix_key (which results in IP_interface_KEY1, IP_interface_KEY2, ...)
	ptr = strtok(tmp_line, delimiter);
	int is_key = 1; // if key is one, then we have a key, otherwise we have a value 
	int first_template = 1; // 1 if this is the first entry to the template string, 0 otherwise
	while (ptr) {
		if (is_key == 1) {
			// store the 
			snprintf(key, LINE_SIZE, "%s_%s", key_prefix, ptr);
		} else {
			uint64_t val = atoi(ptr);
			ret = tsdb_set(&db, key, &val);
			if (ret != 0) {
				fprintf(stderr, "ERROR: updating value (%llu) for key (%s) in tsdb\n", val, key);
			}
		}
		is_key = is_key?0:1;
		ptr = strtok(NULL, delimiter);
	}
	free(tmp_line);
}

void close_tsdb()
{
	tsdb_close(&db);
}

void create_rra(const char* filename, const char* line, char* data_type)
{
	// create a copy of the line for strtok
	char* tmp_line = (char*) malloc(sizeof(char) * (strlen(line) + 1));
	if (!tmp_line) {
		fprintf(stderr, "ERROR: malloc of tmp_line: %s\n", strerror(errno));
		exit(-1);
	}

	strcpy(tmp_line, line);
	char delimiter[] = " :";
	char* ptr;

#define MAX_CREATE_STRING 4096
	char rrd_create_string[MAX_CREATE_STRING];
	snprintf(rrd_create_string, MAX_CREATE_STRING, "rrdtool create %s --start 1 --step 300 ", filename);

	ptr = strtok(tmp_line, delimiter);
	int key = 1; // if key is one, then we have a key, otherwise we have a value 
	size_t command_len  = strlen(rrd_create_string);
	while (ptr) {
		if (key == 1) {
			// NOTE: the maximum size of a rrdtool data source can be 20 bytes, which 
			// means that we can have 19 characters and the trailing NULL character. 
			// we therefore need to limit our string to 19 characters ...
			if (strlen(ptr) > 19) {
				ptr[19] = 0;
			}
			snprintf(rrd_create_string + command_len, MAX_CREATE_STRING - command_len, " DS:%s:%s:600:U:U", ptr, data_type);
			command_len = strlen(rrd_create_string);
		}
		key = key?0:1;
		ptr = strtok(NULL, delimiter);
	}

	snprintf(rrd_create_string + command_len, MAX_CREATE_STRING - command_len, " RRA:AVERAGE:0.5:1:500  RRA:AVERAGE:0.5:1:600  RRA:AVERAGE:0.5:6:700  RRA:AVERAGE:0.5:24:775  RRA:AVERAGE:0.5:288:797  RRA:MAX:0.5:1:500  RRA:MAX:0.5:1:600  RRA:MAX:0.5:6:700  RRA:MAX:0.5:24:775  RRA:MAX:0.5:288:797");
	//printf("%s\n", rrd_create_string);
	system(rrd_create_string);

	free(tmp_line);
}

void update_rra(const char* filename, const char* line, const char* timestamp)
{
	// create the structures that we need for the call to rrd_update
	// we need only 4 args to the call:
	// "update", filename, "updatestring"
	char* rrd_args[6];
#define RRD_ELEM_LEN 4096
#define TEMPLATE_LEN 1024
#define VALUE_LEN 1024
	size_t i;
	for (i = 0; i != 5; ++i) {
		rrd_args[i] = (char*) malloc(RRD_ELEM_LEN*sizeof(char));
		bzero(rrd_args[i], RRD_ELEM_LEN);
	}
	rrd_args[5] = NULL;

	char template_string[TEMPLATE_LEN];
	bzero(template_string, TEMPLATE_LEN);
	size_t template_string_len = 0;

	char value_string[VALUE_LEN];
	bzero(value_string, TEMPLATE_LEN);
	snprintf(value_string, VALUE_LEN, "%s", timestamp);
	size_t value_string_len = strlen(value_string);

	// create a copy of the line for strtok
	char* tmp_line = (char*) malloc(sizeof(char) * (strlen(line) + 1));
	if (!tmp_line) {
		fprintf(stderr, "ERROR: malloc of tmp_line: %s\n", strerror(errno));
		exit(-1);
	}

	strcpy(tmp_line, line);
	char delimiter[] = " :";
	char* ptr;

	ptr = strtok(tmp_line, delimiter);
	int key = 1; // if key is one, then we have a key, otherwise we have a value 
	int first_template = 1; // 1 if this is the first entry to the template string, 0 otherwise
	while (ptr) {
		if (key == 1) {
			// NOTE: the maximum size of a rrdtool data source can be 20 bytes, which 
			// means that we can have 19 characters and the trailing NULL character. 
			// we therefore need to limit our string to 19 characters ...
			if (strlen(ptr) > 19) {
				ptr[19] = 0;
			}
			// the template string has the format
			// KEY1:KEY2:KEY3:KEY4 ...
			snprintf(template_string + template_string_len, TEMPLATE_LEN - template_string_len, "%s%s", first_template?"":":", ptr);
			template_string_len = strlen(template_string);
			first_template = 0;
		} else {
			// the value string has the format
			// N:VALUE1:VALUE2:VALUE3: ...
			snprintf(value_string + value_string_len, VALUE_LEN - value_string_len, ":%s", ptr);
			value_string_len = strlen(value_string);
		}
		key = key?0:1;
		ptr = strtok(NULL, delimiter);
	}
	//printf("%s %lu\n", template_string, strlen(template_string));
	//printf("%s %lu\n", value_string, strlen(value_string));
	snprintf(rrd_args[0], RRD_ELEM_LEN, "update");
	snprintf(rrd_args[1], RRD_ELEM_LEN, "%s", filename);
	snprintf(rrd_args[2], RRD_ELEM_LEN, "--template", template_string);
	snprintf(rrd_args[3], RRD_ELEM_LEN, "%s", template_string);
	snprintf(rrd_args[4], RRD_ELEM_LEN, "%s", value_string);
	//printf("%s %s %s %s %s\n", rrd_args[0], rrd_args[1], rrd_args[2], rrd_args[3], rrd_args[4], rrd_args[5]);
	if (rrd_update(5, (char**)rrd_args) != 0) {
		fprintf(stderr, "Failed to update rrdtool: %s\n", rrd_get_error());
		rrd_clear_error();
	}

	for (i = 0; i != 5; ++i) {
		free(rrd_args[i]);
	}
	free(tmp_line);
}

int read_cache_file(const char* cache_file, const char* rrd_dir, const char* timestamp)
{
	FILE* fd = fopen(cache_file, "r");
	int fgets_ret = 0;
	size_t counter = 0;
	char line[LINE_SIZE];
	while (fgets(line, LINE_SIZE, fd)) {
		// the expected line format is
		// <type>_<ip>_<subid> <field1>:<field_value1> <field2>:<field_value2> ...
		// where <type> denotes the type as in (interface, cpu, loadbalancing, ...)
		// <ip> denotes the device id
		// <subid> denotes an id that depends on the <type>, e.g. if <type> == interface, then subid denotes the interface id
		char* delim = strchr(line, ' ');
		if (NULL == delim) {
			fprintf(stderr, "ERROR: Filename \"%s\" does not match the pattern <control_string> <field_data>'!\n", line);
			continue;
		}
		// extract the control string
		char control_string[LINE_SIZE];
		strncpy(control_string, line, delim - line);
		control_string[delim-line] = 0;
		char* data = delim + 1;
		// last character is probably a '\n'. Remove it
		if (data[strlen(data) - 1] == '\n') {
			data[strlen(data) - 1] = 0;
		}

		// construct the desired name of the rra file. which should be rra-dir/<control_string>.rrd
		char rra_filename[2*LINE_SIZE];
		snprintf(rra_filename, 2*LINE_SIZE, "%s/%s.rrd", rrd_dir, control_string);
		// check if the rra exists and if we have write access to it
		if (-1 == access(rra_filename, W_OK | F_OK)) {
			printf("Creating rra \"%s\"...\n", rra_filename);
			if (strstr(control_string, "cpu_") == control_string || strstr(control_string, "ciscomemory_") == control_string) {
				create_rra(rra_filename, data, "GAUGE");
			} else {
				create_rra(rra_filename, data, "COUNTER");
			}
		}
		//printf("Updating \"%s\"...\n", rra_filename);
		// now try to update the rra
		update_rra(rra_filename, data, timestamp);
		update_tsdb(control_string, data);

		counter++;
		if (counter % 1000 == 0) {
			//printf("Worked on %lu rrdfiles ...\n", counter);
		}

	}
	if (ferror(fd)) {
		fprintf(stderr, "ERROR: reading cache file %s: %s\n", cache_file, strerror(errno));
	}
	fclose(fd);
	return 0;
}

int read_cache_dir(const char* cache_dir, const char* rrd_dir, const char* timestamp)
{
	// walk through all files in cache_dir (do not recurse ...)
	// and add the files to the rras that should be stored in rra-dir
	// create the rrd files if they do not exist
	DIR* dir = opendir(cache_dir);
	if (!dir) {
		fprintf(stderr, "ERROR: Could not open cache-dir \"%s\": %s\n", cache_dir, strerror(errno));
		exit(-4);
	}

	struct dirent* d;
	size_t counter = 0;
	while (NULL != (d = readdir(dir))) {
		if (d->d_type & DT_REG || d->d_type == DT_UNKNOWN) {
			// we are only interested in the file snmp_info_dumps.txt
			if (strcmp(d->d_name, "snmp_info_dumps.txt")) {
				// not our file
				continue;
			}
			// we found our file. parse it and quit
			char cache_filename[1024];
			snprintf(cache_filename, 1024, "%s/%s", cache_dir, d->d_name);
			read_cache_file(cache_filename, rrd_dir, timestamp);
			closedir(dir);
			return 0;

		} 
	}
	fprintf(stderr, "ERROR: Could not find a SNMP file in directory!\n");
	closedir(dir);
	return 0;
}

int main(int argc, char** argv)
{
	if (argc != 4) {
		fprintf(stderr, "Usage: %s <cache-file> <rra-dir> <timestamp>\n", argv[0]);
		return -1;
	}
	char* cache_file = argv[1];
	char* rra_dir = argv[2];
	char* timestamp = argv[3];

	// check if cache_file is a file and check if we can read from it
	struct stat s;
	if (-1 == stat(cache_file, &s)) {
		fprintf(stderr, "ERROR stating cache-file \"%s\": %s\n", cache_file, strerror(errno));
		exit(-1);
	}
	/*
	if (!(s.st_mode & S_IFDIR)) {
		fprintf(stderr, "ERROR: cache-dir is not a directory!\n");
		exit(-2);
	}
	if (-1 == access(cache_dir, R_OK | X_OK)) {
		fprintf(stderr, "ERROR: Insufficient rights on cache-dir \"%s\". Need read and execute rights!\n", cache_dir);
		exit(-3);
	}
	*/

	// check if the rra_dir is a directory and if we have write access to it
	if (-1 == stat(rra_dir, &s)) {
		fprintf(stderr, "ERROR stating rra-dir \"%s\": %s\n", rra_dir, strerror(errno));
		exit(-1);
	}
	if (-1 == access(rra_dir, R_OK | X_OK | W_OK)) {
		fprintf(stderr, "ERROR: Insufficient rights on rra-dir \"%s\". Need read, write, and execute rights!\n", rra_dir);
		exit(-3);
	}

	char tsdb_file[2*LINE_SIZE];
	int ret;
	snprintf(tsdb_file, 2*LINE_SIZE, "%s/%s", rra_dir, "snmp.tsdb");
	init_tsdb(tsdb_file);
	int t = atoi(timestamp);
	ret = tsdb_goto_epoch(&db, t, 0, 1);
	if (ret != 0) {
		fprintf(stderr, "RROR: tsdb failed to go to epoch %lu\n", t);
		exit(1);
	}


	read_cache_file(cache_file, rra_dir, timestamp);
	
	tsdb_flush(&db);
	close_tsdb();


	return 0;
}

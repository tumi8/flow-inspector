#include <stdio.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <time.h>

#include <sys/types.h>
#include <sys/wait.h>

#include <map>
#include <fstream>
#include <vector>
#include <sstream>
#include <iostream>
#include <string>

#include <net-snmp/net-snmp-config.h>
#include <net-snmp/net-snmp-includes.h>

#include <iostream>
#include <string>

/******************************************** new snmp stuff **************************************/

struct oid_struct {
	const char *Name;
	oid Oid[MAX_OID_LEN];
	int OidLen;
};
struct session {
	struct snmp_session *sess;
	size_t oid_counter;
	oid rootOID[MAX_OID_LEN];
	size_t rootOID_len;
	std::string output_dir;
	std::string prefix;
	std::string ip;
	time_t measurement_time;
};

struct host {
	std::string name;
	std::string community;
};

/*************************** globals ***************/


std::vector<std::string> oid_string_list;
std::vector<std::string> oid_prefix_list;
bool do_output;
volatile int active_hosts;


/*************************** methods ***************/

int dump_result (int status, struct snmp_session *sp, struct snmp_pdu *pdu, oid* name, size_t& name_length, struct session* session)
{
	char buf[1024];
	struct variable_list *vp;
	int ix;

	bool running = true;
	switch (status) {
		case STAT_SUCCESS:
			{
			if (!do_output) {
				// this is just a availablity check.
				// don't generate any output 
				return 1;
			}	

			std::stringstream s;
			s << session->output_dir << "/" << session->prefix << "-" << session->ip << "-" << session->measurement_time << ".txt";
			FILE* f = fopen(s.str().c_str(), "a+");
			if (!f) {
				fprintf(stderr, "Error opening %s: %s\n", s.str().c_str(), strerror(errno));
				return 1;
			}
			vp = pdu->variables;
			if (pdu->errstat == SNMP_ERR_NOERROR) {
				while (vp) {
					if (vp->name_length < session->rootOID_len || (memcmp(session->rootOID, vp->name, session->rootOID_len * sizeof(oid)) != 0))  {
						// this oid does not beling to the same rootOID. 
						running = false;
						vp = vp->next_variable;
						continue;
					}
					//snprint_variable(buf, sizeof(buf), vp->name, vp->name_length, vp);
					//fprintf(stdout, "%s: %s\n", sp->peername, buf);
					if ((vp->type != SNMP_ENDOFMIBVIEW) &&
						(vp->type != SNMP_NOSUCHOBJECT) &&
						(vp->type != SNMP_NOSUCHINSTANCE) &&
						(running)) {
							fprint_variable(f, vp->name, vp->name_length, vp);
							/*
							 * Check if last variable, and if so, save for next request.  
							 */
							if (vp->next_variable == NULL) {
								memmove(name, vp->name,
								vp->name_length * sizeof(oid));
								name_length = vp->name_length;
								fclose(f);
								return 2;
							}
					}
					vp = vp->next_variable;
				}
			} else {
				for (ix = 1; vp && ix != pdu->errindex; vp = vp->next_variable, ix++)
					;
				if (vp) {
					snprint_objid(buf, sizeof(buf), vp->name, vp->name_length);
				} else {
					strcpy(buf, "(none)");
				}
				fprintf(stdout, "%s: %s: %s\n", sp->peername, buf, snmp_errstring(pdu->errstat));
			}
			fclose(f);
			}
			return 1;
		case STAT_TIMEOUT:
			struct timeval now;
			struct timezone tz;
			struct tm *tm;
		
			if (do_output) {
				gettimeofday(&now, &tz);
				tm = localtime(&now.tv_sec);
				fprintf(stdout, "%.2d:%.2d:%.2d.%.6d - %s - Timeout\n", tm->tm_hour, tm->tm_min, tm->tm_sec,now.tv_usec, sp->peername);
			} else {
				// this is the availability check. We must output the IP that does not react
				fprintf(stdout, "%s\n", sp->peername);
			}
			return 0;
		case STAT_ERROR:
			if (do_output) {
				snmp_perror(sp->peername);
			} else {
				// this is the availability check. We must output the IP that does not react
				fprintf(stdout, "%s\n", sp->peername);
			}
			return 0;
	}
	return 0;
}

int asynch_response(int operation, struct snmp_session *sp, int reqid, struct snmp_pdu *pdu, void *magic)
{
	struct session *host = (struct session *)magic;
	struct snmp_pdu *req;

	oid name[MAX_OID_LEN];
	size_t name_length = MAX_OID_LEN;
		
	int retVal = 0;
	bool do_request = false;
	if (operation == NETSNMP_CALLBACK_OP_RECEIVED_MESSAGE) {
		if ((retVal = dump_result(STAT_SUCCESS, host->sess, pdu, name, name_length, host)) > 0) {
			// send next GET (if any) 
			// create the oid structs

			if (retVal == 1) {
				// we are now finished with reading the tree for the mib
				// go one and read the next item from the oid_counters
				host->oid_counter++;
				if (host->oid_counter != oid_string_list.size()) {
					read_objid(oid_string_list[host->oid_counter].c_str(), name, &name_length);
					host->rootOID_len = MAX_OID_LEN;
					read_objid(oid_string_list[host->oid_counter].c_str(), host->rootOID, &host->rootOID_len);
					host->prefix = oid_prefix_list[host->oid_counter];
					do_request = true;
				} else {
					do_request = false;
				}
			} else if (retVal == 2) {
				// there are still elements in the subtree. request a follow up
				// based on the last variable (stored in name and name_length)
				do_request = true;
			}
			if (do_request) {
				req = snmp_pdu_create(SNMP_MSG_GETBULK);
				req->non_repeaters = 0;
				req->max_repetitions = 20;    /* fill the packet */

				snmp_add_null_var(req, name, name_length);
				if (snmp_send(host->sess, req)) {
					return 1;
				} else {
					snmp_perror("snmp_send");
					snmp_free_pdu(req);
				}
			}
		}
	} else {
		dump_result(STAT_TIMEOUT, host->sess, pdu, name, name_length, host);
	}
	

	// something went wrong (or end of variables) 
	// this host not active any more
	active_hosts--;
	return 1;
}

int init_snmp()
{
	init_snmp("snmpwalk-worker");
	// make sure we have the output options -O benq activated
	snmp_out_toggle_options((char*)"benq");
}

int check_snmp_availability()
{
	init_snmp("snmpwalk-worker-availability");
}

int send_snmp_queries(std::vector<host>& hosts, std::vector<session>& session_vec, const std::string& output_dir)
{
	time_t measurement_time = time(NULL);	// all snmpbulkwalk output files *MUST* encode the same timestamp
						// regardless of when the actuall process did return its data

	// create one session for each host and each oid. Please note that this
	// will create multiple sessions for a single host!
	for (std::vector<host>::const_iterator i = hosts.begin(); i != hosts.end(); ++i) {
		// get the appropriate previously generated session
		struct session* hs = &session_vec[i - hosts.begin()];

		struct snmp_pdu *req;
		struct snmp_session sess;
		snmp_sess_init(&sess);                      /* initialize session */
		sess.version = SNMP_VERSION_2c;
		sess.peername = strdup(i->name.c_str());
		sess.community = (u_char*)strdup(i->community.c_str());
		sess.community_len = strlen(i->community.c_str());
		sess.callback = asynch_response;            /* default callback */
		sess.callback_magic = hs;
		if (!(hs->sess = snmp_open(&sess))) {
			snmp_perror("snmp_open");
			continue;
		}
		hs->oid_counter = 0;
		hs->measurement_time = measurement_time;
		hs->output_dir = output_dir;
		hs->ip = i->name;
		hs->prefix = oid_prefix_list[hs->oid_counter];

		// create the oid structs
		oid anOID[MAX_OID_LEN];
		size_t anOID_len = MAX_OID_LEN;
		read_objid(oid_string_list[hs->oid_counter].c_str(), anOID, &anOID_len);
		hs->rootOID_len = MAX_OID_LEN;
		read_objid(oid_string_list[hs->oid_counter].c_str(), hs->rootOID, &(hs->rootOID_len));

		//create the request
		req = snmp_pdu_create(SNMP_MSG_GETBULK);        /* send the first GET */
		req->non_repeaters = 0;
		req->max_repetitions = 10;    /* fill the packet */
		snmp_add_null_var(req, anOID, anOID_len);
		if (snmp_send(hs->sess, req)) {
			active_hosts++;
		} else {
			snmp_perror("snmp_send");
			snmp_free_pdu(req);
		}	
	}

	/* loop while any active hosts */
	while (active_hosts) {
		int fds = 0, block = 1;
		fd_set fdset;
		struct timeval timeout;
		
		FD_ZERO(&fdset);
		snmp_select_info(&fds, &fdset, &timeout, &block);
		fds = select(fds, &fdset, NULL, NULL, block ? NULL : &timeout);
		if (fds < 0) {
			perror("select failed");
			exit(1);
		}
		if (fds)
			snmp_read(&fdset);
		else
			snmp_timeout();
	}
	
	/* cleanup */
	for (std::vector<session>::const_iterator i = session_vec.begin(); i != session_vec.end(); ++i) {
		if (i->sess) 
			snmp_close(i->sess);
	}
}



int perform_snmp_measurement(const std::string& filename, const std::string& output_dir, const std::string& types_file)
{
	std::vector<session> session_vec;
	std::vector<host> hosts;

	init_snmp();
	
	std::ifstream in(filename.c_str());
	if (!in) {
		std::cerr << "Error opening snmp device list (" << filename << "): " << strerror(errno) << std::endl;
		return -1;
	}

	// read all hosts and community strings from the input file
	// and store them in the hosts vector. This is later on used
	// to create the sessions
	std::string line;
	while (in && !in.eof()) {
		std::getline(in, line);
		if (line == "") {
			continue;
		}
		
		// Split line into ip address and snmp community string
		// expected format for the lines: 
		// IP COMMUNITY_STRING
		std::stringstream line_split(line);
		std::string ip, community_string;
		line_split >> ip >> community_string;
		host h;
		h.name = ip;
		h.community = community_string;
		hosts.push_back(h);

		// add a new persistent entry to session_vec
		// and get point hs to this element. We need
		// it for the asynchronous callback handler
		struct session s_base;
		session_vec.push_back(s_base);
	}

	// read list of snmp-strings and resulting file names
	std::ifstream types_in(types_file.c_str());
	if (!types_in) {
		std::cout << "ERROR: Could not open types file " << types_file << ": " << strerror(errno) << std::endl;
		return -1;
	}

	while (types_in && !types_in.eof()) {
		std::getline(types_in, line);
		if (line == "") {
			continue;
		}
		// Split line into snmp-oid and IP
		// expected format for the lines: 
		// SNMP-OID (in dotted format) FILE_PREFIX
		std::stringstream line_split(line);
		std::string oid, file_prefix;
		line_split >> oid >> file_prefix;
		oid_string_list.push_back(oid);
		oid_prefix_list.push_back(file_prefix);
	}

	send_snmp_queries(hosts, session_vec, output_dir);

	return 0;
}

int perform_snmp_availability_check()
{
	std::vector<session> session_vec;
	std::vector<host> hosts;

	init_snmp();

	// read hosts from std::cin
	std::string line;
	while (std::cin) {
		std::getline(std::cin, line);
		if (!std::cin.eof()) {
			// Split line into ip address and snmp community string
			// expected format for the lines: 
			// IP COMMUNITY_STRING
			std::stringstream line_split(line);
			std::string ip, community_string;
			line_split >> ip >> community_string;
			host h;
			h.name = ip;
			h.community = community_string;
			hosts.push_back(h);

			// add a new persistent entry to session_vec
			// and get point hs to this element. We need
			// it for the asynchronous callback handler
			struct session s_base;
			session_vec.push_back(s_base);
		}
	}

	oid_string_list.push_back("1.3.6.1.2.1.1.4.0");
	oid_prefix_list.push_back("none");
	send_snmp_queries(hosts, session_vec, "");
}

int main(int argc, char** argv)
{
	if (argc == 4) {
		do_output = true;
		return perform_snmp_measurement(argv[1], argv[2], argv[3]);
	} else if (argc == 1) {
		do_output = false;
		return perform_snmp_availability_check();
	} else {
		std::cerr << "Usage: " << argv[0] << " <ip_list> <output_dir> <snmp-type-list>" << std::endl;
		std::cerr << "or " << std::endl;
		std::cerr << "Usage: " << argv[0] << std::endl;
		return -1;
	}
	return 0;
}

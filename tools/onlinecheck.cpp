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


std::map<pid_t, std::string>  started_processes; 	// contains a list of all started ping processes (changed by main())
std::map<std::string, int> finished_processes;	 	// contains a list of all finished ping processes (only changed in signal handler. Do not touch this
							// in other parts of the code, as we do not perform any locking on the data structure
volatile size_t finished_counter = 0;
volatile size_t started_counter = 0;
volatile int active_hosts;
//
// waits for children. options is passed to the wait4 calls
// so if you don't want this call to block, call the function
// with WNOHANG. Otherwise, specify 0 which will wait for all
// chilren
void wait_for_child(int options)
{
	pid_t cpid;
	int ret;
	while (started_counter!= finished_counter && 0 != (cpid = wait4(-1, &ret, options, NULL))) {
		finished_counter++;
		std::string ip = started_processes[cpid];
		finished_processes[ip] = WEXITSTATUS(ret);
	}
}

int start_process(const std::string& app_name, char* const args[], const std::string& outputfile, const std::string& ip)
{
	static int counter = 0;
	pid_t pid = fork();
	if (pid == 0) {
		// child
		int fid = open(outputfile.c_str(), O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);
		if (fid == -1) {
			std::cerr << "could not open output file \"" << outputfile << "\" Reason: " << strerror(errno) <<  std::endl;
			exit(1);
		}
		dup2( fid, STDOUT_FILENO );
		dup2( fid, STDERR_FILENO );
		close(fid);
		execvp(app_name.c_str(), args);
		// should never get here
		std::cerr << "Failed to exec " << app_name << ": " << strerror(errno) << std::endl;

		// should never get here
		exit(255);
	} else if (pid == -1) {
		std::cerr << "Error forking a new process: " << strerror(errno) << std::endl;
		return -1;
	} else {
		// parent
		counter++;
		if (counter % 100 == 0) {
			// sleep for some time to give the already started processes some time to finish
			// this should reduce the number 
			 struct timespec wait = {
				0, 200000000
		        };
			nanosleep(&wait, NULL);
		}
		started_processes[pid] = ip;
		started_counter++;
		
		// check if any of the processes already finished
		// if one of them already finished, then collect its 
		// result before spawning new processe
		wait_for_child(WNOHANG);
	}
}

int perform_ping_measurement(std::istream& in)
{
	std::string line;
	while (!in.eof()) {
		std::getline(in, line);
		if (line == "") {
			continue;
		}
		char* command[] = {
			(char*)"ping",
			(char*)"-c",
			(char*)"2",
			(char*)line.c_str(),
			NULL
		};
		start_process("ping", command, "/dev/null", line);
	}

	//std::cout << "Started all processes. Now waiting for them to finish ..." << std::endl;
	// wait for all child processes to finsih (finished_processes is modified in 
	// signal handler)
	while (started_counter != finished_counter) {
		wait_for_child(0);
	}

	for (std::map<std::string, int>::const_iterator i = finished_processes.begin(); i != finished_processes.end(); ++i) {
		if (i->second != 0) {
			std::cout << i->first << std::endl;
		}
	}

	return 0;
}


int main(int argc, char** argv)
{
	if (argc != 2 && argc != 1) {
		std::cerr << "Usage: " << argv[0] << " [ <ip_list_file> ]" << std::endl;
		std::cerr << "\tIf <ip_list_file> is provided, " << argv[0] << " will read one IP address per line and perform a ping measurement to the host" << std::endl;
		std::cerr << "\tif no parameter is given, then " << argv[0] << " will read IPs from stdin." << std::endl;
		return -1;
	}

	if (argc == 2) {
		std::ifstream in(argv[1]);
		if (!in) {
			std::cerr << "Error opening snmp device list (" << argv[1] << "): " << strerror(errno) << std::endl;
			return -1;
		}
		return perform_ping_measurement(in);
	} else {
		return perform_ping_measurement(std::cin);
	}

}

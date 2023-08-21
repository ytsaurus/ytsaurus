#include <stdlib.h>
#include <signal.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

bool NeedRotation = false;

void SighupHandler(int)
{
    NeedRotation = true;
}

int main(int argc, char* argv[])
{
    if (argc != 5) {
        std::cerr << "Usage: ./dummy-logger <log_file> <logging_period> <log_records> <termination_delay>" << std::endl;
        return 1;
    }

    std::string logFile{argv[1]};
    int loggingPeriod = atoi(argv[2]);
    int logRecords = atoi(argv[3]);
    int terminationDelay = atoi(argv[4]);

    std::ofstream log{logFile};

    struct sigaction sa;
    sa.sa_handler = &SighupHandler;
    sa.sa_flags = SA_RESTART;
    sigfillset(&sa.sa_mask);

    if (sigaction(SIGHUP, &sa, NULL) == -1) {
        std::cerr << "Cannot handle SIGHUP" << std::endl;
        return 1;
    }

    for (int i = 0; i < logRecords; i++) {
        if (NeedRotation) {
            log = std::ofstream{logFile};
            NeedRotation = false;
        }

        for (int j = 0; j < 7; j++) {
            if (j) {
                log << "\t";
            }
            log << std::to_string(i);
        }
        log << std::endl;

        std::this_thread::sleep_for(std::chrono::milliseconds(loggingPeriod));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(terminationDelay));

    return 0;
}


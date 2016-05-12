#include "init.h"

#include <mapreduce/yt/interface/operation.h>
#include <mapreduce/yt/common/log.h>
#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/io/job_reader.h>
#include <mapreduce/yt/http/requests.h>

#include <util/string/cast.h>
#include <util/folder/dirut.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void WriteVersionToLog()
{
    LOG_INFO("Wrapper version: %s", ~TProcessState::Get()->ClientVersion);
}

void Initialize(int argc, const char* argv[])
{
    auto logLevelStr = to_lower(TConfig::Get()->LogLevel);
    ILogger::ELevel logLevel;
    if (logLevelStr == "fatal") {
        logLevel = ILogger::FATAL;
    } else if (logLevelStr == "error") {
        logLevel = ILogger::ERROR;
    } else if (logLevelStr == "info") {
        logLevel = ILogger::INFO;
    } else if (logLevelStr == "debug") {
        logLevel = ILogger::DEBUG;
    } else {
        Cerr << "Invalid log level: " << TConfig::Get()->LogLevel << Endl;
        exit(1);
    }
    SetLogger(CreateStdErrLogger(logLevel));

    TProcessState::Get()->SetCommandLine(argc, argv);

    if (argc != 5) {
        WriteVersionToLog();
        return;
    }

    Stroka jobType(argv[1]);
    if (jobType != "--yt-map" && jobType != "--yt-reduce") {
        WriteVersionToLog();
        return;
    }

    Stroka jobName(argv[2]);
    size_t outputTableCount = FromString<size_t>(argv[3]);
    int hasState = FromString<int>(argv[4]);

    exit(TJobFactory::Get()->GetJobFunction(~jobName)(outputTableCount, hasState));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


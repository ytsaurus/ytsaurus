#include "init.h"

#include <mapreduce/yt/interface/operation.h>
#include <mapreduce/yt/common/log.h>
#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/io/job_reader.h>
#include <mapreduce/yt/http/requests.h>

#include <util/generic/singleton.h>
#include <util/string/cast.h>
#include <util/folder/dirut.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void Initialize(int argc, const char* argv[])
{
    SetLogger(CreateStdErrLogger(ILogger::DEBUG));
    LOG_INFO("Wrapper version: %s", ~TProcessProperties::Get()->ClientVersion);

    TProcessProperties::Get()->SetCommandLine(argc, argv);

    if (argc != 5) {
        return;
    }

    Stroka jobType(argv[1]);
    if (jobType != "--yt-map" && jobType != "--yt-reduce") {
        return;
    }

    Stroka jobName(argv[2]);
    size_t outputTableCount = FromString<size_t>(argv[3]);
    int hasState = FromString<int>(argv[4]);

    exit(TJobFactory::Get()->GetJobFunction(~jobName)(outputTableCount, hasState));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


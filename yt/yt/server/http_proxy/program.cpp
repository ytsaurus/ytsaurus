#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/server/lib/logging/program_describe_structured_logs_mixin.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/driver/config.h>

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/library/program/helpers.h>

#include <yt/yt/library/profiling/solomon/config.h>

#include <yt/yt/library/auth_server/config.h>

#include <yt/yt/core/logging/config.h>

namespace NYT::NHttpProxy {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TProxyProgram
    : public TServerProgram<TProxyProgramConfig>
    , public NLogging::TProgramDescribeStructuredLogsMixin
{
public:
    TProxyProgram()
        : TProgramDescribeStructuredLogsMixin(Opts_)
    {
        SetMainThreadName("HttpProxyProg");
    }

private:
    void DoStart() final
    {
        // TODO(babenko): refactor
        ConfigureAllocator({.SnapshotUpdatePeriod = GetConfig()->HeapProfiler->SnapshotUpdatePeriod});

        auto bootstrap = CreateHttpProxyBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run()
            .Get()
            .ThrowOnError();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunHttpProxyProgram(int argc, const char** argv)
{
    TProxyProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy

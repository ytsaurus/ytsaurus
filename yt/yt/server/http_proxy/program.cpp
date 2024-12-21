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

class THttpProxyProgram
    : public TServerProgram<NHttpProxy::TProxyConfig>
    , public NLogging::TProgramDescribeStructuredLogsMixin
{
public:
    THttpProxyProgram()
        : TProgramDescribeStructuredLogsMixin(Opts_)
    {
        SetMainThreadName("HttpProxy");
    }

private:
    void DoStart() final
    {
        // TODO(babenko): refactor
        ConfigureAllocator({.SnapshotUpdatePeriod = GetConfig()->HeapProfiler->SnapshotUpdatePeriod});

        auto bootstrap = New<NHttpProxy::TBootstrap>(GetConfig(), GetConfigNode());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunHttpProxyProgram(int argc, const char** argv)
{
    THttpProxyProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy

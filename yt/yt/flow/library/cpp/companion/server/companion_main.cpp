#include "companion_main.h"

#include "config.h"
#include "server.h"

#include "private.h"

#include <yt/yt/library/program/program.h>

#include <util/system/thread.h>

namespace NYT::NFlow::NCompanionServer {

constinit const auto Logger = CompanionServerLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TCompanionProgram final
    : public TProgram
{
public:
    explicit TCompanionProgram(TPipeline pipeline)
        : Pipeline_(std::move(pipeline))
    { }

protected:
    void DoRun() override
    {
        ::TThread::SetCurrentThreadName("CompanionMain");

        ConfigureCrashHandler();
        // A hard exit on SIGTERM is deliberate (same as the flow node): the
        // worker replays in-flight batches, so exactly-once holds; the Python
        // companion's grace-period drain is a latency nicety, not required
        // for correctness.
        ConfigureExitZeroOnSigterm();

        auto config = LoadCompanionExecutionConfigFromEnv();
        auto server = New<TCompanionServer>(std::move(config), std::move(Pipeline_));
        server->Start();

        YT_TLOG_INFO("Companion server started");
        Sleep(TDuration::Max());
    }

private:
    TPipeline Pipeline_;
};

} // namespace

int RunCompanionMain(int argc, const char** argv, TPipeline pipeline)
{
    return TCompanionProgram(std::move(pipeline)).Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer

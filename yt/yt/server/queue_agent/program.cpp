#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

class TQueueAgentProgram
    : public TServerProgram<TQueueAgentServerConfig>
{
public:
    TQueueAgentProgram()
    {
        SetMainThreadName("QueueAgent");
    }

private:
    void DoStart() final
    {
        auto* bootstrap = new TBootstrap(GetConfig(), GetConfigNode());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunQueueAgentProgram(int argc, const char** argv)
{
    TQueueAgentProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent

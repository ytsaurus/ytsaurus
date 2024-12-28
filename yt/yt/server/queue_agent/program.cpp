#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

class TQueueAgentProgram
    : public TServerProgram<TQueueAgentProgramConfig>
{
public:
    TQueueAgentProgram()
    {
        SetMainThreadName("QAProg");
    }

private:
    void DoStart() final
    {
        auto bootstrap = CreateQueueAgentBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run()
            .Get()
            .ThrowOnError();
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

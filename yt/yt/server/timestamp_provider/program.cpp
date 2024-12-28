#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NTimestampProvider {

////////////////////////////////////////////////////////////////////////////////

class TTimestampProviderProgram
    : public TServerProgram<TTimestampProviderProgramConfig>
{
public:
    TTimestampProviderProgram()
    {
        SetMainThreadName("TSProviderProg");
    }

private:
    void DoStart() final
    {
        auto bootstrap = CreateTimestampProviderBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run()
            .Get()
            .ThrowOnError();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunTimestampProviderProgram(int argc, const char** argv)
{
    TTimestampProviderProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTimestampProvider

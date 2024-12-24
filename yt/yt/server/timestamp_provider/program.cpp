#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NTimestampProvider {

////////////////////////////////////////////////////////////////////////////////

class TTimestampProviderProgram
    : public TServerProgram<TTimestampProviderConfig>
{
public:
    TTimestampProviderProgram()
    {
        SetMainThreadName("TSProvider");
    }

private:
    void DoStart() final
    {
        auto* bootstrap = CreateBootstrap(GetConfig()).release();
        DoNotOptimizeAway(bootstrap);
        bootstrap->Initialize();
        bootstrap->Run();
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

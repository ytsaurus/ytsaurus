#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NOffshoreDataGateway {

////////////////////////////////////////////////////////////////////////////////

class TOffshoreDataGatewayProgram
    : public TServerProgram<TOffshoreDataGatewayProgramConfig>
{
public:
    TOffshoreDataGatewayProgram()
    {
        SetMainThreadName("ODGProg");
    }

private:
    void DoStart() final
    {
        auto bootstrap = CreateOffshoreDataGatewayBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run()
            .Get()
            .ThrowOnError();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunOffshoreDataGatewayProgram(int argc, const char** argv)
{
    TOffshoreDataGatewayProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway

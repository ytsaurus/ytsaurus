#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NKafkaProxy {

////////////////////////////////////////////////////////////////////////////////

class TProxyProgram
    : public TServerProgram<TProxyProgramConfig>
{
public:
    TProxyProgram()
    {
        SetMainThreadName("KafkaProxyProg");
    }

private:
    void DoStart() final
    {
        auto bootstrap = CreateKafkaProxyBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run()
            .Get()
            .ThrowOnError();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunKafkaProxyProgram(int argc, const char** argv)
{
    TProxyProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy

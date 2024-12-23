#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NKafkaProxy {

////////////////////////////////////////////////////////////////////////////////

class TKafkaProxyProgram
    : public TServerProgram<TKafkaProxyConfig>
{
public:
    TKafkaProxyProgram()
    {
        SetMainThreadName("KafkaProxy");
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

void RunKafkaProxyProgram(int argc, const char** argv)
{
    TKafkaProxyProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy

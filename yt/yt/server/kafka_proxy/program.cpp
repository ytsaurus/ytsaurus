#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/ytlib/program/native_singletons.h>

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
        auto config = GetConfig();

        ConfigureNativeSingletons(config);

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = CreateBootstrap(std::move(config)).release();
        DoNotOptimizeAway(bootstrap);
        bootstrap->Initialize();
        bootstrap->Run();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunKafkaProxyProgram(int argc, const char** argv)
{
    TKafkaProxyProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy

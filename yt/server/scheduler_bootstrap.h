#pragma once

#include <yt/ytlib/misc/configurable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerBootstrap
{
public:
    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        int RpcPort;
        int MonitoringPort;

        TConfig()
        {
            Register("rpc_port", RpcPort).Default(9000);
            Register("monitoring_port", MonitoringPort).Default(10000);
        }
    };

    TSchedulerBootstrap(
        const Stroka& configFileName,
        TConfig* config);

    void Run();

private:
    Stroka ConfigFileName;
    TConfig::TPtr Config;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

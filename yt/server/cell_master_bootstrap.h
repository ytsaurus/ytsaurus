#pragma once

#include <yt/ytlib/misc/configurable.h>
#include <yt/ytlib/meta_state/persistent_state_manager.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCellMasterBootstrap
{
public:
    //! Describes a configuration of TCellMaster.
    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        //! Meta state configuration.
        NMetaState::TPersistentStateManagerConfig::TPtr MetaState;

        //! RPC interface port number.
        int RpcPort;

        //! HTTP monitoring interface port number.
        int MonitoringPort;

        TConfig()
        {
            Register("meta_state", MetaState);
            Register("rpc_port", RpcPort).Default(9000);
            Register("monitoring_port", MonitoringPort).Default(10000);
        }
    };

    TCellMasterBootstrap(
        const Stroka& configFileName,
        TConfig* config);

    void Run();

private:
    Stroka ConfigFileName;
    TConfig::TPtr Config;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

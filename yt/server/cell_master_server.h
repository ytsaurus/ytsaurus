#pragma once

#include <yt/ytlib/misc/config.h>
#include <yt/ytlib/meta_state/persistent_state_manager.h>
#include <yt/ytlib/monitoring/http_tree_server.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCellMasterServer
{
public:
    //! Describes a configuration of TCellMaster.
    struct TConfig
        : TConfigBase
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

    TCellMasterServer(
        const Stroka& configFileName,
        TConfig* config);

    void Run();

private:
    Stroka ConfigFileName;
    TConfig::TPtr Config;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

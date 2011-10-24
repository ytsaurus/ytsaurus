#pragma once

#include <yt/ytlib/meta_state/meta_state_manager.h>
#include <yt/ytlib/monitoring/http_tree_server.h>

namespace NYT {

using NMetaState::TMetaStateManager;

using NMonitoring::THttpTreeServer;

////////////////////////////////////////////////////////////////////////////////

class TCellMasterServer
{
public:
    //! Describes a configuration of TCellMaster.
    struct TConfig
    {
        //! Meta state configuration.
        TMetaStateManager::TConfig MetaState;

        int MonitoringPort;

        TConfig()
            : MonitoringPort(10000)
        { }

        //! Reads configuration from JSON.
        void Read(TJsonObject* json);
    };

    TCellMasterServer(const TConfig& config);

    void Run();

private:
    TConfig Config;
    THttpTreeServer* MonitoringServer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#pragma once

#include <yt/ytlib/meta_state/meta_state_manager.h>
#include <yt/ytlib/monitoring/http_tree_server.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCellMasterServer
{
public:
    //! Describes a configuration of TCellMaster.
    struct TConfig
    {
        //! Meta state configuration.
        NMetaState::IMetaStateManager::TConfig MetaState;

        int MonitoringPort;

        // TODO: killme
        Stroka NewConfigFileName;

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

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

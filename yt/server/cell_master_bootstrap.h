#pragma once

#include <yt/ytlib/misc/configurable.h>
#include <yt/ytlib/meta_state/persistent_state_manager.h>
#include <yt/ytlib/transaction_server/transaction_manager.h>

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

        //! A number identifying the cell in the whole world.
        ui16 CellId;
        
        //! Meta state configuration.
        NMetaState::TPersistentStateManagerConfig::TPtr MetaState;
        
        NTransactionServer::TTransactionManager::TConfig::TPtr TransactionManager;

        //! RPC interface port number.
        int RpcPort;

        //! HTTP monitoring interface port number.
        int MonitoringPort;

        TConfig()
        {
            Register("cell_id", CellId)
                .Default(0);
            Register("meta_state", MetaState);
            Register("transaction_manager", TransactionManager)
                .DefaultNew();
            Register("rpc_port", RpcPort)
                .Default(9000);
            Register("monitoring_port", MonitoringPort)
                .Default(10000);
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

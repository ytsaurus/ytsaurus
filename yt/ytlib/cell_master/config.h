#pragma once

#include "public.h"

#include <ytlib/meta_state/persistent_state_manager.h>
#include <ytlib/transaction_server/transaction_manager.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

//! Describes a configuration of TCellMaster.
struct TCellMasterConfig
    : public TConfigurable
{
    //! A number identifying the cell in the whole world.
    ui16 CellId;

    //! Meta state configuration.
    NMetaState::TPersistentStateManagerConfig::TPtr MetaState;

    NTransactionServer::TTransactionManager::TConfig::TPtr TransactionManager;

    //! RPC interface port number.
    int RpcPort;

    //! HTTP monitoring interface port number.
    int MonitoringPort;

    TCellMasterConfig()
    {
        Register("cell_id", CellId)
            .Default(0);
        Register("meta_state", MetaState)
            .DefaultNew();
        Register("transaction_manager", TransactionManager)
            .DefaultNew();
        Register("rpc_port", RpcPort)
            .Default(9000);
        Register("monitoring_port", MonitoringPort)
            .Default(10000);
    }
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NCellMaster
} // namespace NYT

#pragma once

#include "public.h"

#include <ytlib/meta_state/persistent_state_manager.h>
// TODO(babenko): replace with config.h
#include <ytlib/transaction_server/transaction_manager.h>
#include <ytlib/chunk_server/config.h>
#include <ytlib/object_server/config.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

//! Describes a configuration of TCellMaster.
struct TCellMasterConfig
    : public TConfigurable
{
    //! Meta state configuration.
    NMetaState::TPersistentStateManagerConfig::TPtr MetaState;

    NTransactionServer::TTransactionManager::TConfig::TPtr Transactions;

    NChunkServer::TChunkManagerConfig::TPtr Chunks;

    NObjectServer::TObjectManagerConfigPtr Objects;

    //! RPC interface port number.
    int RpcPort;

    //! HTTP monitoring interface port number.
    int MonitoringPort;

    TCellMasterConfig()
    {
        Register("meta_state", MetaState)
            .DefaultNew();
        Register("transactions", Transactions)
            .DefaultNew();
        Register("chunks", Chunks)
            .DefaultNew();
        Register("objects", Objects)
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

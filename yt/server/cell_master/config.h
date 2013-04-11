#pragma once

#include "public.h"

#include <ytlib/meta_state/config.h>

#include <server/node_tracker_server/config.h>

#include <server/transaction_server/config.h>

#include <server/chunk_server/config.h>

#include <server/object_server/config.h>

#include <server/bootstrap/config.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

//! Describes a configuration of TCellMaster.
struct TCellMasterConfig
    : public TServerConfig
{
    //! Meta state configuration.
    NMetaState::TPersistentStateManagerConfigPtr MetaState;

    NNodeTrackerServer::TNodeTrackerConfigPtr NodeTracker;

    NTransactionServer::TTransactionManagerConfigPtr TransactionManager;

    NChunkServer::TChunkManagerConfigPtr ChunkManager;

    NObjectServer::TObjectManagerConfigPtr ObjectManager;

    //! HTTP monitoring interface port number.
    int MonitoringPort;

    TCellMasterConfig()
    {
        Register("meta_state", MetaState)
            .DefaultNew();
        Register("node_tracker", NodeTracker)
            .DefaultNew();
        Register("transaction_manager", TransactionManager)
            .DefaultNew();
        Register("chunk_manager", ChunkManager)
            .DefaultNew();
        Register("object_manager", ObjectManager)
            .DefaultNew();
        Register("monitoring_port", MonitoringPort)
            .Default(10000);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT

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
class TCellMasterConfig
    : public TServerConfig
{
public:
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
        RegisterParameter("meta_state", MetaState)
            .DefaultNew();
        RegisterParameter("node_tracker", NodeTracker)
            .DefaultNew();
        RegisterParameter("transaction_manager", TransactionManager)
            .DefaultNew();
        RegisterParameter("chunk_manager", ChunkManager)
            .DefaultNew();
        RegisterParameter("object_manager", ObjectManager)
            .DefaultNew();
        RegisterParameter("monitoring_port", MonitoringPort)
            .Default(10000);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT

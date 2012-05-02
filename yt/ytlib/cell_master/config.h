#pragma once

#include "public.h"

#include <ytlib/meta_state/config.h>
#include <ytlib/transaction_server/config.h>
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
    NMetaState::TPersistentStateManagerConfigPtr MetaState;

    NTransactionServer::TTransactionManagerConfigPtr Transactions;

    NChunkServer::TChunkManagerConfigPtr Chunks;

    NObjectServer::TObjectManagerConfigPtr Objects;

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
        Register("monitoring_port", MonitoringPort)
            .Default(10000);
    }
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NCellMaster
} // namespace NYT

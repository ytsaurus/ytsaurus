#pragma once

#include <yt/yt/ytlib/transaction_client/public.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TChaosNodeConfig)
DECLARE_REFCOUNTED_STRUCT(TChaosSlotDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TChaosNodeDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TChaosCellSynchronizerConfig)
DECLARE_REFCOUNTED_STRUCT(TReplicationCardObserverConfig)
DECLARE_REFCOUNTED_STRUCT(TMigratedReplicationCardRemoverConfig)
DECLARE_REFCOUNTED_STRUCT(TForeignMigratedReplicationCardRemoverConfig)
DECLARE_REFCOUNTED_STRUCT(TChaosManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TCoordinatorManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TMetadataCacheServiceConfig)
DECLARE_REFCOUNTED_STRUCT(TMetadataCacheConfig)
DECLARE_REFCOUNTED_STRUCT(TTransactionManagerConfig)

using NTransactionClient::TTransactionSignature;
using NTransactionClient::InitialTransactionSignature;
using NTransactionClient::FinalTransactionSignature;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode

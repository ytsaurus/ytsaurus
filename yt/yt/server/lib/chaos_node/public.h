#pragma once

#include <yt/yt/ytlib/transaction_client/public.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TChaosNodeConfig)
DECLARE_REFCOUNTED_CLASS(TChaosCellSynchronizerConfig)
DECLARE_REFCOUNTED_CLASS(TReplicationCardObserverConfig)
DECLARE_REFCOUNTED_CLASS(TMigratedReplicationCardRemoverConfig)
DECLARE_REFCOUNTED_CLASS(TForeignMigratedReplicationCardRemoverConfig)
DECLARE_REFCOUNTED_CLASS(TChaosManagerConfig)
DECLARE_REFCOUNTED_CLASS(TCoordinatorManagerConfig)
DECLARE_REFCOUNTED_CLASS(TMetadataCacheServiceConfig)
DECLARE_REFCOUNTED_CLASS(TMetadataCacheConfig)
DECLARE_REFCOUNTED_CLASS(TTransactionManagerConfig)

using NTransactionClient::TTransactionSignature;
using NTransactionClient::InitialTransactionSignature;
using NTransactionClient::FinalTransactionSignature;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode

#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

struct TAlienCellDescriptorLite;
struct TAlienPeerDescriptor;
struct TAlienCellDescriptor;

DECLARE_REFCOUNTED_STRUCT(IChaosCellDirectorySynchronizer)
DECLARE_REFCOUNTED_STRUCT(IChaosObjectChannelFactory)
DECLARE_REFCOUNTED_STRUCT(IChaosCellChannelFactory)
DECLARE_REFCOUNTED_STRUCT(IChaosResidencyCache)
DECLARE_REFCOUNTED_STRUCT(IBannedReplicaTracker)
DECLARE_REFCOUNTED_STRUCT(IBannedReplicaTrackerCache)
DECLARE_REFCOUNTED_STRUCT(IReplicationCardUpdatesBatcher)
DECLARE_REFCOUNTED_STRUCT(TChaosReplicationCardUpdatesBatcherConfig)
DECLARE_REFCOUNTED_STRUCT(TChaosReplicationCardUpdatesBatcherDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(IReplicationCardsWatcher)
DECLARE_REFCOUNTED_STRUCT(IReplicationCardsWatcherClient)
DECLARE_REFCOUNTED_STRUCT(IReplicationCardWatcherCallbacks)
DECLARE_REFCOUNTED_STRUCT(TChaosCellDirectorySynchronizerConfig)
DECLARE_REFCOUNTED_STRUCT(TChaosResidencyCacheConfig)
DECLARE_REFCOUNTED_STRUCT(TChaosResidencyCacheDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TChaosObjectChannelConfig)
DECLARE_REFCOUNTED_STRUCT(TReplicationCardsWatcherConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

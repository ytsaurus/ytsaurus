#pragma once

#include "public.h"
#include "automaton.h"

#include <yt/server/master/chunk_server/public.h>

#include <yt/server/master/cypress_server/public.h>

#include <yt/server/lib/hydra/composite_automaton.h>

#include <yt/server/master/node_tracker_server/public.h>

#include <yt/server/master/object_server/public.h>

#include <yt/server/master/security_server/public.h>

#include <yt/server/master/table_server/public.h>

#include <yt/server/master/tablet_server/public.h>

#include <yt/server/master/transaction_server/public.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMasterSnapshotVersion,
    ((OldVersion711)                                                 (711))
    ((OldVersion712)                                                 (712))
    ((AddTabletCellDecommission)                                     (713))
    ((ChangeTReqKickOrphanedTabletActions)                           (714))
    ((FixTabletErrorCountLag)                                        (715))
    ((AddDynamicTabletCellOptions)                                   (716))
    ((AddReplicatedTableOptions)                                     (717))
    ((WeakGhostsSaveLoad)                                            (718))
    ((MulticellForDynamicTables)                                     (800))
    ((MakeTabletStateBackwardCompatible)                             (801))
    ((AddReplicaOptions)                                             (802))
    ((AddPrimaryLastMountTransactionId)                              (803))
    ((RemoveTTransactionSystem)                                      (804))
    ((AddCypressAnnotations)                                         (805))
    ((SameAsVer718ButIn19_4)                                         (806))
    ((AddTabletCellHealthToTabletCellStatistics)                     (807))
    ((ForwardStartPrerequisiteTransactionToSecondaryMaster)          (808))
    ((PersistRequisitionUpdateRequests)                              (809))
    ((PersistTransactionDeadline)                                    (810))
    ((AddAttributesRevisionContentRevision)                          (811))
    ((AddReassignPeerMutation)                                       (812))
    ((YT_9775_MasterMasterProtocolChange)                            (813))
    ((OldVersion814)                                                 (814))
    ((AddReadRequestRateLimitAndWriteRequestRateLimit)               (815))
    ((InitializeMediumSpecificMaxReplicationFactor)                  (816))
    ((PersistTNodeResourceUsageLimits)                               (817))
    ((IntToI64ForNSecurityServerTClusterResourcesNodeAndChunkCount)  (818))
    ((AddTabletCellLifeStage)                                        (819))
    ((FixSnapshot)                                                   (820))
    ((PerTableTabletBalancerConfig)                                  (821))
    ((UseCurrentMountTransactionIdToLockTableNodeDuringMount)        (822))
    ((SynchronousHandlesForTabletBalancer)                           (823))
    ((RemoveDynamicTableAttrsFromStaticTables)                       (824))
    ((InTChunkReplicationReplaceArrayWithSmallVector)                (825))
    ((ColumnarAcls)                                                  (826))
    ((SecurityTags)                                                  (827))
    ((TCumulativeStatisticsInChunkLists)                             (828))
    ((MultiplyTUserReadRequestRateLimitByTheNumberOfFollowers)       (829))
    ((ChunkView)                                                     (830))
    ((VersionedExpirationTime)                                       (831))
    ((TClusterResourcesDiskSpaceSerialization)                       (832))
    ((YT_10852)                                                      (833))
    ((TTabletCellBundleHealthAdded)                                  (834))
    ((SnapshotLockableMapNodes)                                      (835))
    ((YT_10952_DelayedMembershipClosureRecomputation)                (836))
    ((YT_10726_StagedChunkExpiration)                                (837))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

#define SERIALIZE_INL_H_
#include "serialize-inl.h"
#undef SERIALIZE_INL_H_

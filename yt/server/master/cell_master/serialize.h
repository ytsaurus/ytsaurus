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

NHydra::TReign GetCurrentReign();
bool ValidateSnapshotReign(NHydra::TReign);
NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign reign);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMasterReign,
    ((OldVersion711)                                                 (711))  // shakurov
    ((OldVersion712)                                                 (712))  // aozeritsky
    ((AddTabletCellDecommission)                                     (713))  // savrus
    ((ChangeTReqKickOrphanedTabletActions)                           (714))  // savrus
    ((FixTabletErrorCountLag)                                        (715))  // ifsmirnov
    ((AddDynamicTabletCellOptions)                                   (716))  // savrus
    ((AddReplicatedTableOptions)                                     (717))  // aozeritsky
    ((WeakGhostsSaveLoad)                                            (718))  // shakurov
    ((MulticellForDynamicTables)                                     (800))  // savrus
    ((MakeTabletStateBackwardCompatible)                             (801))  // savrus
    ((AddReplicaOptions)                                             (802))  // aozeritsky
    ((AddPrimaryLastMountTransactionId)                              (803))  // savrus
    ((RemoveTTransactionSystem)                                      (804))  // shakurov
    ((AddCypressAnnotations)                                         (805))  // psushin
    ((SameAsVer718ButIn19_4)                                         (806))  // shakurov
    ((AddTabletCellHealthToTabletCellStatistics)                     (807))  // savrus
    ((ForwardStartPrerequisiteTransactionToSecondaryMaster)          (808))  // savrus
    ((PersistRequisitionUpdateRequests)                              (809))  // shakurov
    ((PersistTransactionDeadline)                                    (810))  // ignat
    ((AddAttributesRevisionContentRevision)                          (811))  // aozeritsky
    ((AddReassignPeerMutation)                                       (812))  // savrus
    ((YT_9775_MasterMasterProtocolChange)                            (813))  // aozeritsky
    ((OldVersion814)                                                 (814))  // aozeritsky
    ((AddReadRequestRateLimitAndWriteRequestRateLimit)               (815))  // aozeritsky
    ((InitializeMediumSpecificMaxReplicationFactor)                  (816))  // shakurov
    ((PersistTNodeResourceUsageLimits)                               (817))  // shakurov
    ((IntToI64ForNSecurityServerTClusterResourcesNodeAndChunkCount)  (818))  // shakurov
    ((AddTabletCellLifeStage)                                        (819))  // savrus
    ((FixSnapshot)                                                   (820))  // savrus
    ((PerTableTabletBalancerConfig)                                  (821))  // ifsmirnov
    ((UseCurrentMountTransactionIdToLockTableNodeDuringMount)        (822))  // savrus
    ((SynchronousHandlesForTabletBalancer)                           (823))  // ifsmirnov
    ((RemoveDynamicTableAttrsFromStaticTables)                       (824))  // savrus
    ((InTChunkReplicationReplaceArrayWithSmallVector)                (825))  // shakurov
    ((ColumnarAcls)                                                  (826))  // babenko
    ((SecurityTags)                                                  (827))  // babenko
    ((TCumulativeStatisticsInChunkLists)                             (828))  // ifsmirnov
    ((MultiplyTUserReadRequestRateLimitByTheNumberOfFollowers)       (829))  // shakurov
    ((ChunkView)                                                     (830))  // ifsmirnov
    ((VersionedExpirationTime)                                       (831))  // shakurov
    ((TClusterResourcesDiskSpaceSerialization)                       (832))  // aozeritsky
    ((YT_10852)                                                      (833))  // shakurov
    ((TTabletCellBundleHealthAdded)                                  (834))  // aozeritsky
    ((SnapshotLockableMapNodes)                                      (835))  // shakurov
    ((YT_10952_DelayedMembershipClosureRecomputation)                (836))  // babenko
    ((YT_10726_StagedChunkExpiration)                                (837))  // shakurov
    ((ChunkViewToParentsArray)                                       (838))  // ifsmirnov
    ((FixTableStatistics)                                            (839))  // savrus
    ((BranchedAndLockedNodeCountMismatchFix_19_6)                    (840))  // shakurov
    ((YT_10639_CumulativeStatisticsInDynamicTables)                  (899))  // ifsmirnov
    ((PortalsInitial)                                                (900))  // babenko
    ((CypressShards)                                                 (901))  // babenko
    ((BulkInsert)                                                    (902))  // savrus
    ((TransactionDepth)                                              (904))  // babenko
    ((MorePortalAttributes)                                          (905))  // babenko
    ((CypressShardName)                                              (906))  // babenko
    ((YT_10855_EpochHistoryManager)                                  (907))  // ifsmirnov
    ((AddReplicatedTableCopy)                                        (908))  // avmatrosov
    ((YT_11349_FixCypressMoveWithEscapedSymbols)                     (910))  // kiselyovp
    ((AddRefsFromTransactionToUsageAccounts)                         (911))  // babenko
    ((ExternalizedTransactions)                                      (912))  // babenko
    ((SyncCellsBeforeRemoval)                                        (913))  // babenko
    ((RequestLimits)                                                 (914))  // aozeritsky
    ((TwoSidedPortalRemoval)                                         (915))  // babenko
    ((YT_10745_Annotation)                                           (916))  // avmatrosov
    ((ReplicateConfigToNewCellFirst)                                 (917))  // babenko
    ((FixSetShardInClone)                                            (918))  // babenko
    ((DropUserStatistics)                                            (919))  // babenko
    ((FixClusterNodeForeignFlag)                                     (920))  // babenko
    ((BranchedAndLockedNodeCountMismatchFix)                         (921))  // shakurov
    ((FixParentTxForConcatUpload)                                    (922))  // babenko
    ((DynamicMasterCacheDiscovery)                                  (1001))  // aleksandra-zh
    ((YT_11927_FixResolve)                                          (1002))  // babenko
    ((CellServer)                                                   (1100))  // savrus
    ((EnableDelayedMembershipClosureRecomputationByDefault)         (1101))  // babenko
    ((NetworkProject)                                               (1102))  // gritukan
    ((DynamicPeerCount)                                             (1103))  // gritukan
    ((UserManagedPools)                                             (1104))  // renadeen
    ((GranularCypressTreeCopy)                                      (1105))  // gritukan
    ((NoTabletErrorsOnMaster)                                       (1106))  // ifsmirnov
    ((TwoLevelMasterCache)                                          (1107))  // aleksandra-zh
    ((DestroyedChunkRemoval)                                        (1108))  // aleksandra-zh
    ((SpecifiedAttributeFix)                                        (1109))  // shakurov
    ((DestroyedChunkRemovalFix)                                     (1110))  // aleksandra-zh
    ((YT_12145_FixReplicatedTablesCopy_19_8)                        (1111))  // babenko
    ((YT_12139_FixDoublePrepare)                                    (1112))  // babenko
    ((TruncateJournals)                                             (1200))  // aleksandra-zh
    ((PrevRandomSeed)                                               (1201))  // aleksandra-zh
    ((FixDenseMapSerialization)                                     (1202))  // aleksandra-zh
    ((YT_12145_FixReplicatedTablesCopy)                             (1203))  // babenko
    ((HierarchicalAccounts)                                         (1204))  // kiselyovp
    ((FixDoBranch)                                                  (1205))  // aleksandra-zh
    ((MasterMemoryUsageAccounting)                                  (1206))  // aleksandra-zh
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

#define SERIALIZE_INL_H_
#include "serialize-inl.h"
#undef SERIALIZE_INL_H_

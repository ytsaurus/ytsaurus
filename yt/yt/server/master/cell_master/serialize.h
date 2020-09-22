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
    ((FixDoBranch_19_8)                                             (1113))  // aleksandra-zh
    ((YT_11951_FixMountLock)                                        (1114))  // savrus
    ((FixNetworkProjectSerialization_19_8)                          (1115))  // gritukan
    ((BeginUploadConcatenateFixes_19_8)                             (1116))  // shakurov
    ((TuneTabletStatisticsUpdate_19_8)                              (1117))  // savrus
    ((InternalizeAbcSchedulerPoolAttribute_19_8)                    (1118))  // mrkastep
    ((FixTabletStatisticsUpdate_19_8)                               (1119))  // savrus
    ((YT_13015_CorrectSrcTxForCrossShardCopy_19_8)                  (1120))  // shakurov
    ((TruncateJournals)                                             (1200))  // aleksandra-zh
    ((PrevRandomSeed)                                               (1201))  // aleksandra-zh
    ((FixDenseMapSerialization)                                     (1202))  // aleksandra-zh
    ((YT_12145_FixReplicatedTablesCopy)                             (1203))  // babenko
    ((HierarchicalAccounts)                                         (1204))  // kiselyovp
    ((FixDoBranch)                                                  (1205))  // aleksandra-zh
    ((MasterMemoryUsageAccounting)                                  (1206))  // aleksandra-zh
    ((InitializeAccountMasterMemoryUsage)                           (1207))  // aleksandra-zh
    ((YT_11279_UnmountForceOnlySuperuser)                           (1208))  // lexolordan
    ((FixNetworkProjectSerialization)                               (1209))  // gritukan
    ((DisableMasterMemoryUsageAccountOvercommitValidation)          (1300))  // aleksandra-zh
    ((FasterTError)                                                 (1301))  // babenko
    ((FixRootAccountLimits)                                         (1302))  // aleksandra-zh
    ((SwitchToAlterTableReplica)                                    (1303))  // babenko
    ((DynamicTimestampProviderDiscovery)                            (1304))  // aleksandra-zh
    ((YT_12365_FixCalculatingStaticMemoryInMounting)                (1305))  // lexolordan
    ((IgnoreTypeMismatch)                                           (1306))  // gritukan
    ((FixTErrorSerialization)                                       (1307))  // ifsmirnov
    ((DynamicTabletSlotCount)                                       (1308))  // gritukan
    ((DynamicStoreRead)                                             (1309))  // ifsmirnov
    ((BeginUploadConcatenateFixes)                                  (1310))  // shakurov
    ((CellPeerRevocationReason)                                     (1311))  // babenko
    ((ErasureJournals)                                              (1312))  // babenko
    ((CellReconfigurationFixes)                                     (1313))  // akozhikhov
    ((InternalizeAbcSchedulerPoolAttribute)                         (1314))  // mrkastep
    ((AggregateTabletStatistics)                                    (1315))  // ifsmirnov
    ((TuneTabletStatisticsUpdate_20_2)                              (1316))  // savrus
    ((ApproximateColumnarStatistics)                                (1317))  // gritukan
    ((BetterClusterResourcesDeserialization)                        (1318))  // kiselyovp
    ((TransferQuota)                                                (1319))  // kiselyovp
    ((RenameTransferQuota)                                          (1320))  // kiselyovp
    ((YT_13015_CorrectSrcTxForCrossShardCopy)                       (1321))  // shakurov
    ((AclCheckWorkaroundForMutatingRequests_20_2)                   (1322))  // shakurov
    ((MountHint)                                                    (1323))  // ifsmirnov
    ((YTINCIDENTS_56_SyncOnPrepare)                                 (1324))  // babenko
    ((MakeProfilingModeAnInheritedAttribute_20_2)                   (1325))  // akozhikhov
    ((FixInheritanceOfProfilingModeForStaticTables)                 (1326))  // akozhikhov
    ((AddSnapshotErasureCodec)                                      (1327))  // babenko
    ((AllowProfilingModeModificationUnderTx)                        (1328))  // akozhikhov
    ((ForbidReshardWhenTableIsLockedByTransaction)                  (1329))  // savrus
    ((FixReshardNonEmptyReplicatedTable)                            (1330))  // ifsmirnov
    ((DisallowSettingBundleOfMountedTables)                         (1331))  // ifsmirnov
    ((NonAliveTxInCloneForeignNode)                                 (1332))  // babenko
    ((ExtraPeerDroppingDelay)                                       (1333))  // gritukan
    // 20.3
    ((SubjectAliases)                                               (1400))  // s-v-m
    ((OpaquePortalEntrances)                                        (1401))  // shakurov
    ((MultisetAttributes)                                           (1402))  // gritukan
    ((MakeProfilingModeAnInheritedAttribute_20_3)                   (1403))  // akozhikhov
    ((YT_13003_SeparateScannersForJournalAndBlobChunks)             (1404))  // shakurov
    ((FixInheritanceOfProfilingMode)                                (1406))  // akozhikhov
    ((YT_13126_ExpirationTimeout)                                   (1407))  // shakurov
    ((YT_12198_LockTimes)                                           (1408))  // babenko
    ((ShardedTransactions)                                          (1409))  // shakurov
    ((FixErrorDatetime)                                             (1410))  // babenko
    ((YT_11903_PreserveCreationTimeInMove)                          (1411))  // babenko
    ((PartitionedTables)                                            (1412))  // max42
    ((DegradedCellsAreHealthy)                                      (1413))  // babenko
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

#define SERIALIZE_INL_H_
#include "serialize-inl.h"
#undef SERIALIZE_INL_H_

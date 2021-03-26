#pragma once

#include "public.h"
#include "automaton.h"

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/lib/hydra/composite_automaton.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/security_server/public.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/master/tablet_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

NHydra::TReign GetCurrentReign();
bool ValidateSnapshotReign(NHydra::TReign);
NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign reign);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMasterReign,
    // 20.2
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
    ((CellPeerLastSeenState)                                        (1334))  // gritukan
    ((ReservedAttributes)                                           (1335))  // ifsmirnov
    ((RecognizeSchedulerPoolCustomAttributesOnLoad)                 (1336))  // renadeen
    ((OptOutTabletDynamicMemoryLimit)                               (1337))  // ifsmirnov
    ((EnableForcedRotationBackingMemoryAccounting_20_2)             (1338))  // babenko
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
    ((FixTrunkNodeInvalidDeltaStatistics)                           (1414))  // shakurov
    ((NestPoolTreeConfig)                                           (1415))  // renadeen
    ((OptionalDedicatedUploadTxObjectTypes)                         (1416))  // shakurov
    ((YT_13127_CompositeNodeExpiration)                             (1417))  // shakurov
    ((CombineStateHash)                                             (1418))  // aleksandra-zh
    ((EnableForcedRotationBackingMemoryAccounting)                  (1419))  // babenko
    ((RemovePartitionedTables)                                      (1420))  // max42
    ((OverlayedJournals)                                            (1421))  // babenko
    ((YT_12193_BetterAlterTable)                                    (1422))  // ermolovd
    ((YT_12559_AbortStuckExternalizedTransactions)                  (1423))  // shakurov
    ((DedicatedUploadTransactionTypesByDefault)                     (1424))  // shakurov
    ((FixClusterStatisticsMasterMemoryUsage)                        (1425))  // aleksandra-zh
    ((OldTxReplicationHiveProtocolCompatibility)                    (1426))  // shakurov
    ((IgnoreStatisticsDuringNodeRegistration)                       (1427))  // gritukan
    ((FixChunkTreeAttachValidation)                                 (1428))  // babenko
    ((MutationIdempotizerToggle)                                    (1429))  // shakurov
    ((FixChunkSealValidation)                                       (1430))  // babenko
    ((SupportIsaReedSolomon63_20_3)                                 (1431))  // akozhikhov
    ((TabletCellStatusGossipPeriod)                                 (1432))  // gritukan
    ((CorrectMergeBranchSemanticsForAttributes)                     (1433))  // shakurov
    ((IncrementalCellStatusGossip)                                  (1434))  // gritukan
    ((RemoveTypeV2)                                                 (1435))  // ermolovd
    ((EnableChangelogChunkPreallocationInBundleOptions)             (1436))  // babenko
    ((CapTrimmedRowCount)                                           (1437))  // ifsmirnov
    ((RevertRemoveTypeV2_20_3_Only)                                 (1438))  // ermolovd
    ((BannedReplicaClusterList)                                     (1439))  // akozhikhov
    ((ReplicationLagInRTT)                                          (1440))  // akozhikhov
    ((ClearSetBeforeDeserialization)                                (1441))  // eshcherbin
    // Late 20.3 starts here.
    ((OrderedRemoteDynamicStoreReader)                              (1444))  // ifsmirnov
    ((VersionedRemoteCopy)                                          (1445))  // ifsmirnov
    ((ChunkFeatures)                                                (1446))  // gritukan
    ((IncreaseUploadReplicationFactorUponFlush)                     (1447))  // akozhikhov
    ((BundleQuotas)                                                 (1448))  // ifsmirnov
    ((VerifySchedulerPoolStrongGuarantees)                          (1449))  // eshcherbin
    ((DoNotUseThisReignItConfictsWithReservedAttributes)            (1450))  // babenko
    ((HierarchicalIntegralLimits)                                   (1451))  // renadeen
    ((MigrateMinShareResourcesToStrongGuaranteeResources)           (1452))  // renadeen
    ((ForcedChunkViewCompactionRevision)                            (1453))  // ifsmirnov
    ((InternedAttributes)                                           (1454))  // babenko
    ((HierarchicalIntegralLimitsFix)                                (1455))  // renadeen
    ((BatchedReplicateTransactionMethod)                            (1456))  // shakurov
    ((PhysicalRowCount)                                             (1457))  // gritukan
    ((DropSealedFromChunkTreeStatistics)                            (1458))  // gritukan
    ((RegisteredLocationUuids)                                      (1459))  // aleksandra-zh
    ((MigrateMinShareResourcesToStrongGuaranteeResources2)          (1460))  // renadeen
    ((ValidateNoDuplicateLocationUuidsFromSameNode)                 (1461))  // babenko
    ((DropHealthFromTabletCellStatistics)                           (1462))  // akozhikhov
    ((TabletIdsForFinishedTabletActions)                            (1463))  // ifsmirnov
    // 21.1 starts here.
    ((SlotLocationStatisticsInNodeNode)                             (1500))  // gritukan
    ((EnableDescendingSortOrder)                                    (1501))  // max42
    ((RowBufferEmptyRowDeserialization)                             (1502))  // max42
    ((RemoveOldCellTracker)                                         (1503))  // gritukan
    ((PerCellPerRoleMasterMemoryLimit)                              (1504))  // aleksandra-zh
    ((InitializeAccountChunkHostMasterMemory)                       (1505))  // aleksandra-zh
    ((Hunks)                                                        (1506))  // babenko
    ((EnableMasterCacheDiscoveryByDefault)                          (1507))  // aleksandra-zh
    ((CellDescriptorMap)                                            (1508))  // aleksandra-zh
    ((MasterAlerts)                                                 (1509))  // gritukan
    ((NodeFlavors)                                                  (1510))  // gritukan
    ((ProxyRoles)                                                   (1511))  // gritukan
    ((ReconfigurableMasterSingletons)                               (1512))  // gritukan
    ((FixMasterMemoryCompats)                                       (1513))  // aleksandra-zh
    ((PullParserDeserialization)                                    (1514))  // levysotsky
    ((UuidType)                                                     (1515))  // ermolovd
    ((MoveTableStatisticsGossipToTableManager)                      (1516))  // shakurov
    ((NativeContentRevision)                                        (1517))  // shakurov
    ((CellarHeartbeat)                                              (1518))  // savrus
    ((RemoveClusterNodeFlavor)                                      (1519))  // gritukan
    // 21.2 starts here.
    ((MasterMergeJobs)                                              (1600))  // aleksandra-zh
);

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NHydra::TSaveContext
{
public:
    using TSavedSchemaMap = THashMap<NTableServer::TSharedTableSchema*, NObjectClient::TVersionedObjectId>;
    DEFINE_BYREF_RW_PROPERTY(TSavedSchemaMap, SavedSchemas);

public:
    TEntitySerializationKey RegisterInternedYsonString(NYson::TYsonString str);

    EMasterReign GetVersion();

private:
    using TYsonStringMap = THashMap<NYson::TYsonString, TEntitySerializationKey>;
    TYsonStringMap InternedYsonStrings_;
};

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public NHydra::TLoadContext
{
public:
    using TLoadedSchemaMap = THashMap<
        NObjectClient::TVersionedObjectId,
        NTableServer::TSharedTableSchema*,
        NObjectClient::TDirectVersionedObjectIdHash>;
    DEFINE_BYVAL_RO_PROPERTY(TBootstrap*, Bootstrap);
    DEFINE_BYREF_RW_PROPERTY(TLoadedSchemaMap, LoadedSchemas);

public:
    explicit TLoadContext(TBootstrap* bootstrap);

    NObjectServer::TObject* GetWeakGhostObject(NObjectServer::TObjectId id) const;

    template <class T>
    const TInternRegistryPtr<T>& GetInternRegistry() const;

    NYson::TYsonString GetInternedYsonString(TEntitySerializationKey key);
    TEntitySerializationKey RegisterInternedYsonString(NYson::TYsonString str);

    EMasterReign GetVersion();

private:
    std::vector<NYson::TYsonString> InternedYsonStrings_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

#define SERIALIZE_INL_H_
#include "serialize-inl.h"
#undef SERIALIZE_INL_H_

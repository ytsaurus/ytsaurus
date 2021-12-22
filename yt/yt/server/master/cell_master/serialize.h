#pragma once

#include "public.h"
#include "automaton.h"

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/lib/hydra_common/composite_automaton.h>

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
    // 21.2 starts here.
    ((MasterMergeJobs)                                              (1600))  // aleksandra-zh
    ((ChunkCounterInMasterMergeJobsIsNoMore)                        (1601))  // babenko
    ((DoNotThrottleRoot)                                            (1602))  // aleksandra-zh
    ((BuiltinEnableSkynetSharing)                                   (1603))  // aleksandra-zh
    ((RefactorTError)                                               (1604))  // babenko
    ((ProperStoreWriterDefaults)                                    (1605))  // babenko
    ((DetailedMasterMemory)                                         (1606))  // aleksandra-zh
    ((IgnoreExistingForPortalExit)                                  (1607))  // s-v-m
    ((ProfilingModePathLetters)                                     (1608))  // prime
    ((FixChunkMergerPersistence)                                    (1609))  // gritukan
    ((CellNamesInUserLimits)                                        (1610))  // aleksandra-zh
    ((WaitUnmountBeforeTabletCellDecommission)                      (1611))  // savrus
    ((ReplicaLagLimit)                                              (1612))  // gritukan
    ((InheritEnableChunkMerger)                                     (1613))  // aleksandra-zh
    ((FlagForDetailedProfiling)                                     (1614))  // akozhikhov
    ((HunksReshard)                                                 (1615))  // babenko
    ((HunkCompaction)                                               (1616))  // babenko
    ((ChaosCells)                                                   (1617))  // savrus
    ((MakeAbcFolderIdBuiltin)                                       (1618))  // cookiedoth
    ((NoAggregateForHunkColumns)                                    (1619))  // babenko
    ((HunksNotInTabletStatic)                                       (1620))  // ifsmirnov
    ((TrueTableSchemaObjects)                                       (1621))  // shakurov
    ((ChunkFormat)                                                  (1622))  // gritukan
    ((HunksAlter)                                                   (1623))  // babenko
    ((RefBuiltinEmptySchema)                                        (1624))  // shakurov
    ((DoNotMergeDynamicTables)                                      (1625))  // aleksandra-zh
    ((AccountResourceUsageLease)                                    (1626))  // ignat
    ((FixZombieSchemaLoading)                                       (1627))  // shakurov
    ((UpdateTransactionChunkUsageAfterUnstage)                      (1628))  // cookiedoth
    ((FixTablesWithNullTabletCellBundle)                            (1629))  // shakurov
    ((ChangeDynamicTableMedium)                                     (1630))  // ifsmirnov
    ((MinTabletCountForTabletBalancer)                              (1631))  // ifsmirnov
    ((SocratesReservedReign1)                                       (1632))  // aleksandra-zh
    ((SocratesReservedReign2)                                       (1633))  // aleksandra-zh
    ((AccessLogImprovement)                                         (1634))  // cookiedoth
    ((SanitizeUnrecognizedOptionsAlert)                             (1635))  // gritukan
    ((Areas)                                                        (1636))  // savrus
    ((ChaosCellMaps)                                                (1637))  // savrus
    ((AllyReplicas)                                                 (1638))  // ifsmirnov
    ((PersistentCellStatistics)                                     (1639))  // ifsmirnov
    ((LimitObjectSubtreeSize)                                       (1640))  // cookiedoth
    ((PeriodicCompactionMode)                                       (1641))  // ifsmirnov
    ((SpecializedReplicasData)                                      (1642))  // gritukan
    ((DropProtosFromChunk)                                          (1643))  // gritukan
    ((CopyDynamicTableAttributes)                                   (1644))  // ifsmirnov
    ((AutomatonThreadBucketWeights)                                 (1645))  // gritukan
    ((CellIdsInReshardTabletActions)                                (1646))  // ifsmirnov
    ((YT_15179)                                                     (1647))  // shakurov
    ((SchemaIdUponMount)                                            (1648))  // akozhikhov
    ((MulticellStatisticsForAllyReplicas)                           (1649))  // ifsmirnov
    ((RecomputeApprovedReplicaCount)                                (1650))  // ifsmirnov
    ((DropDanglingChunkViews)                                       (1651))  // ifsmirnov
    ((SyncAlienCells)                                               (1652))  // savrus
    ((ErasureInMemory)                                              (1653))  // akozhikhov
    ((FixPreserveOwnerUnderTx)                                      (1654))  // aleksandra-zh
    ((XdeltaAggregation)                                            (1655))  // leasid
    ((RefHunkChunks)                                                (1656))  // babenko
    ((PersistNodesBeingMerged)                                      (1657))  // aleksandra-zh
    ((RescheduleMergeOnLeaderActive)                                (1658))  // aleksandra-zh
    ((InitializeAccountChunkHostMasterMemory2)                      (1659))  // aleksandra-zh
    ((JournalTruncateFixes)                                         (1660))  // aleksandra-zh
    ((PreloadPendingStoreCountBulkInsert)                           (1661))  // ifsmirnov
    ((RemoveTabletCellConfig)                                       (1662))  // savrus
    ((TruncateOverlayedJournals)                                    (1663))  // aleksandra-zh
    ((ChunkMergeModes)                                              (1664))  // aleksandra-zh
    ((OptimizeChunkReplacer)                                        (1665))  // aleksandra-zh
    ((ChunkMergerFixes)                                             (1666))  // aleksandra-zh
    ((TableCollocation)                                             (1667))  // akozhikhov
    ((CollocationTypeAttribute)                                     (1668))  // gritukan
    ((RecomputeUnrecognizedDynamicConfigOptions)                    (1669))  // shakurov
    ((FixZombieReplicaRemoval)                                      (1670))  // aleksandra-zh
    ((MoreChunkMergerLimits)                                        (1671))  // aleksandra-zh
    ((DontUseUnconfirmedMergedChunk)                                (1672))  // aleksandra-zh
    ((FixChunkMergerAccounting)                                     (1673))  // aleksandra-zh
    ((CalculatePivotKeysForHunks)                                   (1674))  // ifsmirnov
    // 21.3 starts here.
    ((DropPoolTreeInternedAttributes)                               (1800))  // ignat
    ((VirtualMutations)                                             (1801))  // gritukan
    ((CreateUserIgnoreExisting)                                     (1802))  // kvk1920
    ((SaveForcefullyUnmountedTablets)                               (1803))  // ifsmirnov
    ((HandlePoolAttributesThatBecameUninterned)                     (1804))  // renadeen
    ((PerFlavorNodeMaps)                                            (1805))  // gritukan
    ((FixAccountChunkMergerAttributesReplication)                   (1806))  // babenko
    ((CopyJournals)                                                 (1807))  // gritukan
    ((HostObjects)                                                  (1808))  // gritukan
    ((RemoveInterDCEdgeCapacities)                                  (1809))  // gritukan
    ((BackupsInitial)                                               (1810))  // ifsmirnov
    ((ReassignPeersSetLeading)                                      (1811))  // alexkolodezny
    ((MasterSmartPtrs)                                              (1812))  // shakurov
    ((FixClusterNodeMapMigration)                                   (1813))  // gritukan
    ((ChunkAutotomizer)                                             (1814))  // gritukan
    ((ForbidNestedPortals)                                          (1815))  // gritukan
    ((CRP)                                                          (1816))  // shakurov
    ((EnableCrpBuiltinAttribute)                                    (1817))  // shakurov
    ((CrpTokenCountFixes)                                           (1818))  // shakurov
    ((OneMoreChunkMergerOptimization)                               (1819))  // aleksandra-zh
    ((DoubleSnapshotDivergenceFix)                                  (1820))  // shakurov
    // 22.1 starts here.
    ((EnableCellBalancerInConfig)                                   (1900))  // alexkolodezny
    ((RefFromTabletToDynamicStore)                                  (1901))  // ifsmirnov
    ((ChaosDataTransfer)                                            (1902))  // savrus
    ((AccessControlNode)                                            (1903))  // kvk1920
    ((CheckReplicatedTablesCommitOrderingIsStrong)                  (1904))  // akozhikhov
    ((CheckReplicatedTablesAtomicityIsFull)                         (1905))  // akozhikhov
    ((RemovedIsResponseKeeperWarmingUp)                             (1906))  // h0pless
    ((ChunkViewModifier)                                            (1907))  // ifsmirnov
    ((AccountGossipStatisticsOptimization)                          (1908))  // h0pless
    ((MediumOverridesViaHeartbeats)                                 (1909))  // kvk1920
);

constexpr EMasterReign First_21_2_MasterReign = EMasterReign::MasterMergeJobs;

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NHydra::TSaveContext
{
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
    // COMPAT(shakurov): remove #LoadedSchemas once 21.1 is deployed.
    using TLoadedSchemaMap = THashMap<
        NObjectClient::TVersionedObjectId,
        NTableServer::TMasterTableSchema*,
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

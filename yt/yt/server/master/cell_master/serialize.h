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
    ((Crp)                                                          (1816))  // shakurov
    ((EnableCrpBuiltinAttribute)                                    (1817))  // shakurov
    ((CrpTokenCountFixes)                                           (1818))  // shakurov
    ((OneMoreChunkMergerOptimization)                               (1819))  // aleksandra-zh
    ((DoubleSnapshotDivergenceFix)                                  (1820))  // shakurov
    ((DontStoreChunkMetaWithDeadHunkChunks)                         (1821))  // babenko
    ((ValidateTabletContainsStoreInPrepareUpdateTabletStores)       (1822))  // ifsmirnov
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
    ((QueueAgentUser)                                               (1910))  // max42
    ((BanSuperusers)                                                (1911))  // gritukan
    ((EnablePortalAwareStatisticsGossip)                            (1912))  // babenko
    ((HunkErasureCodec)                                             (1913))  // babenko
    ((BackupCheckpoints)                                            (1914))  // ifsmirnov
    ((ChunkLocation)                                                (1915))  // babenko
    ((ReplicationLogTables)                                         (1916))  // savrus
    ((ReplicationCardTokenIsNoMore)                                 (1917))  // babenko
    ((FixObjectSmartPtrLeak)                                        (1918))  // kvk1920
    ((MaxClipTimestampInChunkView)                                  (1919))  // ifsmirnov
    ((ChaosReplicatedTable)                                         (1920))  // babenko
    ((ImplicitReplicationCardId)                                    (1921))  // babenko
    ((AlterReplicatedTables)                                        (1922))  // ifsmirnov
    ((UnifiedReplicaMetadata)                                       (1923))  // babenko
    ((ExtraMountConfigKeys)                                         (1924))  // ifsmirnov
    ((QueueAgentStageAttribute)                                     (1925))  // max42
    ((OwnsReplicationCard)                                          (1926))  // babenko
    ((RegisterQueueConsumerPermission)                              (1927))  // max42
    ((ChunkWeightStatisticsHistogram)                               (1928))  // h0pless
    ((SystemBlocks)                                                 (1929))  // akozhikhov
    ((RefCountedInheritableAttributes)                              (1930))  // babenko
    ((AutoCreateReplicationCard)                                    (1931))  // babenko
    ((RebalancerKillingMergeJobsFix)                                (1932))  // h0pless
    ((DoNotMergeDynamicTables)                                      (1933))  // gritukan
    ((QueueList)                                                    (1934))  // achulkov2
    ((BackupErrors)                                                 (1935))  // ifsmirnov
    ((EnableTypeV3Dyntable)                                         (1936))  // ermolovd
    ((TabletBalancerUser)                                           (1937))  // alexelexa
    ((ReplicationLogTablesTrimming)                                 (1938))  // savrus
    ((RecomputeTabletCellBundleRefCounters)                         (1939))  // gritukan
    ((ConsumerAttributes)                                           (1940))  // achulkov2
    ((ChaosCellRemoval)                                             (1941))  // savrus
    ((SingleChaosFlavor)                                            (1942))  // savrus
    ((HunkErasureCodecCheck)                                        (1943))  // babenko
    ((StripedErasureChunks)                                         (1944))  // gritukan
    // 22.1 but cherry-picked later.
    ((BackupOrdered)                                                (1950))  // ifsmirnov
    // 22.2 starts here.
    ((LogicalDataWeight)                                            (2000))  // achulkov2
    ((DropEnableForcedRotationBackingMemoryAccounting)              (2001))  // babenko
    ((PersistentNodeTouchTime)                                      (2002))  // shakurov
);

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
    DEFINE_BYVAL_RO_PROPERTY(TBootstrap*, Bootstrap);

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

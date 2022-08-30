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
bool ValidateSnapshotReign(NHydra::TReign reign);
NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign reign);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMasterReign,
    // 22.2 starts here.
    ((LogicalDataWeight)                                            (2000))  // achulkov2
    ((DropEnableForcedRotationBackingMemoryAccounting)              (2001))  // babenko
    ((PersistentNodeTouchTime)                                      (2002))  // shakurov
    ((UnconditionallyClearPrerequisitesFromExternalizedRequests)    (2003))  // shakurov
    ((SequoiaObjects)                                               (2004))  // gritukan
    ((MoveSyncSuppressionFlagsToMulticellSyncExt)                   (2005))  // shakurov
    ((BanObviousCyclicSymlinks)                                     (2006))  // h0pless
    ((MasterJobThrottlerPerType)                                    (2007))  // h0pless
    ((PortalAclAndAttributeSynchronization)                         (2008))  // kvk1920
    ((ProfilingPeriodDynamicConfig)                                 (2009))  // shakurov
    ((SequoiaGorshok)                                               (2010))  // gritukan
    ((SequoiaTransaction)                                           (2011))  // gritukan
    ((Aevum)                                                        (2012))  // gritukan
    ((DedicatedChunkHost)                                           (2013))  // h0pless
    ((ChangedExceptionTypeInResolve)                                (2014))  // h0pless
    ((ChunkListType)                                                (2015))  // gritukan
    ((BuiltinMountConfig)                                           (2016))  // ifsmirnov
    ((BackupReplicated)                                             (2017))  // ifsmirnov
    ((DefaultMaxBackingStoreMemoryRatio)                            (2018))  // ifsmirnov
    ((FarewellToOldCFR)                                             (2019))  // akozhikhov
    ((InitTouchTimeOnCloning)                                       (2020))  // shakurov
    ((AdaptiveHedgingManager)                                       (2021))  // akozhikhov
    ((NewReplicatedTableTracker)                                    (2022))  // akozhikhov
    ((RemoveReplicateHostNameOption)                                (2023))  // prime
    ((ForcedUnmountEdenStoreIds)                                    (2024))  // ifsmirnov
    ((RecomputeAccountResourceUsageOnceAgain)                       (2025))  // aleksandra-zh
    ((LetsRecomputeAccountResourceUsageAgainWhyNotItsNice)          (2026))  // gritukan
    ((BackupsMisc)                                                  (2027))  // ifsmirnov
    ((FixChunkWeightHistograms)                                     (2028))  // h0pless
    ((PerClusterBackupFlag)                                         (2029))  // ifsmirnov
    ((SequoiaConfirmChunks)                                         (2030))  // aleksandra-zh
    ((AreaChaosOptions)                                             (2031))  // savrus
    ((CrpPullReplication)                                           (2032))  // aleksandra-zh
    ((SuspendTabletCell)                                            (2033))  // gritukan
    ((MountConfigAttributesInUserAttributes)                        (2034))  // ifsmirnov
    ((RecomputeResourceUsageOnceAgainᛝ)                             (2035))  // gritukan
    ((FixPrerequisiteTxReplication)                                 (2036))  // gritukan
    ((AccessControlObjectOverhaul)                                  (2037))  // shakurov
    ((RotationAfterBackup)                                          (2038))  // ifsmirnov
    ((FixCellarHeartbeat)                                           (2039))  // savrus
    ((SetNoneForHunkErasureCodecAttribute)                          (2040))  // gritukan
    ((OrderedChaosTables)                                           (2041))  // savrus
    ((RecomputeCellBundleRefCounters)                               (2042))  // gritukan
    ((BackupVsTabletAction)                                         (2043))  // ifsmirnov
    ((AlwaysUseNewHeartbeats)                                       (2044))  // gepardo
    ((ExpectedTabletState)                                          (2045))  // savrus
    // 22.3 starts here.
    ((JobProxyBuildVersion)                                         (2100))  // galtsev
    ((TabletActionExpirationTimeout)                                (2101))  // alexelexa
    ((DestroySequoiaChunks)                                         (2102))  // aleksandra-zh
    ((TabletBase)                                                   (2103))  // gritukan
    ((PersistentResponseKeeper)                                     (2104))  // aleksandra-zh
    ((HunkStorage)                                                  (2105))  // gritukan
    ((RemoveCellRoles)                                              (2106))  // aleksandra-zh
    ((ShardedChunkMaps)                                             (2107))  // gritukan
    ((LinkHunkStorageNode)                                          (2108))  // aleksandra-zh
    ((AddPerUserChunkThrottlers)                                    (2109))  // h0pless
    ((RemoveLegacyHeartbeats)                                       (2110))  // gritukan
    ((DiskFamilyWhitelist)                                          (2111))  // kvk1920
    ((ForbidPortalCreationUnderNestedTransaction)                   (2112))  // kvk1920
    ((TransientInheritedAttributeDictionary)                        (2113))  // kvk1920
    ((MakeThrottlerAttributeRemovable)                              (2114))  // h0pless
    ((TransactionRotator)                                           (2115))  // kvk1920
    ((MaxExternalCellBias)                                          (2116))  // babenko
    ((ChunkLocationInReplica)                                       (2117))  // kvk1920
    ((CleanupSomeDynconfigCompatFlags)                              (2118))  // shakurov
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

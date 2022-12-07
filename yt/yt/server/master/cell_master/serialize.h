#pragma once

#include "public.h"
#include "automaton.h"

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/lib/hydra_common/serialize.h>

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
    ((ErrorSanitizer)                                               (2119))  // gritukan
    ((ImaginaryLocationOrderingFix)                                 (2120))  // shakurov
    ((NotSoImaginaryChunkLocations)                                 (2121))  // shakurov
    ((FixOrderedTablesReplicatoinProgress)                          (2122))  // savrus
    ((FixDestroyedReplicasPersistence)                              (2123))  // babenko
    ((FixLatePrepareTxAbort)                                        (2124))  // gritukan
    ((NewStateHashForPersistentResponseKeeper)                      (2125))  // gritukan
    ((DropSomeUselessConfigOptions_22_3)                            (2126))  // akozhikhov
    ((EffectiveErasureCodecs)                                       (2127))  // gritukan
    ((MulticellEnableConsistentChunkReplicaPlacement)               (2128))  // shakurov
    ((ShardedChunkLocationMap)                                      (2129))  // kvk1920
    ((ClearRevisionOnDuplicateEndorsement)                          (2130))  // ifsmirnov
    ((ShardedCellJanitor)                                           (2131))  // babenko
    ((QueueAgentStageWritabilityAndDefaults_22_3)                   (2132))  // achulkov2
    ((FixUpdateTabletStoresTransaction)                             (2133))  // gritukan
    ((RecomputeTabletErrorCount)                                    (2134))  // gritukan
    ((ParameterizedTabletBalancingMetric)                           (2135))  // alexelexa
    ((RemovableQueueAgentStage_22_3)                                (2136))  // achulkov2
    ((FixTransactionRotator_22_3)                                   (2137))  // kvk1920
    ((FixNodeRegistration_22_3)                                     (2138))  // kvk1920
    // 22.4 starts here.
    ((ZookeeperShards)                                              (2200))  // gritukan
    ((QueueAgentStageWritabilityAndDefaults)                        (2201))  // achulkov2
    ((UserPassword)                                                 (2202))  // gritukan
    ((SetUserPassword)                                              (2203))  // gritukan
    ((RefCountedCoWs)                                               (2204))  // babenko
    ((RemovableQueueAgentStage)                                     (2205))  // achulkov2
    ((FixTransactionRotator)                                        (2206))  // kvk1920
    ((BundleControllerConfigAttribute)                              (2207))  // capone212
    ((FixNodeRegistration)                                          (2208))  // kvk1920
    ((ChunkReincarnator)                                            (2209))  // kvk1920
    ((InternedForcedChunkViewCompactionRevision)                    (2210))  // ifsmirnov
);

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NHydra::TSaveContext
{
public:
    explicit TSaveContext(NHydra::ICheckpointableOutputStream* output);

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
    TLoadContext(
        TBootstrap* bootstrap,
        NHydra::ICheckpointableInputStream* input);

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

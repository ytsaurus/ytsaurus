#pragma once

#include "public.h"
#include "automaton.h"

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/lib/hydra/serialize.h>
#include <yt/yt/server/lib/hydra/checkpointable_stream.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/security_server/public.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/master/tablet_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/core/concurrency/thread_pool.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

NHydra::TReign GetCurrentReign();
bool ValidateSnapshotReign(NHydra::TReign reign);
NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign reign);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMasterReign,
    // 23.2 starts here.
    ((TabletServants)                                               (2400))  // ifsmirnov
    ((MediumBase)                                                   (2401))  // gritukan
    ((S3Medium)                                                     (2402))  // gritukan
    ((ColumnRenamingSeparateFlags)                                  (2403))  // orlovorlov
    ((MasterCellChunkStatisticsCollectorConfig)                     (2404))  // kvk1920
    ((QueueReplicatedTablesList)                                    (2405))  // cherepashka
    ((PendingRestartMaintenanceFlag)                                (2406))  // danilalexeev
    ((MakeDestroyedReplicasSetSharded)                              (2407))  // danilalexeev
    ((AvenuesInTabletManager)                                       (2408))  // ifsmirnov
    ((ChaosReplicatedQueuesAndConsumersList)                        (2409))  // cherepashka
    ((ValidateTableSettingsInTabletActions)                         (2410))  // alexelexa
    ((GetRidOfCellIndex)                                            (2411))  // kvk1920
    ((DontForgetToCommitInSetNodeByYPath)                           (2412))  // kvk1920
    ((ResetErrorCountOfUnmountedTablets)                            (2413))  // alexelexa
    ((SequoiaReplicas)                                              (2414))  // aleksandra-zh
    ((AutoTurnOffPendingRestartMaintenanceFlag)                     (2415))  // danilalexeev
    ((AllowSetMountConfigUnderTransaction)                          (2416))  // dave11ar
    ((AddChunkSchemas)                                              (2417))  // h0pless
    ((ChaosManagerSnapshotSaveAndLoadMovement)                      (2418))  // cherepashka
    ((ForbidChangeBuiltinAttributesInExperiments)                   (2419))  // dave11ar
    ((InMemoryModeAndBundleInExperimentDescriptor)                  (2420))  // dave11ar
    ((PortalPermissionValidationBugFix)                             (2421))  // shakurov
    ((ForbidIrreversibleChanges)                                    (2422))  // vovamelnikov
    ((AddSchemafulNodeTypeHandler)                                  (2423))  // h0pless
    ((UseSequoiaReplicas)                                           (2424))  // aleksandra-zh
    ((PerRequestReadComplexityLimits)                               (2425))  // kvk1920
    ((ProxyMaintenanceRequests)                                     (2426))  // kvk1920
    ((AccountsProfilingInSecurityManager)                           (2427))  // vovamelnikov
    ((ReworkClusterResourceLimitsInfinityRelatedBehavior)           (2428))  // kvk1920
    ((KeyPrefixFilter)                                              (2429))  // akozhikhov
    ((MulticellChunkReincarnator)                                   (2430))  // kvk1920
    ((CypressTransactionService)                                    (2431))  // h0pless
    ((SequoiaMapNode)                                               (2432))  // kvk1920
    ((ConcatToSingleCellChunkOwner)                                 (2433))  // shakurov
    ((ChunkMergerQueuesUsagePerAccount)                             (2434))  // vovamelnikov
    ((EnableChangelogChunkPreallocationByDefault)                   (2435))  // akozhikhov
    ((ReplicateAlienClusterRegistry)                                (2436))  // ponasenko-rs
    ((ChunkMergerModeUnderTransaction)                              (2437))  // cherepashka
    ((FixSystemTransactionReplication)                              (2438))  // h0pless
    ((QueueAgentStageForChaos)                                      (2439))  // nadya73
    ((FixMergerStatistics)                                          (2440))  // aleksandra-zh
    ((DisposalNodesLimit)                                           (2441))  // cherepashka
    ((FixTransientAbort_23_2)                                       (2442))  // babenko
    ((ZombieACOs)                                                   (2443))  // shakurov
    ((ReinitializeRootResourceLimits_23_2)                          (2444))  // kvk1920
    ((LastSeenUserAttribute)                                        (2445))  // cherepashka
    ((ChunkReincarnatorTestingUtilities_23_2)                       (2446))  // kvk1920
    // 23.3 starts here.
    ((SecondaryIndex)                                               (2500))  // sabdenovch
    ((SecondaryIndexReplication)                                    (2501))  // sabdenovch
    ((RemoveChunkJobDynamicConfig)                                  (2502))  // danilalexeev
    ((SecondaryIndexUnmountedCheck)                                 (2503))  // sabdenovch
    ((ReinitializeRootResourceLimits)                               (2504))  // kvk1920
    ((NoMountRevisionCheckInBulkInsert)                             (2505))  // ifsmirnov
    ((FixSharedRangeMove)                                           (2506))  // ponasenko-rs
    ((TabletSharedWriteLocks)                                       (2507))  // ponasenko-rs
    ((HunksForever)                                                 (2508))  // babenko
    ((ConfigurablePoolNameValidationRegex)                          (2509))  // renadeen
    ((ChunkReincarnatorTestingUtilities)                            (2510))  // kvk1920
);

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NHydra::TSaveContext
{
public:
    TSaveContext(
        NHydra::ICheckpointableOutputStream* output,
        NLogging::TLogger logger,
        NConcurrency::IThreadPoolPtr backgroundThreadPool);
    TSaveContext(
        IZeroCopyOutput* output,
        const TSaveContext* parentContext);

    TEntitySerializationKey RegisterInternedYsonString(NYson::TYsonString str);

    EMasterReign GetVersion();

private:
    const TSaveContext* const ParentContext_ = nullptr;

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
        NHydra::ICheckpointableInputStream* input,
        NConcurrency::IThreadPoolPtr backgroundThreadPool);
    TLoadContext(
        IZeroCopyInput* input,
        const TLoadContext* parentContext);

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

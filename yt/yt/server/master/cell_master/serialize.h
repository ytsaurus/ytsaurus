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
    ((ConfigurablePoolNameValidationRegex_23_2)                     (2447))  // renadeen
    ((ChunkReincarnatorMinorFixes)                                  (2448))  // kvk1920
    ((AddGroundSupport)                                             (2449))  // h0pless
    ((FixTransactionACLs)                                           (2450))  // h0pless
    ((AddTransactionCompatibilityWithMethodCheck)                   (2451))  // h0pless
    ((DontValidatePermissionsOnNodeUnregistration)                  (2452))  // kvk1920
    ((LimitForChunkCountInMergePipeline)                            (2453))  // cherepashka
    ((SequoiaChunkPurgatory)                                        (2454))  // aleksandra-zh
    ((AttributeBasedAccessControl)                                  (2455))  // shakurov
    ((CreateHunkStorageWithProperAttributes_23_2)                   (2456))  // akozhikhov
    ((FixChunkMergerCopy)                                           (2457))  // aleksandra-zh
    ((ChaosReplicatedConsumersFix)                                  (2458))  // cherepashka
    ((FixListNodeDeprecation_23_2)                                  (2459))  // kvk1920
    ((ValueDictionaryCompression_23_2)                              (2460))  // akozhikhov
    ((CheckChunkCountPerTabletBeforeMount_23_2)                     (2461))  // alexelexa
    ((PersistLastSeenLeaseTransactionTimeout_23_2)                  (2462))  // danilalexeev
    ((AccountProfilingIncumbency_23_2)                              (2463))  // h0pless
    ((FixLastSeenPersistance_23_2)                                  (2464))  // cherepashka
    ((IncreasedMaxKeyColumnInDynamicTableTo128_23_2)                (2465))  // sabdenovch
    // 24.1 starts here.
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
    ((SecondaryIndexForbidsPortal)                                  (2511))  // sabdenovch
    ((SchedulerSystemOutputCompletionTransaction)                   (2512))  // kvk1920
    ((SequoiaPropertiesBeingCreated)                                (2513))  // danilalexeev
    ((TabletPrerequisites)                                          (2514))  // gritukan
    ((FixErrorSerialization)                                        (2515))  // gritukan
    ((Uint32ForNodeId)                                              (2516))  // babenko
    ((HiveManagerLamportTimestamp)                                  (2517))  // danilalexeev
    ((FixAsyncTableStatisticsUpdate)                                (2518))  // danilalexeev
    ((CreateHunkStorageWithProperAttributes)                        (2519))  // akozhikhov
    ((SecondaryIndexOverReplicatedTables)                           (2520))  // sabdenovch
    ((MakeRequisitionComputationSharded)                            (2521))  // danilalexeev
    ((FixMutationTypeNaming)                                        (2522))  // babenko
    ((FixListNodeDeprecation)                                       (2523))  // kvk1920
    ((IncreasedMaxKeyColumnInDynamicTable)                          (2524))  // sabdenovch
    ((EnableRealChunkLocationsByDefault)                            (2525))  // kvk1920
    ((SaneTxActionAbort)                                            (2526))  // kvk1920
    ((ValueDictionaryCompression)                                   (2527))  // akozhikhov
    ((SaneTxActionAbortFix)                                         (2528))  // kvk1920
    ((FixResponseHash)                                              (2529))  // babenko
    ((SmoothTabletMovement)                                         (2530))  // ifsmirnov
    ((DistributedTabletPrerequisites)                               (2531))  // gritukan
    ((SecondaryIndexUnfolding)                                      (2532))  // sabdenovch
    ((TablesInSequoia)                                              (2533))  // h0pless
    ((CheckChunkCountPerTabletBeforeMount)                          (2534))  // alexelexa
    ((ErasureHunkStorage_24_1)                                      (2535))  // akozhikhov
    ((TabletCellsHydraPersistenceMigration)                         (2536))  // danilalexeev
    ((ErasureHunkCodecInDoCopy)                                     (2537))  // akozhikhov
    ((CachedMaxSnapshotId)                                          (2538))  // ifsmirnov
    ((MasterCellsCompositionReconfigurationOnNodes)                 (2539))  // cherepashka
    ((ImproveMaintenanceRequestsApi)                                (2540))  // kvk1920
    ((PersistLastSeenLeaseTransactionTimeout)                       (2541))  // danilalexeev
    ((WideDateTimeTypes)                                            (2542))  // whatsername
    ((HunkMedia)                                                    (2543))  // kivedernikov
    ((SequoiaQueues)                                                (2544))  // aleksandra-zh
    ((ChunkListTraversalIncumbency)                                 (2545))  // danilalexeev
    ((UsersInSequoia)                                               (2546))  // cherepashka
    ((RemoveEnableSharedWriteLocksFlag_24_1)                        (2547))  // ponasenko-rs
    ((AddTtlSystemColumn_24_1)                                      (2548))  // alexelexa
    ((ChunkMergerModeFix)                                           (2549))  // cherepashka
    ((DynamizeLocalJanitorConfig)                                   (2550))  // danilalexeev
    ((SchemalessEndUploadPreservesTableSchemaByDefault)             (2551))  // shakurov
    ((MirrorCypressTransactionsToSequoia)                           (2552))  // kvk1920
    ((SecondaryIndexUser_24_1)                                      (2553))  // sabdenovch
    ((RemoveEnableSharedWriteLocksFlagLeftovers_24_1)               (2554))  // ponasenko-rs
    ((BundleResourceUsageGossipNotToIncludeSelfUsage)               (2555))  // ifsmirnov
    ((RemoveChaosIndependentPeersAssumption_24_1)                   (2556))  // ponasenko-rs
    ((ZombifyTabletAction)                                          (2557))  // ifsmirnov
    ((AccountProfilingIncumbency_24_1)                              (2558))  // h0pless
    ((RemoveParameterizedBalancingMetricSetting_24_1)               (2559))  // alexelexa
    ((SecondaryIndexSchemaValidation_24_1)                          (2560))  // sabdenovch
    ((NodeReplicationMutation)                                      (2561))  // cherepashka
    ((TraceIdInSequoia)                                             (2562))  // cherepashka
    ((DynamicMasterCellReconfigurationOnNodes)                      (2563))  // cherepashka
    ((MissingRackAttribute_24_1)                                    (2564))  // danilalexeev
    ((FixMakeChunkLocationsOnline)                                  (2565))  // cherepashka
    ((FixLastSeenPersistance)                                       (2566))  // cherepashka
    ((AnyTypedKeysInSortedTables)                                   (2567))  // whatsername
    ((SecondaryIndexPredicate_24_1)                                 (2568))  // sabdenovch
    ((IncreasedMaxKeyColumnInDynamicTableTo128_24_1)                (2569))  // sabdenovch
    ((FixEarlyInitializingUnregisteredMasters)                      (2570))  // cherepashka
    ((ForceRackAwarenessForErasureParts)                            (2571))  // danilalexeev
    ((FixCypressTransactionMirroring_24_1)                          (2572))  // kvk1920
    ((QueueProducers_24_1)                                          (2573))  // apachee
    ((PendingRemovalUserAttribute)                                  (2574))  // cherepashka
    ((DynamizationCypressConfig)                                    (2575))  // kivedernikov
    ((ChunkMetaLimit)                                               (2576))  // kivedernikov
    ((RemovedRedundantStatisticsFromChunkOwnerBase)                 (2577))  // cherepashka
    ((SerializationOfDataStatistics)                                (2578))  // cherepashka
    ((ReturnedHandleOfInvalidDataWeight)                            (2579))  // cherepashka
    ((SequoiaReplicasConfig)                                        (2580))  // aleksandra-zh
    ((HunkStorageRemovalInTableZombify)                             (2581))  // akozhikhov
    ((DeltaStatisticsPointer)                                       (2582))  // cherepashka
    ((InheritChunkMergerModeWhenCopy_24_1)                          (2583))  // cherepashka
    ((ForbidExportChunkWithHunks)                                   (2584))  // kivedernikov
    ((RemovedDuplicateChunkCountFromSnapshot)                       (2585))  // cherepashka
    ((UnapprovedSequoiaReplicas)                                    (2586))  // aleksandra-zh
    ((ChunkMergerBackoff)                                           (2587))  // cherepashka
    ((ForbiddenErasureCodecs_ForbiddenComprCodecsRefactoring_24_1)  (2588))  // abogutskiy, shakurov
    ((MediumIndexSizeofReduction)                                   (2589))  // cherepashka
    // 24.2 starts here.
    ((DropLegacyClusterNodeMap)                                     (2600))  // babenko
    ((ErasureHunkStorage)                                           (2601))  // akozhikhov
    ((YsonDeserializeMicroseconds)                                  (2602))  // dgolear
    ((RemoveEnableSharedWriteLocksFlag)                             (2603))  // ponasenko-rs
    ((AdminsGroup)                                                  (2604))  // aleksandr.gaev
    ((FixAlterTableReplicaWithRTT)                                  (2605))  // ngc224
    ((AddTtlSystemColumn)                                           (2606))  // alexelexa
    ((RemoveChaosIndependentPeersAssumption)                        (2607))  // ponasenko-rs
    ((SecondaryIndexUser)                                           (2608))  // sabdenovch
    ((RemoveEnableSharedWriteLocksFlagLeftovers)                    (2609))  // ponasenko-rs
    ((AccountProfilingIncumbency)                                   (2610))  // h0pless
    ((RemoveParameterizedBalancingMetricSetting)                    (2611))  // alexelexa
    ((RipAevum)                                                     (2612))  // babenko
    ((SecondaryIndexSchemaValidation)                               (2613))  // sabdenovch
    ((ErasureChunksCanBeNonVital)                                   (2614))  // achulkov2
    ((MissingRackAttribute)                                         (2615))  // danilalexeev
    ((SecondaryIndexPredicate)                                      (2616))  // sabdenovch
    ((AddTableNodeCustomRuntimeData)                                (2617))  // gryzlov-ad
    ((IncreasedMaxKeyColumnInDynamicTableTo128)                     (2618))  // sabdenovch
    ((FixCypressTransactionMirroring)                               (2619))  // kvk1920
    ((QueueProducers)                                               (2620))  // apachee
    ((AddForbiddenErasureCodecsOption)                              (2621))  // abogutskiy
    ((InheritChunkMergerModeWhenCopy)                               (2622))  // cherepashka
    ((ForbiddenCompressionCodecsOptionRefactoring)                  (2623))  // abogutskiy
    ((MultipleTableUpdateQueues)                                    (2624))  // babenko
    ((RefactorCrossCellCopyInPreparationForSequoia)                 (2625))  // h0pless
    ((CypressNodeReachability)                                      (2626))  // danilalexeev
);

static_assert(TEnumTraits<EMasterReign>::IsMonotonic, "Master reign enum is not monotonic");

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

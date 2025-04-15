#pragma once

#include "public.h"

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
    ((SecondaryIndexPerUserValidation_24_1)                         (2590))  // sabdenovch
    ((SecondaryIndexUnique_24_1)                                    (2591))  // sabdenovch
    ((FixMergerStatisticsOnceAgain)                                 (2592))  // aleksandra-zh
    ((IntraCellCrossShardLinks)                                     (2593))  // shakurov
    ((NonrecursivePermissionCheckOnExpirationSettingsUpdate_24_1)   (2594))  // babenko
    ((ReplicationCollocationOptions_24_1)                           (2595))  // akozhikhov
    ((NestedAggregateColumns_24_1)                                  (2596))  // lukyan
    ((DynamicTableSchemaContraint)                                  (2597))  // whatsername
    ((RemoveSchemalessEndUploadPreservesTableSchema_24_1)           (2598))  // h0pless
    // 24.2 starts here.
    ((Start_24_2)                                                   (2700))  //
    ((DropLegacyClusterNodeMap)                                     (2701))  // babenko
    ((ErasureHunkStorage)                                           (2702))  // akozhikhov
    ((YsonDeserializeMicroseconds)                                  (2703))  // dgolear
    ((RemoveEnableSharedWriteLocksFlag)                             (2704))  // ponasenko-rs
    ((AdminsGroup)                                                  (2705))  // aleksandr.gaev
    ((FixAlterTableReplicaWithRTT)                                  (2706))  // ngc224
    ((AddTtlSystemColumn)                                           (2707))  // alexelexa
    ((RemoveChaosIndependentPeersAssumption)                        (2708))  // ponasenko-rs
    ((SecondaryIndexUser)                                           (2709))  // sabdenovch
    ((RemoveEnableSharedWriteLocksFlagLeftovers)                    (2710))  // ponasenko-rs
    ((AccountProfilingIncumbency)                                   (2711))  // h0pless
    ((RemoveParameterizedBalancingMetricSetting)                    (2712))  // alexelexa
    ((RipAevum)                                                     (2713))  // babenko
    ((SecondaryIndexSchemaValidation)                               (2714))  // sabdenovch
    ((ErasureChunksCanBeNonVital)                                   (2715))  // achulkov2
    ((MissingRackAttribute)                                         (2716))  // danilalexeev
    ((SecondaryIndexPredicate)                                      (2717))  // sabdenovch
    ((AddTableNodeCustomRuntimeData)                                (2718))  // gryzlov-ad
    ((IncreasedMaxKeyColumnInDynamicTableTo128)                     (2719))  // sabdenovch
    ((FixCypressTransactionMirroring)                               (2720))  // kvk1920
    ((QueueProducers)                                               (2721))  // apachee
    ((AddForbiddenErasureCodecsOption)                              (2722))  // abogutskiy
    ((InheritChunkMergerModeWhenCopy)                               (2723))  // cherepashka
    ((ForbiddenCompressionCodecsOptionRefactoring)                  (2724))  // abogutskiy
    ((MultipleTableUpdateQueues)                                    (2725))  // babenko
    ((RefactorCrossCellCopyInPreparationForSequoia)                 (2726))  // h0pless
    ((CypressNodeReachability)                                      (2727))  // danilalexeev
    ((SecondaryIndexPerUserValidation)                              (2728))  // sabdenovch
    ((SecondaryIndexUnique)                                         (2729))  // sabdenovch
    ((DropImaginaryChunkLocations)                                  (2730))  // kvk1920
    ((RackDataCenter)                                               (2731))  // proller
    ((NonrecursivePermissionCheckOnExpirationSettingsUpdate)        (2732))  // babenko
    ((SecondaryIndexExternalCellTag)                                (2733))  // sabdenovch
    ((ReplicationCollocationOptions)                                (2734))  // akozhikhov
    ((NestedAggregateColumns)                                       (2735))  // lukyan
    ((ForbiddenFieldsInMountConfig)                                 (2736))  // ifsmirnov
    ((NativeTransactionExternalization)                             (2737))  // kvk1920
    ((RemoveCompatAroundAclSubjectTagFilterValidation)              (2738))  // h0pless
    ((CypressLocksInSequoia)                                        (2739))  // kvk1920
    ((RemoveStuckAttributes)                                        (2740))  // aleksandra-zh
    ((OptionalHunkPrimaryMedium)                                    (2741))  // kazachonok
    ((RemoveSchemalessEndUploadPreservesTableSchema)                (2742))  // h0pless
    ((TtlVerificationInRemoveExpiredNodes)                          (2743))  // koloshmet
    ((ValidateResourceUsageIncreaseOnPrimaryMediumChange)           (2744))  // danilalexeev
    ((Int64InHistogramSnapshot)                                     (2745))  // babenko
    ((SecondaryIndexUnfoldedColumnApi)                              (2746))  // sabdenovch
    ((Decimal256)                                                   (2747))  // achulkov2+ermolovd
    ((HunkStorageMulticell)                                         (2748))  // akozhikhov
    ((NoAvenuesDuringMigrationTo24_2)                               (2749))  // ifsmirnov
    // 25.1 starts here.
    ((SequoiaSetActionLatePrapare)                                  (2800))  // danilalexeev
    ((RemoveUseHydraPersistenceDirectoryFlag)                       (2801))  // danilalexeev
    ((ParentIdForSequoiaNodes)                                      (2802))  // kvk1920
    ((SequoiaThrottlers)                                            (2803))  // danilalexeev
    ((SequoiaMultisetAttributesAction)                              (2804))  // danilalexeev
    ((SequoiaNodeExpiration)                                        (2805))  // danilalexeev
    ((IncumbentSchedulerConfigDefaults)                             (2806))  // cherepashka
    ((ScanFormatIsDefaultForDynamicTables)                          (2807))  // sabdenovch
    ((EnumsAndChunkReplicationReductionsInTTableNode)               (2808))  // cherepashka
    ((PreserveAclFlagForMove)                                       (2809))  // koloshmet
    ((RemoveDrtDisableOptions)                                      (2810))  // ponasenko-rs
    ((IntroduceNewPipelineForCrossCellCopy)                         (2811))  // h0pless
    ((DanglingLocationsCleaning)                                    (2812))  // koloshmet
    ((AllowToMoveReplicationLogTables)                              (2813))  // osidorkin
    ((BranchesInSequoia)                                            (2814))  // kvk1920
    ((SecondaryIndexStates)                                         (2815))  // sabdenovch
    ((MasterCellsRemoval)                                           (2816))  // cherepashka
    ((ExecNodeCellAggregatedStateReliabilityPropagationFix)         (2817))  // cherepashka
    ((DropDynamicConfigExtraRefFlagForExportedObjects)              (2818))  // cherepashka
    ((DropLegacyZookeeperShard)                                     (2819))  // cherepashka
    ((OpaqueSchemaAttribute)                                        (2820))  // cherepashka
    ((FixNullPtrDereferenceInCreateForeignObject)                   (2821))  // cherepashka
    ((FixCompositeKeyDeserialization)                               (2822))  // ermolovd
    ((FixBuiltinAdminsGroupId)                                      (2823))  // cherepashka
    ((StableOrderedSecondaryIndicesDestruction)                     (2824))  // koloshmet
    ((FixExportedObjectsRefs)                                       (2825))  // aleksandra-zh
    ((SecondaryIndexAbandonment)                                    (2826))  // sabdenovch
    ((SequoiaRefreshQueues)                                         (2827))  // aleksandra-zh
    ((PerLocationNodeHeartbeat)                                     (2828))  // danilalexeev
    ((HunkSpecificMediaFixes)                                       (2829))  // shakurov
    ((RipLogicalChunkCount)                                         (2830))  // ifsmirnov
    ((CancelTabletTransition)                                       (2831))  // ifsmirnov
    ((MulticellStatisticsCollector)                                 (2832))  // koloshmet
    ((ForbidAlterKeyColumnToAny)                                    (2833))  // dtorilov
    ((UpdateRttConfig)                                              (2834))  // akozhikhov
    ((TabletTransactionSerializationType_25_1_NOOP)                 (2835))  // ponasenko-rs
    ((FixChunkStatisticsInMasterCellRemoval)                        (2836))  // cherepashka
    ((ResetHunkSpecificMedia)                                       (2837))  // shakurov
    ((FixReshardOfOrderedTablesWithHunks)                           (2838))  // akozhikhov
    ((FixTabletSizeCalculationForCellAssignmentWhenMounting)        (2839))  // alexelexa
    ((ResetHunkSpecificMediaAndRecomputeTabletStatistics)           (2840))  // shakurov
    ((PrepareModifyReplicasRefreshFlagsChecks)                      (2841))  // babenko
    ((PersistAuxiliaryNodeStatistics_25_1)                          (2842))  // ifsmirnov
    ((GlobalObjectReplicationRespectsTypeHandlers)                  (2843))  // shakurov
    ((LostVitalChunksSample_25_1)                                   (2844))  // koloshmet
    ((SecondaryIndexOuroboros)                                      (2845))  // sabdenovch
    // 25.2 starts here.
    ((Start_25_2)                                                   (2900))  // ponasenko-rs
    ((TabletTransactionSerializationType)                           (2901))  // ponasenko-rs
    ((CypressProxyTracker)                                          (2902))  // kvk1920
    ((PersistAuxiliaryNodeStatistics)                               (2903))  // ifsmirnov
    ((LostVitalChunksSample)                                        (2904))  // koloshmet
    ((MasterCompactTableSchema)                                     (2905))  // cherepashka
    ((PrerequisiteTransactionsInSequoia)                            (2906))  // cherepashka
    ((MasterCellRolesChangeValidation)                              (2907))  // cherepashka
    ((DropLegayReplicas)                                            (2908))  // babenko
    ((KeyBoundsInTabletChunkManager)                                (2909))  // ifsmirnov
    ((DropChunkMergerCompats)                                       (2910))  // cherepashka
    ((SecondaryIndexEvaluated)                                      (2911))  // sabdenovch
    ((ResetHunkMediaOnBranchedNodes)                                (2912))  // shakurov
    ((PerChunkReplicaDataNodeRegistrationTrottling)                 (2913))  // cherepashka
    ((MissingObjectErrorAttribute)                                  (2914))  // kvk1920
    ((FixAttributeInheritanceInCreateVerb)                          (2915))  // h0pless
    ((FixLastSeenReplicas)                                          (2916))  // kvk1920
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

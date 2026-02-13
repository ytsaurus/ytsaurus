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
    ((PerChunkReplicaDataNodeRegistrationThrottling)                (2913))  // cherepashka
    ((MissingObjectErrorAttribute)                                  (2914))  // kvk1920
    ((FixAttributeInheritanceInCreateVerb)                          (2915))  // h0pless
    ((FixLastSeenReplicas)                                          (2916))  // kvk1920
    ((FixReplicatedTransactionFinish)                               (2917))  // kvk1920
    ((FixSysOperationCommittedAttribute)                            (2918))  // kvk1920
    ((AddLockableDynamicTables)                                     (2919))  // dave11ar
    ((DeterministicStateHashComputationInResponseKeeper)            (2920))  // koloshmet
    ((CypressProxyVersion)                                          (2921))  // kvk1920
    ((AddEscapingInCrossCellCopy)                                   (2922))  // h0pless
    ((MinorRefactoringInExpirationTracker)                          (2923))  // h0pless
    ((FixTransactionActionAbort)                                    (2924))  // kvk1920
    ((ResetHunkMediaOnBranchedNodesOnly)                            (2925))  // shakurov
    ((ResetInheritACLInCrossCellCopy)                               (2926))  // h0pless
    ((FixDetachmentOfJournalHunkChunk)                              (2927))  // akozhikhov
    ((AutomaticCellMapMigration)                                    (2928))  // danilalexeev
    ((ReplicaDataInReplicatableTabletContent)                       (2929))  // ifsmirnov
    ((TransactionActionStates)                                      (2930))  // babenko
    ((SupportTzTypes)                                               (2931))  // nadya02
    ((ResourceQuotaAttributeForBundles)                             (2932))  // ifsmirnov
    ((FixSettingListAttributeForUnexistingNode)                     (2933))  // kvk1920
    ((IntroduceCypressToSequoiaCopy)                                (2934))  // h0pless
    ((TableSchemaCache)                                             (2935))  // cherepashka
    ((DestroyTransactionActionStateInCommit)                        (2936))  // kvk1920
    ((ReachabilityBasedSequoiaNodeRefCount)                         (2937))  // kvk1920
    ((OrchidInSequoia)                                              (2938))  // kvk1920
    ((TransientCypressProxyRegistration)                            (2939))  // kvk1920
    ((TablesInSequoia)                                              (2940))  // kvk1920
    ((SequoiaTransactionTitle)                                      (2941))  // kvk1920
    ((SequoiaPathMangling)                                          (2942))  // danilalexeev
    ((RemoveCompatsInEndUpload)                                     (2943))  // h0pless
    ((MoveRetainedTimestampAndOthersToExtraAttributes)              (2944))  // ifsmirnov
    ((ChunkLocationCounterId)                                       (2945))  // aleksandra-zh
    ((SysOperationsInSequoia)                                       (2946))  // kvk1920
    ((CheckNodeWriteSessions)                                       (2947))  // koloshmet
    ((DontValidateLockCountOnExternalCells)                         (2948))  // h0pless
    ((DedicatedChunkHostInRoleValidation)                           (2949))  // cherepashka
    ((DocumentInSequoia)                                            (2950))  // kvk1920
    ((CheckReplicationProgressSchema)                               (2951))  // savrus
    ((EnableSmoothTabletMovementFlag)                               (2952))  // ifsmirnov
    ((PerRowSequencerFixes)                                         (2953))  // ponasenko-rs
    ((AutomaticCellMapMigration_25_2)                               (2954))  // danilalexeev
    ((FixUseAsWithNullObjects)                                      (2955))  // cherepashka
    ((MakeCompactTableSchemaRefCounted)                             (2956))  // cherepashka
    ((CrossCellCopyFinalFixes)                                      (2957))  // shakurov
    ((HydraLogicalClock)                                            (2958))  // h0pless
    ((FixBuiltinUserIds_25_2)                                       (2959))  // cherepashka
    ((DropEnableFixRequisitionUpdateCompat_25_2)                    (2960))  // kvk1920
    ((FixValidateTabletContainsStoreForBulkInsertOutputTimestamps)  (2961))  // dave11ar
    // 25.3 starts here.
    ((Start_25_3)                                                   (3000))  // community bot
    ((DropOldMountConfigKeyLists)                                   (3001))  // ifsmirnov
    ((TransactionCommitsAndAbortsValidatePermissions)               (3002))  // faucct
    ((ValidateUnversionedChunkConstraintsBeforeMount)               (3003))  // atalmenev
    ((FixBuiltinUserIds)                                            (3004))  // cherepashka
    ((DropEnableFixRequisitionUpdateCompat)                         (3005))  // kvk1920
    ((SequoiaPrerequisiteRevisionsOnWrite)                          (3006))  // cherepashka
    ((DropHydraRemoveExpiredNodes)                                  (3007))  // danilalexeev
    ((TransactionsCanNowFeelImpendingDoom)                          (3008))  // h0pless
    ((ValidateClockCellTagOnChaosMount)                             (3009))  // ponasneko-rs
    ((WriteAclToSequoiaTable)                                       (3010))  // danilalexeev
    ((WeakPtrInTableReplicas)                                       (3011))  // babenko
    ((ChunkLocationDisposal)                                        (3012))  // grphil
    ((MulticellChunksSamples)                                       (3013))  // grphil
    ((CypressProxyState)                                            (3014))  // h0pless
    ((FirstClassFullReadSupport)                                    (3015))  // coteeq
    ((RowLevelSecurity)                                             (3016))  // coteeq
    ((TabletActionManager)                                          (3017))  // ifsmirnov
    ((AdditionalMulticellChunksSamples)                             (3018))  // grphil
    ((BulkInsertSendsDynamicStoresToMountingTablets)                (3019))  // ifsmirnov
    ((SequoiaPrerequisiteRevisionsOnRead)                           (3020))  // cherepashka
    ((TransactionFinisher)                                          (3021))  // kvk1920
    ((DisablePermissionCheckForSequoiaNodes)                        (3022))  // shakurov
    ((AddSchemaRevision)                                            (3023))  // theevilbird
    ((RespectChunkMergerModeAttributeWhenChunkMergerIsDisabled)     (3024))  // cherepashka
    ((KindaFixHunkChunkListInReshard)                               (3025))  // babenko
    ((PreserveUnflushedTimestampForUnmountedTablets)                (3026))  // ifsmirnov
    ((LocalUserRequestThrottlers)                                   (3027))  // faucct
    ((FixResolvePrerequisitePathToLocalObjectForSymlinks)           (3028))  // cherepashka
    ((SequencerStateFix)                                            (3029))  // aleksandra-zh
    ((DropLegacyCellMap)                                            (3030))  // danilalexeev
    ((FixZombieLocations)                                           (3031))  // aleksandra-zh
    ((AdHocPermissionValidation)                                    (3032))  // danilalexeev
    ((DropSecondaryIndexCreationPermissionFlags)                    (3033))  // sabdenovch
    ((AddRegisteredLocationState)                                   (3034))  // grphil
    ((RootstockScionAttributesSync)                                 (3035))  // danilalexeev
    ((SequoiaInheritableAttributes)                                 (3036))  // kvk1920
    ((PendingRemovalInCheckPermissionByAcl)                         (3037))  // cherepashka
    ((DropUseProperReplicaAdditionReasonFlag)                       (3038))  // grphil
    ((FixOldestPartMissingChunksRanking)                            (3039))  // grphil
    ((FixLoadingTransactionLeasesStateFromSnapshot)                 (3040))  // kvk1920
    ((FixPrerequisiteLeasesIssuingForMasterCells)                   (3041))  // cherepashka
    ((FixSchemaDivergence)                                          (3042))  // h0pless
    ((FixSequoiaAccountInheritance)                                 (3043))  // babenko
    ((FixAccountInSequoiaCopy)                                      (3044))  // babenko
    ((SupportMaxNodeChildCountInSequoia)                            (3045))  // babenko
    ((MountPermissionToSequoiaNodes)                                (3046))  // danilalexeev
    ((FixSecurityTagsMessingWithChunkListStructure)                 (3047))  // h0pless
    ((SortUniqueTxReplicatedToCellTags_25_3)                        (3048))  // shakurov
    // 25.4 starts here.
    ((Start_25_4)                                                   (3100))  // community bot
    ((HunksInStaticTables)                                          (3101))  // akozhikhov
    ((SealHunkJournalChunkWithMultipleParents)                      (3102))  // akozhikhov
    ((SequoiaTabletCellBundles)                                     (3103))  // danilalexeev
    ((SequoiaMapKeyLengthLimit)                                     (3104))  // babenko
    ((FixSchemaDivergence_25_4)                                     (3105))  // h0pless
    ((SetClipTimestampInAlter)                                      (3106))  // alexelexa
    ((SecondaryIndexAcd)                                            (3107))  // sabdenovch
    ((RestartedNodeState)                                           (3108))  // grphil
    ((SequoiaLinkIsLink)                                            (3109))  // kvk1920
    ((RemoveSchemaRevision)                                         (3110))  // theevilbird
    ((ObjectAccessControlInSequoia)                                 (3111))  // danilalexeev
    ((InplaceReshard)                                               (3112))  // atalmenev
    ((FixRestartedNodesProcessing)                                  (3113))  // grphil
    ((RefactoringAroundChunkStoredReplicas)                         (3114))  // cherepashka
    ((FixSecurityTagsMessingWithChunkListStructure_25_4)            (3115))  // h0pless
    ((FixMaintenanceRequestInconsistencyOnMasterCellAddition)       (3116))  // kvk1920
    ((FixAccountsInSequoiaCopy)                                     (3117))  // babenko
    ((UnresolvedSuffixOnRequestInvolvesSequoia)                     (3118))  // danilalexeev
    ((AddSignatureKeysmithUser)                                     (3119))  // pavook
    ((CreationWithAmpersandInPath)                                  (3120))  // kvk1920
    ((AuthorizedExpiration)                                         (3121))  // koloshmet
    ((RemoveListNodes)                                              (3122))  // h0pless
    ((ChangeSequoiaChunkPurgatory)                                  (3123))  // grphil
    ((ChunkRequisitionActiveEntries)                                (3124))  // shakurov
    ((AddHasRowLevelAceAttribute)                                   (3125))  // coteeq
    ((ColumnConstraintsForTablesOnMaster)                           (3126))  // cherepashka
    ((HydraLogicalRecordId)                                         (3127))  // h0pless
    ((FixStatisticsAccountingUponHunkJournalChunkSeal)              (3128))  // akozhikhov
    ((AddStrongerTxAccessValidationCheck)                           (3129))  // h0pless
    ((StructFieldRenamingAndRemoval)                                (3130))  // s-berdnikov
    ((SequoiaChunkPurgatoryRecordsCount)                            (3131))  // grphil
    ((UnbuiltinSequoiaTabletCellBundles)                            (3132))  // danilalexeev
    ((TransferBundleResources)                                      (3133))  // ifsmirnov
    ((ProvisionalFlush)                                             (3134))  // atalmenev
    ((RerunUpdateModeMigration)                                     (3135))  // h0pless
    ((SetTouchAndAccessTimeInMutationsSynchronously)                (3136))  // koloshmet
    ((ChangedExpirationResetApi)                                    (3137))  // koloshmet
    ((ExplicitAccountSetOnSequoiaNodeCreation)                      (3138))  // danilalexeev
    ((PersistAccountStatisticsGossip)                               (3139))  // theevilbird
    ((RemoveRecalculationMaxReplicasPerFailureDomainFlag)           (3140))  // theevilbird
    ((IncreaseTabletToCellRatioDefaultToEffectiveInfinity)          (3141))  // alexelexa
    ((AddConstraintsIntoMasterSnapshot)                             (3142))  // cherepashka
    ((SaveOriginatorTabletsAfterReshard)                            (3143))  // atalmenev
    ((RemoveChunkReplicaReasonFix)                                  (3144))  // grphil
    ((SmoothMovementOrdered)                                        (3145))  // ifsmirnov
    ((FixSequoiaNodeAclParsing)                                     (3146))  // danilalexeev
    ((AddPerTabletConflictHorizonTimestamp)                         (3147))  // ponasenko-rs
    ((UserClusterwideRequestRateLimits)                             (3148))  // shakurov
    ((SortUniqueTxReplicatedToCellTags)                             (3149))  // shakurov
    ((FixMirroredTransactionCreationFailure)                        (3150))  // kvk1920
    ((SafeRemovalJobScheduling)                                     (3151))  // grphil
    ((ReturnSecondaryMastersCellConfigAsResponseForNodeHeartbeat)   (3152))  // cherepashka
    ((FixDynamicallyPropagatedMastersCellTags)                      (3153))  // aleksandra-zh
    ((DataNodeValidationFullHeartbeats)                             (3154))  // danilalexeev
    ((FillAttributesOnNativeCellDuringMaterialization)              (3155))  // h0pless
    ((LocationIndiciesInChunkConfirmation)                          (3156))  // cherepashka
    ((KulenovClock)                                                 (3157))  // aleksandra-zh
    ((ReignInHiveMessages_25_4)                                     (3158))  // ifsmirnov
    ((TransferableInheritedAttributes_25_4)                         (3159))  // kvk1920
    // 26.1 starts here.
    ((Start_26_1)                                                   (3200))  // community bot
    ((SecondaryIndexUnfoldedNames)                                  (3201))  // sabdenovch
    ((LocationIndiciesInChunkConfirmation_26_1)                     (3202))  // cherepashka
    ((KulenovClock_26_1)                                            (3203))  // aleksandra-zh
    ((ReignInHiveMessages)                                          (3204))  // ifsmirnov
    ((TransferableInheritedAttributes)                              (3205))  // kvk1920
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

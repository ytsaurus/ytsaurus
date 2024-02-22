#pragma once

#include "fair_share_strategy_operation_controller.h"
#include "helpers.h"
#include "allocation.h"
#include "private.h"
#include "resource_tree.h"
#include "resource_tree_element.h"
#include "scheduler_strategy.h"
#include "scheduling_context.h"
#include "packing.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/job_metrics.h>
#include <yt/yt/server/lib/scheduler/scheduling_tag.h>
#include <yt/yt/server/lib/scheduler/resource_metering.h>

#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/misc/adjusted_exponential_moving_average.h>

#include <yt/yt/library/vector_hdrf/resource_vector.h>

#include <yt/yt/library/vector_hdrf/fair_share_update.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>
#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

using NVectorHdrf::TSchedulableAttributes;
using NVectorHdrf::TDetailedFairShare;
using NVectorHdrf::TIntegralResourcesState;

////////////////////////////////////////////////////////////////////////////////

static constexpr int UnassignedTreeIndex = -1;
static constexpr int UndefinedSlotIndex = -1;

////////////////////////////////////////////////////////////////////////////////

static constexpr double InfiniteSatisfactionRatio = 1e+9;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESchedulerElementType,
    (Root)
    (Pool)
    (Operation)
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EStarvationStatus,
    (NonStarving)
    (Starving)
    (AggressivelyStarving)
);

////////////////////////////////////////////////////////////////////////////////

struct IFairShareTreeElementHost
    : public virtual TRefCounted
{
    virtual TResourceTree* GetResourceTree() = 0;

    virtual void BuildElementLoggingStringAttributes(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TSchedulerElement* element,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IFairShareTreeElementHost)

////////////////////////////////////////////////////////////////////////////////

//! Attributes that are kept between fair share updates.
struct TPersistentAttributes
{
    EStarvationStatus StarvationStatus;
    TInstant LastNonStarvingTime = TInstant::Now();
    std::optional<TInstant> BelowFairShareSince;
    TAdjustedExponentialMovingAverage HistoricUsageAggregator = TAdjustedExponentialMovingAverage();

    TResourceVector BestAllocationShare = TResourceVector::Ones();
    TInstant LastBestAllocationShareUpdateTime;

    TIntegralResourcesState IntegralResourcesState;

    std::optional<TJobResources> AppliedSpecifiedResourceLimits;

    void ResetOnElementEnabled();
};

////////////////////////////////////////////////////////////////////////////////

struct TFairSharePostUpdateContext
{
    const TFairShareStrategyTreeConfigPtr& TreeConfig;
    const TInstant Now;

    TEnumIndexedArray<EUnschedulableReason, int> UnschedulableReasons;

    TNonOwningOperationElementMap EnabledOperationIdToElement;
    TNonOwningOperationElementMap DisabledOperationIdToElement;
    TNonOwningPoolElementMap PoolNameToElement;
};

////////////////////////////////////////////////////////////////////////////////

struct TResourceDistributionInfo
{
    TJobResources DistributedStrongGuaranteeResources;
    TJobResources DistributedResourceFlow;
    TJobResources DistributedBurstGuaranteeResources;
    TJobResources DistributedResources;
    TJobResources UndistributedResources;
    TJobResources UndistributedResourceFlow;
    TJobResources UndistributedBurstGuaranteeResources;
};

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerElementPostUpdateAttributes
{
    TJobResources UnschedulableOperationsResourceUsage;

    double SatisfactionRatio = 0.0;
    double LocalSatisfactionRatio = 0.0;

    // Only for pools.
    IDigestPtr SatisfactionDigest;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulerElementFixedState
{
public:
    // Tree config.
    DEFINE_BYREF_RO_PROPERTY(TFairShareStrategyTreeConfigPtr, TreeConfig);

    // Flag indicates that we can change fields of scheduler elements.
    DEFINE_BYVAL_RO_PROPERTY(bool, Mutable, true);

    // These fields are persistent between updates.
    DEFINE_BYREF_RW_PROPERTY(TPersistentAttributes, PersistentAttributes);

    // These fields calculated in preupdate and used for update.
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceDemand);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceUsageAtUpdate);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceLimits);

    // Used for profiling in snapshotted version.
    DEFINE_BYREF_RW_PROPERTY(int, SchedulableElementCount, 0);
    DEFINE_BYREF_RW_PROPERTY(int, SchedulablePoolCount, 0);
    DEFINE_BYREF_RW_PROPERTY(int, SchedulableOperationCount, 0);

    // Assigned in preupdate, used in schedule allocations.
    DEFINE_BYVAL_RO_PROPERTY(bool, Tentative, false);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TJobResources>, MaybeSpecifiedResourceLimits);

    // These fields are set in post update.
    DEFINE_BYREF_RO_PROPERTY(TResourceVector, LimitedDemandShare);

    // These fields are set in post update and used in schedule allocations.
    DEFINE_BYVAL_RO_PROPERTY(int, TreeIndex, UnassignedTreeIndex);

    DEFINE_BYVAL_RO_PROPERTY(double, EffectiveFairShareStarvationTolerance, 1.0);
    DEFINE_BYVAL_RO_PROPERTY(TDuration, EffectiveFairShareStarvationTimeout);
    DEFINE_BYVAL_RO_PROPERTY(bool, EffectiveAggressiveStarvationEnabled, false);

    DEFINE_BYVAL_RO_PROPERTY(TSchedulerElement*, LowestStarvingAncestor, nullptr);
    DEFINE_BYVAL_RO_PROPERTY(TSchedulerElement*, LowestAggressivelyStarvingAncestor, nullptr);

    DEFINE_BYREF_RO_PROPERTY(TSchedulerElementPostUpdateAttributes, PostUpdateAttributes);

    DEFINE_BYREF_RO_PROPERTY(TJobResourcesConfigPtr, EffectiveNonPreemptibleResourceUsageThresholdConfig);

protected:
    TSchedulerElementFixedState(
        ISchedulerStrategyHost* strategyHost,
        IFairShareTreeElementHost* treeElementHost,
        TFairShareStrategyTreeConfigPtr treeConfig,
        TString treeId);

    ISchedulerStrategyHost* const StrategyHost_;
    IFairShareTreeElementHost* const TreeElementHost_;

    // These fields calculated in preupdate and used for update.
    TJobResources SchedulingTagFilterResourceLimits_;

    // These attributes are calculated during fair share update and further used in schedule allocations.
    TSchedulableAttributes Attributes_;

    // Used everywhere.
    TSchedulerCompositeElement* Parent_ = nullptr;

    // Assigned in preupdate, used in fair share update.
    TJobResources TotalResourceLimits_;
    i64 PendingAllocationCount_ = 0;
    TInstant StartTime_;

    const TString TreeId_;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulerElement
    : public virtual NVectorHdrf::TElement
    , public TSchedulerElementFixedState
{
public:
    //! Common interface.
    virtual TSchedulerElementPtr Clone(TSchedulerCompositeElement* clonedParent) = 0;

    virtual ESchedulerElementType GetType() const = 0;

    virtual TString GetTreeId() const;

    const NLogging::TLogger& GetLogger() const override;
    bool AreDetailedLogsEnabled() const override;

    TString GetLoggingString(const TFairShareTreeSnapshotPtr& treeSnapshot) const;

    TSchedulerCompositeElement* GetMutableParent();
    const TSchedulerCompositeElement* GetParent() const;

    void InitAccumulatedResourceVolume(TResourceVolume resourceVolume);

    EStarvationStatus GetStarvationStatus() const;

    TJobResources GetInstantResourceUsage() const;

    virtual std::optional<double> GetSpecifiedFairShareStarvationTolerance() const = 0;
    virtual std::optional<TDuration> GetSpecifiedFairShareStarvationTimeout() const = 0;
    virtual std::optional<bool> IsAggressiveStarvationEnabled() const = 0;

    virtual TJobResourcesConfigPtr GetSpecifiedNonPreemptibleResourceUsageThresholdConfig() const = 0;

    virtual ESchedulableStatus GetStatus() const;

    virtual TJobResources GetSpecifiedStrongGuaranteeResources() const;
    virtual TResourceVector GetMaxShare() const = 0;

    double GetMaxShareRatio() const;
    double GetResourceDominantUsageShareAtUpdate() const;
    double GetAccumulatedResourceRatioVolume() const;
    TResourceVolume GetAccumulatedResourceVolume() const;

    bool IsStrictlyDominatesNonBlocked(const TResourceVector& lhs, const TResourceVector& rhs) const;

    //! Trunk node interface.
    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const;
    virtual void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config);

    //! Pre fair share update methods.
    // At this stage we prepare attributes that need to be computed in the control thread
    // in a thread-unsafe manner.
    virtual void PreUpdateBottomUp(NVectorHdrf::TFairShareUpdateContext* context);

    virtual TJobResourcesConfigPtr GetSpecifiedResourceLimitsConfig() const = 0;

    TJobResources GetSchedulingTagFilterResourceLimits() const;
    TJobResources GetTotalResourceLimits() const;
    TJobResources GetMaxShareResourceLimits() const;

    virtual void CollectResourceTreeOperationElements(std::vector<TResourceTreeElementPtr>* elements) const = 0;

    //! Fair share update methods that implements NVectorHdrf::TElement interface.
    const TJobResources& GetResourceDemand() const override;
    const TJobResources& GetResourceUsageAtUpdate() const override;
    const TJobResources& GetResourceLimits() const override;

    double GetWeight() const override;

    TSchedulableAttributes& Attributes() override;
    const TSchedulableAttributes& Attributes() const override;

    NVectorHdrf::TCompositeElement* GetParentElement() const override;
    const NVectorHdrf::TJobResourcesConfig* GetStrongGuaranteeResourcesConfig() const override;

    TInstant GetStartTime() const;

    //! Post fair share update methods.
    virtual void UpdateStarvationStatuses(TInstant now, bool enablePoolStarvation);
    virtual void MarkImmutable();

    virtual bool IsSchedulable() const = 0;

    //! Schedule allocations interface.
    double ComputeLocalSatisfactionRatio(const TJobResources& resourceUsage) const;

    // bool IsActive(const TDynamicAttributesList& dynamicAttributesList) const;
    std::optional<TJobResources> ComputeMaybeSpecifiedResourceLimits() const;
    bool AreSpecifiedResourceLimitsViolated() const;

    //! Resource tree methods.
    bool IsAlive() const;
    void SetNonAlive();
    TJobResources GetResourceUsageWithPrecommit() const;
    bool CheckAvailableDemand(const TJobResources& delta);

    //! Other methods based on tree snapshot.
    virtual void BuildResourceMetering(
        const std::optional<TMeteringKey>& parentKey,
        const THashMap<TString, TResourceVolume>& poolResourceUsages,
        TMeteringMap* meteringMap) const;

private:
    TResourceTreeElementPtr ResourceTreeElement_;

protected:
    NLogging::TLogger Logger;

    TSchedulerElement(
        ISchedulerStrategyHost* strategyHost,
        IFairShareTreeElementHost* treeElementHost,
        TFairShareStrategyTreeConfigPtr treeConfig,
        TString treeId,
        TString id,
        EResourceTreeElementKind elementKind,
        const NLogging::TLogger& logger);
    TSchedulerElement(
        const TSchedulerElement& other,
        TSchedulerCompositeElement* clonedParent);

    ISchedulerStrategyHost* GetHost() const;

    void SetOperationAlert(
        TOperationId operationId,
        EOperationAlertType alertType,
        const TError& alert,
        std::optional<TDuration> timeout);

    virtual void BuildLoggingStringAttributes(TDelimitedStringBuilderWrapper& delimitedBuilder) const;

    //! Pre update methods.
    virtual void DisableNonAliveElements() = 0;

    bool AreTotalResourceLimitsStable() const;

    TJobResources ComputeSchedulingTagFilterResourceLimits() const;
    TJobResources ComputeResourceLimits() const;

    //! Post update methods.
    virtual void UpdateRecursiveAttributes();

    virtual void SetStarvationStatus(EStarvationStatus starvationStatus);
    virtual void CheckForStarvation(TInstant now) = 0;

    ESchedulableStatus GetStatusImpl(double defaultTolerance) const;
    void CheckForStarvationImpl(
        TDuration fairShareStarvationTimeout,
        TDuration fairShareAggressiveStarvationTimeout,
        TInstant now);

    virtual void ComputeSatisfactionRatioAtUpdate();

    void ResetSchedulableCounters();

    virtual void BuildSchedulableChildrenLists(TFairSharePostUpdateContext* context) = 0;

    // Enumerates elements of the tree using inorder traversal. Returns first unused index.
    virtual int EnumerateElements(int startIndex, bool isSchedulableValueFilter);

    virtual void BuildElementMapping(TFairSharePostUpdateContext* context) = 0;

    bool IsResourceBlocked(EJobResourceType resource) const;
    bool AreAllResourcesBlocked() const;

private:
    // Update methods.
    virtual std::optional<double> GetSpecifiedWeight() const = 0;
    i64 GetPendingAllocationCount() const;

    friend class TSchedulerCompositeElement;
    friend class TSchedulerOperationElement;
    friend class TSchedulerPoolElement;
};

DEFINE_REFCOUNTED_TYPE(TSchedulerElement)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerCompositeElementFixedState
{
public:
    // Used only in trunk version and profiling.
    DEFINE_BYREF_RW_PROPERTY(int, OperationCount);
    DEFINE_BYREF_RW_PROPERTY(int, RunningOperationCount);
    DEFINE_BYREF_RW_PROPERTY(int, LightweightRunningOperationCount);
    DEFINE_BYREF_RW_PROPERTY(std::list<TOperationId>, PendingOperationIds);

    // Computed in fair share update and used in schedule allocations.
    DEFINE_BYREF_RO_PROPERTY(TNonOwningElementList, SchedulableChildren);

    // Computed in post update and used in schedule allocations.
    DEFINE_BYVAL_RO_PROPERTY(EFifoPoolSchedulingOrder, EffectiveFifoPoolSchedulingOrder);
    DEFINE_BYVAL_RO_PROPERTY(bool, EffectiveUsePoolSatisfactionForScheduling);

protected:
    // Used in fair share update.
    ESchedulingMode Mode_ = ESchedulingMode::Fifo;
    std::vector<EFifoSortParameter> FifoSortParameters_;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulerCompositeElement
    : public virtual NVectorHdrf::TCompositeElement
    , public TSchedulerElement
    , public TSchedulerCompositeElementFixedState
{
public:
    TSchedulerCompositeElement(
        ISchedulerStrategyHost* strategyHost,
        IFairShareTreeElementHost* treeElementHost,
        TFairShareStrategyTreeConfigPtr treeConfig,
        const TString& treeId,
        const TString& id,
        EResourceTreeElementKind elementKind,
        const NLogging::TLogger& logger);
    TSchedulerCompositeElement(
        const TSchedulerCompositeElement& other,
        TSchedulerCompositeElement* clonedParent);

    //! Common interface.
    void AddChild(TSchedulerElement* child, bool enabled = true);
    void EnableChild(const TSchedulerElementPtr& child);
    void DisableChild(const TSchedulerElementPtr& child);
    void RemoveChild(TSchedulerElement* child);
    bool IsEnabledChild(TSchedulerElement* child);

    void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config) override;

    const std::vector<TSchedulerElementPtr>& EnabledChildren() const;

    //! Trunk node interface.
    virtual int GetMaxOperationCount() const = 0;
    virtual int GetMaxRunningOperationCount() const = 0;
    int GetAvailableRunningOperationCount() const;

    virtual bool AreLightweightOperationsEnabled() const = 0;
    // NB(eshcherbin): This name was chosen for consistency with other "effective" attributes.
    bool GetEffectiveLightweightOperationsEnabled() const;

    virtual TPoolIntegralGuaranteesConfigPtr GetIntegralGuaranteesConfig() const = 0;

    void IncreaseOperationCount(int delta);
    void IncreaseRunningOperationCount(int delta);
    void IncreaseLightweightRunningOperationCount(int delta);

    virtual bool IsExplicit() const;
    virtual bool IsDefaultConfigured() const = 0;
    virtual bool AreImmediateOperationsForbidden() const = 0;
    virtual bool IsEphemeralHub() const = 0;

    bool IsEmpty() const;

    // For diagnostics purposes.
    TResourceVolume GetIntegralPoolCapacity() const;

    // For diagnostics purposes.
    virtual std::vector<EFifoSortParameter> GetFifoSortParameters() const = 0;

    //! Pre fair share update methods.
    void PreUpdateBottomUp(NVectorHdrf::TFairShareUpdateContext* context) override;

    //! Fair share update methods that implements NVectorHdrf::TCompositeElement interface.
    TElement* GetChild(int index) final;
    const TElement* GetChild(int index) const final;
    int GetChildCount() const final;

    std::vector<TSchedulerOperationElement*> GetChildOperations() const;
    int GetChildOperationCount() const noexcept;

    int GetChildPoolCount() const noexcept;

    ESchedulingMode GetMode() const final;
    bool HasHigherPriorityInFifoMode(const NVectorHdrf::TElement* lhs, const NVectorHdrf::TElement* rhs) const final;

    //! Post fair share update methods.
    void UpdateStarvationStatuses(TInstant now, bool enablePoolStarvation) override;
    void MarkImmutable() override;

    bool IsSchedulable() const override;

    virtual std::optional<EFifoPoolSchedulingOrder> GetSpecifiedFifoPoolSchedulingOrder() const = 0;
    virtual std::optional<bool> ShouldUsePoolSatisfactionForScheduling() const = 0;

    //! Schedule allocations related methods.
    bool ShouldUseFifoSchedulingOrder() const;
    bool HasHigherPriorityInFifoMode(const TSchedulerElement* lhs, const TSchedulerElement* rhs) const;

    NYPath::TYPath GetFullPath(bool explicitOnly, bool withTreeId = true) const;

    virtual TGuid GetObjectId() const = 0;

    //! Other methods.
    virtual THashSet<TString> GetAllowedProfilingTags() const = 0;

    virtual const TOffloadingSettings& GetOffloadingSettings() const = 0;

    virtual std::optional<bool> IsIdleCpuPolicyAllowed() const = 0;

protected:
    using TChildMap = THashMap<TSchedulerElementPtr, int>;
    using TChildList = std::vector<TSchedulerElementPtr>;

    // Supported in trunk version, used in fair share update.
    TChildMap EnabledChildToIndex_;
    TChildList EnabledChildren_;
    TChildList SortedEnabledChildren_;

    TChildMap DisabledChildToIndex_;
    TChildList DisabledChildren_;

    static void AddChild(TChildMap* map, TChildList* list, const TSchedulerElementPtr& child);
    static void RemoveChild(TChildMap* map, TChildList* list, const TSchedulerElementPtr& child);
    static bool ContainsChild(const TChildMap& map, const TSchedulerElementPtr& child);

    //! Pre fair share update methods.
    void DisableNonAliveElements() override;

    void CollectResourceTreeOperationElements(std::vector<TResourceTreeElementPtr>* elements) const override;

    //! Post fair share update methods.
    void UpdateRecursiveAttributes() override;

    void BuildSchedulableChildrenLists(TFairSharePostUpdateContext* context) override;

    int EnumerateElements(int startIndex, bool isSchedulableValueFilter) override;

    void ComputeSatisfactionRatioAtUpdate() override;

    void BuildElementMapping(TFairSharePostUpdateContext* context) override;

    // Used to implement GetWeight.
    virtual bool IsInferringChildrenWeightsFromHistoricUsageEnabled() const = 0;
    virtual TDuration GetHistoricUsageAggregatorPeriod() const = 0;

private:
    friend class TSchedulerElement;
    friend class TSchedulerOperationElement;
    friend class TSchedulerRootElement;

    void DoIncreaseOperationCount(int delta, int TSchedulerCompositeElement::* operationCounter);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerCompositeElement)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerPoolElementFixedState
{
protected:
    TSchedulerPoolElementFixedState(TString id, NObjectClient::TObjectId objectId);

    const TString Id_;

    // Used only in trunk node.
    bool DefaultConfigured_ = true;
    bool EphemeralInDefaultParentPool_ = false;
    std::optional<TString> UserName_;
    NObjectClient::TObjectId ObjectId_;

    // Used in preupdate.
    TSchedulingTagFilter SchedulingTagFilter_;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulerPoolElement
    : public NVectorHdrf::TPool
    , public TSchedulerCompositeElement
    , public TSchedulerPoolElementFixedState
{
public:
    TSchedulerPoolElement(
        ISchedulerStrategyHost* strategyHost,
        IFairShareTreeElementHost* treeElementHost,
        const TString& id,
        TGuid objectId,
        TPoolConfigPtr config,
        bool defaultConfigured,
        TFairShareStrategyTreeConfigPtr treeConfig,
        const TString& treeId,
        const NLogging::TLogger& logger);
    TSchedulerPoolElement(
        const TSchedulerPoolElement& other,
        TSchedulerCompositeElement* clonedParent);

    //! Common interface.
    TSchedulerElementPtr Clone(TSchedulerCompositeElement* clonedParent) override;

    ESchedulerElementType GetType() const override;

    TString GetId() const override;

    void AttachParent(TSchedulerCompositeElement* newParent);
    void ChangeParent(TSchedulerCompositeElement* newParent);
    void DetachParent();

    ESchedulableStatus GetStatus() const override;

    // Used for diagnostics only.
    TResourceVector GetMaxShare() const override;

    //! Trunk node interface.
    TPoolConfigPtr GetConfig() const;
    void SetConfig(TPoolConfigPtr config);
    void SetDefaultConfig();
    void SetObjectId(NObjectClient::TObjectId objectId);

    void SetUserName(const std::optional<TString>& userName);
    const std::optional<TString>& GetUserName() const;

    int GetMaxOperationCount() const override;
    int GetMaxRunningOperationCount() const override;

    bool AreLightweightOperationsEnabled() const override;

    TPoolIntegralGuaranteesConfigPtr GetIntegralGuaranteesConfig() const override;

    void SetEphemeralInDefaultParentPool();
    bool IsEphemeralInDefaultParentPool() const;

    bool IsExplicit() const override;
    bool IsDefaultConfigured() const override;
    bool AreImmediateOperationsForbidden() const override;
    bool IsEphemeralHub() const override;

    std::vector<EFifoSortParameter> GetFifoSortParameters() const override;

    const TSchedulingTagFilter& GetSchedulingTagFilter() const override;

    //! Fair share update methods that implements NVectorHdrf::TPool interface.
    bool AreDetailedLogsEnabled() const final;
    const NVectorHdrf::TJobResourcesConfig* GetStrongGuaranteeResourcesConfig() const override;

    double GetSpecifiedBurstRatio() const override;
    double GetSpecifiedResourceFlowRatio() const override;

    EIntegralGuaranteeType GetIntegralGuaranteeType() const override;
    TResourceVector GetIntegralShareLimitForRelaxedPool() const override;
    bool CanAcceptFreeVolume() const override;
    bool ShouldDistributeFreeVolumeAmongChildren() const override;

    const TIntegralResourcesState& IntegralResourcesState() const override;
    TIntegralResourcesState& IntegralResourcesState() override;

    bool IsFairShareTruncationInFifoPoolEnabled() const override;

    bool ShouldComputePromisedGuaranteeFairShare() const override;

    //! Post fair share update methods.
    std::optional<double> GetSpecifiedFairShareStarvationTolerance() const override;
    std::optional<TDuration> GetSpecifiedFairShareStarvationTimeout() const override;
    std::optional<bool> IsAggressiveStarvationEnabled() const override;

    TJobResourcesConfigPtr GetSpecifiedNonPreemptibleResourceUsageThresholdConfig() const override;

    std::optional<EFifoPoolSchedulingOrder> GetSpecifiedFifoPoolSchedulingOrder() const override;
    std::optional<bool> ShouldUsePoolSatisfactionForScheduling() const override;

    //! Other methods.
    void BuildResourceMetering(
        const std::optional<TMeteringKey>& parentKey,
        const THashMap<TString, TResourceVolume>& poolResourceUsages,
        TMeteringMap* meteringMap) const override;

    THashSet<TString> GetAllowedProfilingTags() const override;

    TGuid GetObjectId() const override;

    const TOffloadingSettings& GetOffloadingSettings() const override;

    std::optional<bool> IsIdleCpuPolicyAllowed() const override;

protected:
    //! Pre fair share update methods.
    TJobResourcesConfigPtr GetSpecifiedResourceLimitsConfig() const override;

    //! Post fair share update methods.
    void SetStarvationStatus(EStarvationStatus starvationStatus) override;
    void CheckForStarvation(TInstant now) override;

    void BuildElementMapping(TFairSharePostUpdateContext* context) override;

private:
    TPoolConfigPtr Config_;

    bool IsInferringChildrenWeightsFromHistoricUsageEnabled() const override;
    TDuration GetHistoricUsageAggregatorPeriod() const override;

    std::optional<double> GetSpecifiedWeight() const override;

    const TSchedulerCompositeElement* GetNearestAncestorWithResourceLimits(const TSchedulerCompositeElement* element) const;

    void DoSetConfig(TPoolConfigPtr newConfig);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerPoolElement)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerOperationElementFixedState
{
public:
    // Used by trunk node.
    DEFINE_BYREF_RW_PROPERTY(std::optional<TString>, PendingByPool);

    DEFINE_BYREF_RO_PROPERTY(TJobResourcesWithQuotaList, DetailedMinNeededAllocationResources);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, AggregatedMinNeededAllocationResources);
    DEFINE_BYREF_RO_PROPERTY(bool, ScheduleAllocationBackoffCheckEnabled, false);

    DEFINE_BYREF_RO_PROPERTY(THashSet<int>, DiskRequestMedia);

    DEFINE_BYVAL_RO_PROPERTY(std::optional<EUnschedulableReason>, UnschedulableReason);

protected:
    TSchedulerOperationElementFixedState(
        IOperationStrategyHost* operation,
        TFairShareStrategyOperationControllerConfigPtr controllerConfig,
        TSchedulingTagFilter schedulingTagFilter);

    const TOperationId OperationId_;

    IOperationStrategyHost* const OperationHost_;
    TFairShareStrategyOperationControllerConfigPtr ControllerConfig_;

    // Used only in trunk version.
    TString UserName_;

    // Used for accumulated usage logging.
    EOperationType Type_;
    NYson::TYsonString TrimmedAnnotations_;

    // Used only for profiling.
    int SlotIndex_ = UndefinedSlotIndex;

    // Used to compute operation demand.
    TJobResources TotalNeededResources_;

    // Used in trunk node.
    bool RunningInThisPoolTree_ = false;

    // Fixed in preupdate and used to calculate resource limits.
    TSchedulingTagFilter SchedulingTagFilter_;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulerOperationElement
    : public NVectorHdrf::TOperationElement
    , public TSchedulerElement
    , public TSchedulerOperationElementFixedState
{
public:
    DEFINE_BYREF_RO_PROPERTY(TStrategyOperationSpecPtr, Spec);

public:
    TSchedulerOperationElement(
        TFairShareStrategyTreeConfigPtr treeConfig,
        TStrategyOperationSpecPtr spec,
        TOperationFairShareTreeRuntimeParametersPtr runtimeParameters,
        TFairShareStrategyOperationControllerPtr controller,
        TFairShareStrategyOperationControllerConfigPtr controllerConfig,
        TFairShareStrategyOperationStatePtr state,
        ISchedulerStrategyHost* strategyHost,
        IFairShareTreeElementHost* treeElementHost,
        IOperationStrategyHost* operation,
        const TString& treeId,
        const NLogging::TLogger& logger);
    TSchedulerOperationElement(
        const TSchedulerOperationElement& other,
        TSchedulerCompositeElement* clonedParent);

    //! Common interface.
    TSchedulerElementPtr Clone(TSchedulerCompositeElement* clonedParent) override;

    ESchedulerElementType GetType() const override;

    TString GetId() const override;
    TOperationId GetOperationId() const;

    void SetRuntimeParameters(TOperationFairShareTreeRuntimeParametersPtr runtimeParameters);
    TOperationFairShareTreeRuntimeParametersPtr GetRuntimeParameters() const;

    void BuildLoggingStringAttributes(TDelimitedStringBuilderWrapper& delimitedBuilder) const override;
    bool AreDetailedLogsEnabled() const final;

    ESchedulableStatus GetStatus() const override;

    void UpdateControllerConfig(const TFairShareStrategyOperationControllerConfigPtr& config);

    const NVectorHdrf::TJobResourcesConfig* GetStrongGuaranteeResourcesConfig() const override;
    TResourceVector GetMaxShare() const override;

    const TFairShareStrategyOperationStatePtr& GetFairShareStrategyOperationState() const;

    //! Trunk node interface.
    int GetSlotIndex() const;

    const TSchedulingTagFilter& GetSchedulingTagFilter() const override;

    TString GetUserName() const;
    EOperationType GetOperationType() const;
    const NYson::TYsonString& GetTrimmedAnnotations() const;

    void MarkOperationRunningInPool();
    bool IsOperationRunningInPool() const;

    bool IsLightweightEligible() const;
    bool IsLightweight() const;

    void MarkPendingBy(TSchedulerCompositeElement* violatedPool);

    void AttachParent(TSchedulerCompositeElement* newParent, int slotIndex);
    void ChangeParent(TSchedulerCompositeElement* newParent, int slotIndex);
    void DetachParent();

    //! Pre fair share update methods.
    void PreUpdateBottomUp(NVectorHdrf::TFairShareUpdateContext* context) override;

    //! Fair share update methods that implements NVectorHdrf::TOperationElement interface.
    TResourceVector GetBestAllocationShare() const override;
    bool IsGang() const override;

    //! Post fair share update methods.
    TInstant GetLastNonStarvingTime() const;

    std::optional<double> GetSpecifiedFairShareStarvationTolerance() const override;
    std::optional<TDuration> GetSpecifiedFairShareStarvationTimeout() const override;
    std::optional<bool> IsAggressiveStarvationEnabled() const override;

    TJobResourcesConfigPtr GetSpecifiedNonPreemptibleResourceUsageThresholdConfig() const override;

    bool IsSchedulable() const override;

    //! Controller related methods.
    // TODO(eshcherbin): Maybe expose controller itself in the API?
    TControllerEpoch GetControllerEpoch() const;

    void OnScheduleAllocationStarted(const ISchedulingContextPtr& schedulingContext);
    void OnScheduleAllocationFinished(const ISchedulingContextPtr& schedulingContext);

    bool IsMaxScheduleAllocationCallsViolated() const;
    bool IsMaxConcurrentScheduleAllocationCallsPerNodeShardViolated(const ISchedulingContextPtr& schedulingContext) const;
    bool IsMaxConcurrentScheduleAllocationExecDurationPerNodeShardViolated(const ISchedulingContextPtr& schedulingContext) const;
    bool HasRecentScheduleAllocationFailure(NProfiling::TCpuInstant now) const;
    bool IsSaturatedInTentativeTree(
        NProfiling::TCpuInstant now,
        const TString& treeId,
        TDuration saturationDeactivationTimeout) const;

    // TODO(eshcherbin): Rename?
    TControllerScheduleAllocationResultPtr ScheduleAllocation(
        const ISchedulingContextPtr& context,
        const TJobResources& availableResources,
        const TDiskResources& availableDiskResources,
        TDuration timeLimit,
        const TString& treeId,
        const TFairShareStrategyTreeConfigPtr& treeConfig);
    void OnScheduleAllocationFailed(
        NProfiling::TCpuInstant now,
        const TString& treeId,
        const TControllerScheduleAllocationResultPtr& scheduleAllocationResult);
    void AbortAllocation(
        TAllocationId allocationId,
        EAbortReason abortReason,
        TControllerEpoch allocationEpoch);

    TJobResources GetAggregatedInitialMinNeededResources() const;

    //! Resource tree methods.
    EResourceTreeIncreaseResult TryIncreaseHierarchicalResourceUsagePrecommit(
        const TJobResources& delta,
        TJobResources* availableResourceLimitsOutput = nullptr);
    void IncreaseHierarchicalResourceUsage(const TJobResources& delta);
    void DecreaseHierarchicalResourceUsagePrecommit(const TJobResources& precommittedResources);
    void CommitHierarchicalResourceUsage(const TJobResources& resourceUsage, const TJobResources& precommitedResources);
    void ReleaseResources(bool markAsNonAlive);

    //! Other methods.
    std::optional<TString> GetCustomProfilingTag() const;

    bool IsLimitingAncestorCheckEnabled() const;

    bool IsIdleCpuPolicyAllowed() const;

protected:
    //! Pre update methods.
    void CollectResourceTreeOperationElements(std::vector<TResourceTreeElementPtr>* elements) const override;

    //! Post update methods.
    void SetStarvationStatus(EStarvationStatus starvationStatus) override;
    void CheckForStarvation(TInstant now) override;

    void UpdateRecursiveAttributes() override;

    void OnFifoSchedulableElementCountLimitReached(TFairSharePostUpdateContext* context);
    void BuildSchedulableChildrenLists(TFairSharePostUpdateContext* context) override;

    void BuildElementMapping(TFairSharePostUpdateContext* context) override;

private:
    TOperationFairShareTreeRuntimeParametersPtr RuntimeParameters_;

    const TFairShareStrategyOperationControllerPtr Controller_;
    const TFairShareStrategyOperationStatePtr FairShareStrategyOperationState_;

    std::optional<double> GetSpecifiedWeight() const override;

    //! Pre fair share update methods.
    void DisableNonAliveElements() override;

    std::optional<EUnschedulableReason> ComputeUnschedulableReason() const;

    TJobResources ComputeResourceDemand() const;

    TJobResourcesConfigPtr GetSpecifiedResourceLimitsConfig() const override;

    friend class TSchedulerCompositeElement;
};

DEFINE_REFCOUNTED_TYPE(TSchedulerOperationElement)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerRootElementFixedState
{
public:
    // TODO(ignat): move it to TFairShareTreeSnapshot.
    DEFINE_BYVAL_RO_PROPERTY(int, TreeSize);
};

class TSchedulerRootElement
    : public NVectorHdrf::TRootElement
    , public TSchedulerCompositeElement
    , public TSchedulerRootElementFixedState
{
public:
    TSchedulerRootElement(
        ISchedulerStrategyHost* strategyHost,
        IFairShareTreeElementHost* treeElementHost,
        TFairShareStrategyTreeConfigPtr treeConfig,
        const TString& treeId,
        const NLogging::TLogger& logger);
    TSchedulerRootElement(const TSchedulerRootElement& other);

    //! Common interface.
    TString GetId() const override;

    TSchedulerRootElementPtr Clone();

    TSchedulerElementPtr Clone(TSchedulerCompositeElement* clonedParent) override;

    ESchedulerElementType GetType() const override;

    // Used for diagnostics purposes.
    TJobResources GetSpecifiedStrongGuaranteeResources() const override;
    TResourceVector GetMaxShare() const override;

    std::vector<EFifoSortParameter> GetFifoSortParameters() const override;

    //! Trunk node interface.
    int GetMaxRunningOperationCount() const override;
    int GetMaxOperationCount() const override;

    bool AreLightweightOperationsEnabled() const override;

    TPoolIntegralGuaranteesConfigPtr GetIntegralGuaranteesConfig() const override;

    bool IsDefaultConfigured() const override;
    bool AreImmediateOperationsForbidden() const override;
    bool IsEphemeralHub() const override;

    const TSchedulingTagFilter& GetSchedulingTagFilter() const override;

    //! Pre fair share update methods.
    // Computes various lightweight attributes in the tree. Must be called in control thread.
    void PreUpdate(NVectorHdrf::TFairShareUpdateContext* context);

    //! Fair share update methods that implements NVectorHdrf::TRootElement interface.
    double GetSpecifiedBurstRatio() const override;
    double GetSpecifiedResourceFlowRatio() const override;

    bool IsFairShareTruncationInFifoPoolEnabled() const override;

    bool ShouldComputePromisedGuaranteeFairShare() const override;

    //! Post update methods.
    void PostUpdate(TFairSharePostUpdateContext* postUpdateContext);

    std::optional<double> GetSpecifiedFairShareStarvationTolerance() const override;
    std::optional<TDuration> GetSpecifiedFairShareStarvationTimeout() const override;
    std::optional<bool> IsAggressiveStarvationEnabled() const override;

    TJobResourcesConfigPtr GetSpecifiedNonPreemptibleResourceUsageThresholdConfig() const override;

    std::optional<EFifoPoolSchedulingOrder> GetSpecifiedFifoPoolSchedulingOrder() const override;
    std::optional<bool> ShouldUsePoolSatisfactionForScheduling() const override;

    void BuildPoolSatisfactionDigests(TFairSharePostUpdateContext* postUpdateContext);

    //! Other methods.
    THashSet<TString> GetAllowedProfilingTags() const override;

    void BuildResourceMetering(
        const std::optional<TMeteringKey>& parentKey,
        const THashMap<TString, TResourceVolume>& poolResourceUsages,
        TMeteringMap* meteringMap) const override;

    TResourceDistributionInfo GetResourceDistributionInfo() const;
    void BuildResourceDistributionInfo(NYTree::TFluentMap fluent) const;

    TGuid GetObjectId() const override;

    const TOffloadingSettings& GetOffloadingSettings() const override;

    std::optional<bool> IsIdleCpuPolicyAllowed() const override;

protected:
    //! Post update methods.
    void CheckForStarvation(TInstant now) override;

private:
    // Pre fair share update methods.
    std::optional<double> GetSpecifiedWeight() const override;

    TJobResourcesConfigPtr GetSpecifiedResourceLimitsConfig() const override;

    bool IsInferringChildrenWeightsFromHistoricUsageEnabled() const override;
    TDuration GetHistoricUsageAggregatorPeriod() const override;

    bool CanAcceptFreeVolume() const override;
    bool ShouldDistributeFreeVolumeAmongChildren() const override;
};

DEFINE_REFCOUNTED_TYPE(TSchedulerRootElement)

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Use in all relevant places.
template <class TAttributes>
const TAttributes& GetSchedulerElementAttributesFromVector(const std::vector<TAttributes>& vector, const TSchedulerElement* element);
template <class TAttributes>
TAttributes& GetSchedulerElementAttributesFromVector(std::vector<TAttributes>& vector, const TSchedulerElement* element);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

////////////////////////////////////////////////////////////////////////////////

#define YT_ELEMENT_LOG_DETAILED(schedulerElement, ...) \
    do { \
        const auto& Logger = schedulerElement->GetLogger(); \
        if (schedulerElement->AreDetailedLogsEnabled()) { \
            YT_LOG_DEBUG(__VA_ARGS__); \
        } else { \
            YT_LOG_TRACE(__VA_ARGS__); \
        } \
    } while(false)

////////////////////////////////////////////////////////////////////////////////

#define FAIR_SHARE_TREE_ELEMENT_INL_H_
#include "fair_share_tree_element-inl.h"
#undef FAIR_SHARE_TREE_ELEMENT_INL_H_

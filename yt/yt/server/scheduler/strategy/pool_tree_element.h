#pragma once

#include "private.h"
#include "resource_tree.h"
#include "resource_tree_element.h"
#include "strategy.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/job_metrics.h>
#include <yt/yt/server/lib/scheduler/scheduling_tag.h>
#include <yt/yt/server/lib/scheduler/resource_metering.h>

#include <yt/yt/ytlib/scheduler/config.h>
#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/misc/adjusted_exponential_moving_average.h>

#include <yt/yt/library/vector_hdrf/resource_vector.h>

#include <yt/yt/library/vector_hdrf/fair_share_update.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>
#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NScheduler::NStrategy {

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

DEFINE_ENUM(EStarvationChangeReason,
    (FairShareDecreased)
    (UsageIncreased)
);

////////////////////////////////////////////////////////////////////////////////

struct IPoolTreeElementHost
    : public virtual TRefCounted
{
    virtual TResourceTree* GetResourceTree() = 0;

    virtual void BuildElementLoggingStringAttributes(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TPoolTreeElement* element,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IPoolTreeElementHost)

////////////////////////////////////////////////////////////////////////////////

struct TStarvationInterval
{
    const TDuration Duration;
    const EStarvationChangeReason Reason;
};

////////////////////////////////////////////////////////////////////////////////

//! Attributes that are kept between fair share updates.
struct TPersistentAttributes
{
    EStarvationStatus StarvationStatus;
    TInstant LastNonStarvingTime = TInstant::Now();
    std::optional<TInstant> BelowFairShareSince;
    std::optional<TInstant> StarvingSince;
    std::optional<TResourceVector> FairShareOnStarvationStart;
    std::optional<TResourceVector> UsageOnStarvationStart;
    TAdjustedExponentialMovingAverage HistoricUsageAggregator = TAdjustedExponentialMovingAverage();

    TResourceVector BestAllocationShare = TResourceVector::Ones();
    TInstant LastBestAllocationShareUpdateTime;

    TIntegralResourcesState IntegralResourcesState;

    std::optional<TJobResources> AppliedSpecifiedResourceLimits;
    TJobResources AppliedSpecifiedResourceLimitsOvercommitTolerance;

    void ResetOnElementEnabled();
};

////////////////////////////////////////////////////////////////////////////////

struct TFairSharePreUpdateContext
{
    const TInstant Now;
    const TJobResources TotalResourceLimits;
    TJobResourcesByTagFilter ResourceLimitsByTagFilter;
};

////////////////////////////////////////////////////////////////////////////////

struct TFairSharePostUpdateContext
{
    const TStrategyTreeConfigPtr& TreeConfig;
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

struct TPoolTreeElementPostUpdateAttributes
{
    TJobResources UnschedulableOperationsResourceUsage;

    double SatisfactionRatio = 0.0;
    double LocalSatisfactionRatio = 0.0;

    // Only for pools.
    IDigestPtr SatisfactionDigest;

    std::optional<TStarvationInterval> StarvationInterval;
};

////////////////////////////////////////////////////////////////////////////////

class TPoolTreeElementFixedState
{
public:
    // Tree config.
    DEFINE_BYREF_RO_PROPERTY(TStrategyTreeConfigPtr, TreeConfig);

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
    DEFINE_BYREF_RO_PROPERTY(TJobResources, SpecifiedResourceLimitsOvercommitTolerance);

    // These fields are set in post update.
    DEFINE_BYREF_RO_PROPERTY(TResourceVector, LimitedDemandShare);

    // These fields are set in post update and used in schedule allocations.
    DEFINE_BYVAL_RO_PROPERTY(int, TreeIndex, UnassignedTreeIndex);

    DEFINE_BYVAL_RO_PROPERTY(double, EffectiveFairShareStarvationTolerance, 1.0);
    DEFINE_BYVAL_RO_PROPERTY(TDuration, EffectiveFairShareStarvationTimeout);
    DEFINE_BYVAL_RO_PROPERTY(bool, EffectiveAggressiveStarvationEnabled, false);
    DEFINE_BYVAL_RO_PROPERTY(std::optional<TDuration>, EffectiveWaitingForResourcesOnNodeTimeout);

    DEFINE_BYVAL_RO_PROPERTY(TPoolTreeElement*, LowestStarvingAncestor, nullptr);
    DEFINE_BYVAL_RO_PROPERTY(TPoolTreeElement*, LowestAggressivelyStarvingAncestor, nullptr);

    DEFINE_BYREF_RO_PROPERTY(TPoolTreeElementPostUpdateAttributes, PostUpdateAttributes);

    // TODO(eshcherbin): Move this to allocation scheduler.
    // Currently it's painful to do, because this attribute is used in operation shared state,
    // where we don't have static attributes.
    DEFINE_BYREF_RO_PROPERTY(TJobResourcesConfigPtr, EffectiveNonPreemptibleResourceUsageThresholdConfig);

protected:
    TPoolTreeElementFixedState(
        IStrategyHost* strategyHost,
        IPoolTreeElementHost* treeElementHost,
        TStrategyTreeConfigPtr treeConfig,
        std::string treeId);

    IStrategyHost* const StrategyHost_;
    IPoolTreeElementHost* const TreeElementHost_;

    // These fields calculated in preupdate and used for update.
    TJobResources SchedulingTagFilterResourceLimits_;

    // These attributes are calculated during fair share update and further used in schedule allocations.
    TSchedulableAttributes Attributes_;

    // Used everywhere.
    TPoolTreeCompositeElement* Parent_ = nullptr;

    // Assigned in preupdate, used in fair share update.
    TJobResources TotalResourceLimits_;
    i64 PendingAllocationCount_ = 0;

    const std::string TreeId_;
};

////////////////////////////////////////////////////////////////////////////////

class TPoolTreeElement
    : public virtual NVectorHdrf::TElement
    , public TPoolTreeElementFixedState
{
public:
    //! Common interface.
    virtual TPoolTreeElementPtr Clone(TPoolTreeCompositeElement* clonedParent) = 0;

    virtual ESchedulerElementType GetType() const = 0;

    virtual std::string GetTreeId() const;

    const NLogging::TLogger& GetLogger() const override;
    bool AreDetailedLogsEnabled() const override;

    TString GetLoggingString(const TPoolTreeSnapshotPtr& treeSnapshot) const;

    TPoolTreeCompositeElement* GetMutableParent();
    const TPoolTreeCompositeElement* GetParent() const;

    void InitAccumulatedResourceVolume(TResourceVolume resourceVolume);

    EStarvationStatus GetStarvationStatus() const;

    TJobResources GetInstantResourceUsage(bool withPrecommit = false) const;
    TResourceTreeElement::TDetailedResourceUsage GetInstantDetailedResourceUsage() const;

    virtual std::optional<double> GetSpecifiedFairShareStarvationTolerance() const = 0;
    virtual std::optional<TDuration> GetSpecifiedFairShareStarvationTimeout() const = 0;

    virtual TJobResourcesConfigPtr GetSpecifiedNonPreemptibleResourceUsageThresholdConfig() const = 0;

    virtual ESchedulableStatus GetStatus() const;

    virtual TJobResources GetSpecifiedStrongGuaranteeResources() const;

    virtual TJobResources GetSpecifiedResourceFlow() const;
    virtual TJobResources GetSpecifiedBurstGuaranteeResources() const;

    double GetResourceDominantUsageShareAtUpdate() const;
    double GetAccumulatedResourceRatioVolume() const;
    TResourceVolume GetAccumulatedResourceVolume() const;

    bool IsStrictlyDominatesNonBlocked(const TResourceVector& lhs, const TResourceVector& rhs) const;

    //! Trunk node interface.
    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const;
    virtual void UpdateTreeConfig(const TStrategyTreeConfigPtr& config);

    //! Fair share update initialization method.
    // At this stage we prepare attributes that need to be computed in the control thread
    // in a thread-unsafe manner.
    virtual void InitializeUpdate(TInstant now);

    //! PreUpdate method prepares heavy attributes for fair share update in offloaded invoker.
    virtual void PreUpdate(TFairSharePreUpdateContext* context);

    //! Const getters which are used in InitializeUpdate and PreUpdate methods.
    TJobResources GetSchedulingTagFilterResourceLimits() const;
    TJobResources GetTotalResourceLimits() const;

    virtual TJobResourcesConfigPtr GetSpecifiedResourceLimitsConfig() const = 0;
    virtual TJobResourcesConfigPtr GetSpecifiedResourceLimitsOvercommitToleranceConfig() const;

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

    //! Post fair share update methods.
    virtual void UpdateStarvationStatuses(TInstant now, bool enablePoolStarvation);
    virtual void MarkImmutable();

    virtual bool IsSchedulable() const = 0;

    //! Schedule allocations interface.
    double ComputeLocalSatisfactionRatio(const TJobResources& resourceUsage) const;

    std::optional<TJobResources> ComputeMaybeSpecifiedResourceLimits() const;
    TJobResources ComputeSpecifiedResourceLimitsOvercommitTolerance() const;
    bool AreSpecifiedResourceLimitsViolated() const;

    //! Resource tree methods.
    bool IsAlive() const;
    void SetNonAlive();
    TJobResources GetResourceUsageWithPrecommit() const;
    bool CheckAvailableDemand(const TJobResources& delta);

    //! Other methods based on tree snapshot.
    virtual void BuildResourceMetering(
        const std::optional<TMeteringKey>& lowestMeteredAncestorKey,
        const THashMap<TString, TResourceVolume>& poolResourceUsages,
        TMeteringMap* meteringMap) const;

    bool IsDemandFullySatisfied() const;

private:
    TResourceTreeElementPtr ResourceTreeElement_;

protected:
    NLogging::TLogger Logger;

    TPoolTreeElement(
        IStrategyHost* strategyHost,
        IPoolTreeElementHost* treeElementHost,
        TStrategyTreeConfigPtr treeConfig,
        std::string treeId,
        TString id,
        EResourceTreeElementKind elementKind,
        const NLogging::TLogger& logger);
    TPoolTreeElement(
        const TPoolTreeElement& other,
        TPoolTreeCompositeElement* clonedParent);

    IStrategyHost* GetHost() const;

    void SetOperationAlert(
        TOperationId operationId,
        EOperationAlertType alertType,
        const TError& alert,
        std::optional<TDuration> timeout);

    virtual void BuildLoggingStringAttributes(TDelimitedStringBuilderWrapper& delimitedBuilder) const;

    //! Pre update methods.
    virtual void DisableNonAliveElements() = 0;

    bool AreTotalResourceLimitsStable() const;

    TJobResources ComputeSchedulingTagFilterResourceLimits(TFairSharePreUpdateContext* context) const;

    TJobResources ComputeResourceLimits() const;

    //! Post update methods.
    virtual void UpdateRecursiveAttributes();

    virtual void SetStarvationStatus(EStarvationStatus starvationStatus, TInstant now);
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

    friend class TPoolTreeCompositeElement;
    friend class TPoolTreeOperationElement;
    friend class TPoolTreePoolElement;
};

DEFINE_REFCOUNTED_TYPE(TPoolTreeElement)

////////////////////////////////////////////////////////////////////////////////

class TPoolTreeCompositeElementFixedState
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
    // Something was here, but now it's gone.

protected:
    // Used in fair share update.
    ESchedulingMode Mode_ = ESchedulingMode::Fifo;
    std::vector<EFifoSortParameter> FifoSortParameters_;
};

////////////////////////////////////////////////////////////////////////////////

class TPoolTreeCompositeElement
    : public virtual NVectorHdrf::TCompositeElement
    , public TPoolTreeElement
    , public TPoolTreeCompositeElementFixedState
{
public:
    TPoolTreeCompositeElement(
        IStrategyHost* strategyHost,
        IPoolTreeElementHost* treeElementHost,
        TStrategyTreeConfigPtr treeConfig,
        const std::string& treeId,
        const TString& id,
        EResourceTreeElementKind elementKind,
        const NLogging::TLogger& logger);
    TPoolTreeCompositeElement(
        const TPoolTreeCompositeElement& other,
        TPoolTreeCompositeElement* clonedParent);

    //! Common interface.
    void AddChild(TPoolTreeElement* child, bool enabled = true);
    void EnableChild(const TPoolTreeElementPtr& child);
    void DisableChild(const TPoolTreeElementPtr& child);
    void RemoveChild(TPoolTreeElement* child);
    bool IsEnabledChild(TPoolTreeElement* child);

    void UpdateTreeConfig(const TStrategyTreeConfigPtr& config) override;

    const std::vector<TPoolTreeElementPtr>& EnabledChildren() const;

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
    virtual bool AreGangOperationsAllowed() const = 0;
    virtual bool IsEphemeralHub() const = 0;

    bool IsEmpty() const;

    // For diagnostics purposes.
    TResourceVolume GetIntegralPoolCapacity() const;

    // For diagnostics purposes.
    virtual std::vector<EFifoSortParameter> GetFifoSortParameters() const = 0;

    //! Pre fair share update methods.
    void InitializeUpdate(TInstant now) override;
    void PreUpdate(TFairSharePreUpdateContext* context) override;

    //! Fair share update methods that implements NVectorHdrf::TCompositeElement interface.
    TElement* GetChild(int index) final;
    const TElement* GetChild(int index) const final;
    int GetChildCount() const final;

    std::vector<TPoolTreeOperationElement*> GetChildOperations() const;
    int GetChildOperationCount() const noexcept;

    int GetChildPoolCount() const noexcept;

    ESchedulingMode GetMode() const final;
    bool HasHigherPriorityInFifoMode(const NVectorHdrf::TElement* lhs, const NVectorHdrf::TElement* rhs) const final;

    bool IsStepFunctionForGangOperationsEnabled() const override;

    //! Post fair share update methods.
    void UpdateStarvationStatuses(TInstant now, bool enablePoolStarvation) override;
    void MarkImmutable() override;

    bool IsSchedulable() const override;

    virtual std::optional<bool> IsAggressiveStarvationEnabled() const = 0;

    virtual std::optional<TDuration> GetSpecifiedWaitingForResourcesOnNodeTimeout() const = 0;

    //! Schedule allocations related methods.
    bool HasHigherPriorityInFifoMode(const TPoolTreeOperationElement* lhs, const TPoolTreeOperationElement* rhs) const;

    NYPath::TYPath GetFullPath(bool explicitOnly, bool withTreeId = true) const;

    virtual TGuid GetObjectId() const = 0;

    //! Other methods.
    virtual THashSet<TString> GetAllowedProfilingTags() const = 0;

    virtual const TOffloadingSettings& GetOffloadingSettings() const = 0;

    virtual std::optional<bool> IsIdleCpuPolicyAllowed() const = 0;

    virtual std::optional<std::string> GetRedirectToCluster() const;

protected:
    using TChildMap = THashMap<TPoolTreeElementPtr, int>;
    using TChildList = std::vector<TPoolTreeElementPtr>;

    // Supported in trunk version, used in fair share update.
    TChildMap EnabledChildToIndex_;
    TChildList EnabledChildren_;
    TChildList SortedEnabledChildren_;

    TChildMap DisabledChildToIndex_;
    TChildList DisabledChildren_;

    static void AddChild(TChildMap* map, TChildList* list, const TPoolTreeElementPtr& child);
    static void RemoveChild(TChildMap* map, TChildList* list, const TPoolTreeElementPtr& child);
    static bool ContainsChild(const TChildMap& map, const TPoolTreeElementPtr& child);

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
    virtual std::optional<TDuration> GetMaybeHistoricUsageAggregatorPeriod() const = 0;

private:
    friend class TPoolTreeElement;
    friend class TPoolTreeOperationElement;
    friend class TPoolTreeRootElement;

    void DoIncreaseOperationCount(int delta, int TPoolTreeCompositeElement::* operationCounter);
};

DEFINE_REFCOUNTED_TYPE(TPoolTreeCompositeElement)

////////////////////////////////////////////////////////////////////////////////

class TPoolTreePoolElementFixedState
{
protected:
    TPoolTreePoolElementFixedState(TString id, NObjectClient::TObjectId objectId);

    const TString Id_;

    // Used only in trunk node.
    bool DefaultConfigured_ = true;
    bool EphemeralInDefaultParentPool_ = false;
    std::optional<std::string> UserName_;
    NObjectClient::TObjectId ObjectId_;

    // Used in preupdate.
    TSchedulingTagFilter SchedulingTagFilter_;
};

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Think of better naming for pool tree element to remove this tautology.
class TPoolTreePoolElement
    : public NVectorHdrf::TPool
    , public TPoolTreeCompositeElement
    , public TPoolTreePoolElementFixedState
{
public:
    TPoolTreePoolElement(
        IStrategyHost* strategyHost,
        IPoolTreeElementHost* treeElementHost,
        const TString& id,
        TGuid objectId,
        TPoolConfigPtr config,
        bool defaultConfigured,
        TStrategyTreeConfigPtr treeConfig,
        const std::string& treeId,
        const NLogging::TLogger& logger);
    TPoolTreePoolElement(
        const TPoolTreePoolElement& other,
        TPoolTreeCompositeElement* clonedParent);

    //! Common interface.
    TPoolTreeElementPtr Clone(TPoolTreeCompositeElement* clonedParent) override;

    ESchedulerElementType GetType() const override;

    TString GetId() const override;

    void AttachParent(TPoolTreeCompositeElement* newParent);
    void ChangeParent(TPoolTreeCompositeElement* newParent);
    void DetachParent();

    ESchedulableStatus GetStatus() const override;

    //! Trunk node interface.
    TPoolConfigPtr GetConfig() const;
    void SetConfig(TPoolConfigPtr config);
    void SetDefaultConfig();
    void SetObjectId(NObjectClient::TObjectId objectId);

    void SetUserName(const std::optional<std::string>& userName);
    const std::optional<std::string>& GetUserName() const;

    int GetMaxOperationCount() const override;
    int GetMaxRunningOperationCount() const override;

    bool AreLightweightOperationsEnabled() const override;

    TPoolIntegralGuaranteesConfigPtr GetIntegralGuaranteesConfig() const override;

    TJobResources GetSpecifiedResourceFlow() const override;
    TJobResources GetSpecifiedBurstGuaranteeResources() const override;

    void SetEphemeralInDefaultParentPool();
    bool IsEphemeralInDefaultParentPool() const;

    bool IsExplicit() const override;
    bool IsDefaultConfigured() const override;
    bool AreImmediateOperationsForbidden() const override;
    bool AreGangOperationsAllowed() const override;
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

    bool IsStepFunctionForGangOperationsEnabled() const override;

    bool ShouldComputePromisedGuaranteeFairShare() const override;

    bool IsPriorityStrongGuaranteeAdjustmentEnabled() const override;
    bool IsPriorityStrongGuaranteeAdjustmentDonorshipEnabled() const override;

    //! Post fair share update methods.
    void UpdateRecursiveAttributes() override;

    std::optional<double> GetSpecifiedFairShareStarvationTolerance() const override;
    std::optional<TDuration> GetSpecifiedFairShareStarvationTimeout() const override;
    std::optional<bool> IsAggressiveStarvationEnabled() const override;

    TJobResourcesConfigPtr GetSpecifiedNonPreemptibleResourceUsageThresholdConfig() const override;

    std::optional<TDuration> GetSpecifiedWaitingForResourcesOnNodeTimeout() const override;

    //! Other methods.
    void BuildResourceMetering(
        const std::optional<TMeteringKey>& lowestMeteredAncestorKey,
        const THashMap<TString, TResourceVolume>& poolResourceUsages,
        TMeteringMap* meteringMap) const override;

    THashSet<TString> GetAllowedProfilingTags() const override;

    TGuid GetObjectId() const override;

    const TOffloadingSettings& GetOffloadingSettings() const override;

    std::optional<bool> IsIdleCpuPolicyAllowed() const override;

    std::optional<std::string> GetRedirectToCluster() const override;

protected:
    //! Pre fair share update methods.
    TJobResourcesConfigPtr GetSpecifiedResourceLimitsConfig() const override;
    TJobResourcesConfigPtr GetSpecifiedResourceLimitsOvercommitToleranceConfig() const override;

    //! Post fair share update methods.
    void SetStarvationStatus(EStarvationStatus starvationStatus, TInstant now) override;
    void CheckForStarvation(TInstant now) override;

    void BuildElementMapping(TFairSharePostUpdateContext* context) override;

private:
    TPoolConfigPtr Config_;

    std::optional<TDuration> GetMaybeHistoricUsageAggregatorPeriod() const override;

    std::optional<double> GetSpecifiedWeight() const override;

    const TPoolTreeCompositeElement* GetNearestAncestorWithResourceLimits(const TPoolTreeCompositeElement* element) const;

    void DoSetConfig(TPoolConfigPtr newConfig);

    void PropagatePoolAttributesToOperations();
};

DEFINE_REFCOUNTED_TYPE(TPoolTreePoolElement)

////////////////////////////////////////////////////////////////////////////////

class TPoolTreeOperationElementFixedState
{
public:
    // Used by trunk node.
    DEFINE_BYREF_RW_PROPERTY(std::optional<TString>, PendingByPool);

    DEFINE_BYREF_RO_PROPERTY(TAllocationGroupResourcesMap, GroupedNeededResources);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, AggregatedMinNeededAllocationResources);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, AggregatedInitialMinNeededAllocationResources);
    DEFINE_BYREF_RO_PROPERTY(bool, ScheduleAllocationBackoffCheckEnabled, false);

    DEFINE_BYREF_RO_PROPERTY(THashSet<int>, DiskRequestMedia);

    DEFINE_BYVAL_RO_PROPERTY(std::optional<EUnschedulableReason>, UnschedulableReason);

protected:
    TPoolTreeOperationElementFixedState(
        IOperationPtr operation,
        TStrategyOperationControllerConfigPtr controllerConfig,
        TSchedulingTagFilter schedulingTagFilter);

    const TOperationId OperationId_;

    IOperationPtr Operation_;
    TStrategyOperationControllerConfigPtr ControllerConfig_;

    // Used only in trunk version.
    std::string UserName_;

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

    TInstant StartTime_;
};

////////////////////////////////////////////////////////////////////////////////

class TPoolTreeOperationElement
    : public NVectorHdrf::TOperationElement
    , public TPoolTreeElement
    , public TPoolTreeOperationElementFixedState
{
public:
    DEFINE_BYREF_RO_PROPERTY(TStrategyOperationSpecPtr, Spec);
    DEFINE_BYREF_RO_PROPERTY(TOperationOptionsPtr, OperationOptions);

public:
    TPoolTreeOperationElement(
        TStrategyTreeConfigPtr treeConfig,
        TStrategyOperationSpecPtr spec,
        TOperationOptionsPtr operationOptions,
        TOperationPoolTreeRuntimeParametersPtr runtimeParameters,
        TOperationControllerPtr controller,
        TStrategyOperationControllerConfigPtr controllerConfig,
        TStrategyOperationStatePtr state,
        IStrategyHost* strategyHost,
        IPoolTreeElementHost* treeElementHost,
        IOperationPtr operation,
        const std::string& treeId,
        const NLogging::TLogger& logger);
    TPoolTreeOperationElement(
        const TPoolTreeOperationElement& other,
        TPoolTreeCompositeElement* clonedParent);

    //! Common interface.
    TPoolTreeElementPtr Clone(TPoolTreeCompositeElement* clonedParent) override;

    ESchedulerElementType GetType() const override;

    TInstant GetStartTime() const;

    TString GetId() const override;
    TOperationId GetOperationId() const;
    std::optional<std::string> GetTitle() const;

    void SetRuntimeParameters(TOperationPoolTreeRuntimeParametersPtr runtimeParameters);
    TOperationPoolTreeRuntimeParametersPtr GetRuntimeParameters() const;

    void UpdatePoolAttributes(bool runningInEphemeralPool);

    void BuildLoggingStringAttributes(TDelimitedStringBuilderWrapper& delimitedBuilder) const override;
    bool AreDetailedLogsEnabled() const final;

    ESchedulableStatus GetStatus() const override;

    void UpdateControllerConfig(const TStrategyOperationControllerConfigPtr& config);

    const NVectorHdrf::TJobResourcesConfig* GetStrongGuaranteeResourcesConfig() const override;

    const TStrategyOperationStatePtr& GetStrategyOperationState() const;

    //! Trunk node interface.
    int GetSlotIndex() const;

    void SetSchedulingTagFilter(TSchedulingTagFilter schedulingTagFilter);
    const TSchedulingTagFilter& GetSchedulingTagFilter() const override;

    std::string GetUserName() const;
    EOperationType GetOperationType() const;
    const NYson::TYsonString& GetTrimmedAnnotations() const;

    void MarkOperationRunningInPool();
    bool IsOperationRunningInPool() const;

    bool IsLightweightEligible() const;
    bool IsLightweight() const;

    void MarkPendingBy(TPoolTreeCompositeElement* violatedPool);

    void AttachParent(TPoolTreeCompositeElement* newParent, int slotIndex);
    void ChangeParent(TPoolTreeCompositeElement* newParent, int slotIndex);
    void DetachParent();

    //! Pre fair share update methods.
    void InitializeUpdate(TInstant now) override;
    void PreUpdate(TFairSharePreUpdateContext* context) override;

    //! Fair share update methods that implements NVectorHdrf::TOperationElement interface.
    TResourceVector GetBestAllocationShare() const override;
    bool IsGangLike() const override;

    //! Post fair share update methods.
    std::optional<TInstant> GetStarvingSince() const;

    std::optional<double> GetSpecifiedFairShareStarvationTolerance() const override;
    std::optional<TDuration> GetSpecifiedFairShareStarvationTimeout() const override;

    TJobResourcesConfigPtr GetSpecifiedNonPreemptibleResourceUsageThresholdConfig() const override;

    bool IsSchedulable() const override;

    //! Controller related methods.
    // TODO(eshcherbin): Maybe expose controller itself in the API?
    TControllerEpoch GetControllerEpoch() const;

    void OnScheduleAllocationStarted(const NPolicy::ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext);
    void OnScheduleAllocationFinished(const NPolicy::ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext);

    bool IsMaxScheduleAllocationCallsViolated() const;
    bool IsMaxConcurrentScheduleAllocationCallsPerNodeShardViolated(const NPolicy::ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext) const;
    bool IsMaxConcurrentScheduleAllocationExecDurationPerNodeShardViolated(const NPolicy::ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext) const;
    bool HasRecentScheduleAllocationFailure(NProfiling::TCpuInstant now) const;
    bool IsSaturatedInTentativeTree(
        NProfiling::TCpuInstant now,
        const std::string& treeId,
        TDuration saturationDeactivationTimeout) const;

    // TODO(eshcherbin): Rename?
    TControllerScheduleAllocationResultPtr ScheduleAllocation(
        const NPolicy::ISchedulingHeartbeatContextPtr& context,
        const TJobResources& availableResources,
        const TDiskResources& availableDiskResources,
        TDuration timeLimit,
        const std::string& treeId);
    void OnScheduleAllocationFailed(
        NProfiling::TCpuInstant now,
        const std::string& treeId,
        const TControllerScheduleAllocationResultPtr& scheduleAllocationResult);
    void AbortAllocation(
        TAllocationId allocationId,
        EAbortReason abortReason,
        TControllerEpoch allocationEpoch);

    TAllocationGroupResourcesMap GetInitialGroupedNeededResources() const;
    TJobResources GetAggregatedInitialMinNeededResources() const;

    //! Resource tree methods.
    EResourceTreeIncreaseResult TryIncreaseHierarchicalResourceUsagePrecommit(
        const TJobResources& delta,
        bool allowLimitsOvercommit,
        const std::optional<TJobResources>& additionalLocalResourceLimits = std::nullopt,
        TJobResources* availableResourceLimitsOutput = nullptr);
    void IncreaseHierarchicalResourceUsage(const TJobResources& delta);
    void DecreaseHierarchicalResourceUsagePrecommit(const TJobResources& precommittedResources);
    void CommitHierarchicalResourceUsage(const TJobResources& resourceUsage, const TJobResources& precommittedResources);
    void ReleaseResources(bool markAsNonAlive);

    EResourceTreeIncreasePreemptedResult TryIncreaseHierarchicalPreemptedResourceUsagePrecommit(const TJobResources& delta, std::string* violatedIdOutput);
    bool CommitHierarchicalPreemptedResourceUsage(const TJobResources& delta);

    //! Other methods.
    std::optional<TString> GetCustomProfilingTag() const;

    bool IsLimitingAncestorCheckEnabled() const;

    bool IsIdleCpuPolicyAllowed() const;

    bool IsGang() const;
    bool IsSingleAllocationVanillaOperation() const;
    bool IsDefaultGpuFullHost() const;

    TDuration GetEffectiveAllocationPreemptionTimeout() const;
    TDuration GetEffectiveAllocationGracefulPreemptionTimeout() const;

protected:
    //! Pre update methods.
    void CollectResourceTreeOperationElements(std::vector<TResourceTreeElementPtr>* elements) const override;

    //! Post update methods.
    void SetStarvationStatus(EStarvationStatus starvationStatus, TInstant now) override;
    void CheckForStarvation(TInstant now) override;

    void UpdateRecursiveAttributes() override;

    void OnFifoSchedulableElementCountLimitReached(TFairSharePostUpdateContext* context);
    void BuildSchedulableChildrenLists(TFairSharePostUpdateContext* context) override;

    void BuildElementMapping(TFairSharePostUpdateContext* context) override;

private:
    TOperationPoolTreeRuntimeParametersPtr RuntimeParameters_;

    const TOperationControllerPtr Controller_;
    const TStrategyOperationStatePtr StrategyOperationState_;

    std::optional<double> GetSpecifiedWeight() const override;

    //! Pre fair share update methods.
    void DisableNonAliveElements() override;

    std::optional<EUnschedulableReason> ComputeUnschedulableReason() const;

    void InitializeResourceUsageAndDemand();

    TJobResourcesConfigPtr GetSpecifiedResourceLimitsConfig() const override;

    friend class TPoolTreeCompositeElement;
};

DEFINE_REFCOUNTED_TYPE(TPoolTreeOperationElement)

////////////////////////////////////////////////////////////////////////////////

class TPoolTreeRootElementFixedState
{
public:
    // TODO(ignat): move it to TPoolTreeSnapshot.
    DEFINE_BYVAL_RO_PROPERTY(int, TreeSize);
};

class TPoolTreeRootElement
    : public NVectorHdrf::TRootElement
    , public TPoolTreeCompositeElement
    , public TPoolTreeRootElementFixedState
{
public:
    TPoolTreeRootElement(
        IStrategyHost* strategyHost,
        IPoolTreeElementHost* treeElementHost,
        TStrategyTreeConfigPtr treeConfig,
        const std::string& treeId,
        const NLogging::TLogger& logger);
    TPoolTreeRootElement(const TPoolTreeRootElement& other);

    //! Common interface.
    TString GetId() const override;

    TPoolTreeRootElementPtr Clone();

    TPoolTreeElementPtr Clone(TPoolTreeCompositeElement* clonedParent) override;

    ESchedulerElementType GetType() const override;

    // Used for diagnostics purposes.
    TJobResources GetSpecifiedStrongGuaranteeResources() const override;

    std::vector<EFifoSortParameter> GetFifoSortParameters() const override;

    //! Trunk node interface.
    int GetMaxRunningOperationCount() const override;
    int GetMaxOperationCount() const override;

    bool AreLightweightOperationsEnabled() const override;

    TPoolIntegralGuaranteesConfigPtr GetIntegralGuaranteesConfig() const override;

    bool IsDefaultConfigured() const override;
    bool AreImmediateOperationsForbidden() const override;
    bool AreGangOperationsAllowed() const override;
    bool IsEphemeralHub() const override;

    const TSchedulingTagFilter& GetSchedulingTagFilter() const override;

    //! Pre fair share update methods.
    // Computes various lightweight attributes in the tree. Must be called in control thread.
    void InitializeFairShareUpdate(TInstant now);

    //! Fair share update methods that implements NVectorHdrf::TRootElement interface.
    double GetSpecifiedBurstRatio() const override;
    double GetSpecifiedResourceFlowRatio() const override;

    bool ShouldComputePromisedGuaranteeFairShare() const override;

    bool IsPriorityStrongGuaranteeAdjustmentEnabled() const override;
    bool IsPriorityStrongGuaranteeAdjustmentDonorshipEnabled() const override;

    //! Post update methods.
    void PostUpdate(TFairSharePostUpdateContext* postUpdateContext);

    void UpdateRecursiveAttributes() override;

    std::optional<double> GetSpecifiedFairShareStarvationTolerance() const override;
    std::optional<TDuration> GetSpecifiedFairShareStarvationTimeout() const override;
    std::optional<bool> IsAggressiveStarvationEnabled() const override;

    TJobResourcesConfigPtr GetSpecifiedNonPreemptibleResourceUsageThresholdConfig() const override;

    std::optional<TDuration> GetSpecifiedWaitingForResourcesOnNodeTimeout() const override;

    void BuildPoolSatisfactionDigests(TFairSharePostUpdateContext* postUpdateContext);

    //! Other methods.
    THashSet<TString> GetAllowedProfilingTags() const override;

    void BuildResourceMetering(
        const std::optional<TMeteringKey>& lowestMeteredAncestorKey,
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

    std::optional<TDuration> GetMaybeHistoricUsageAggregatorPeriod() const override;

    bool CanAcceptFreeVolume() const override;
    bool ShouldDistributeFreeVolumeAmongChildren() const override;
};

DEFINE_REFCOUNTED_TYPE(TPoolTreeRootElement)

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Use in all relevant places.
template <class TAttributes>
const TAttributes& GetSchedulerElementAttributesFromVector(const std::vector<TAttributes>& vector, const TPoolTreeElement* element);
template <class TAttributes>
TAttributes& GetSchedulerElementAttributesFromVector(std::vector<TAttributes>& vector, const TPoolTreeElement* element);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy

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
#include "pool_tree_element-inl.h"
#undef FAIR_SHARE_TREE_ELEMENT_INL_H_

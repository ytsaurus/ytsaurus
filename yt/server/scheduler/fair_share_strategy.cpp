#include "stdafx.h"
#include "fair_share_strategy.h"
#include "scheduler_strategy.h"
#include "master_connector.h"
#include "job_resources.h"

#include <ytlib/ytree/yson_serializable.h>
#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/ytree/fluent.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/logging/log.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = SchedulerLogger;
static NProfiling::TProfiler& Profiler = SchedulerProfiler;
static Stroka DefaultPoolId("default");
static const double RatioPrecision = 1e-12;

////////////////////////////////////////////////////////////////////

struct ISchedulerElement;
typedef TIntrusivePtr<ISchedulerElement> ISchedulerElementPtr;

struct ISchedulableElement;
typedef TIntrusivePtr<ISchedulableElement> ISchedulableElementPtr;

class TOperationElement;
typedef TIntrusivePtr<TOperationElement> TOperationElementPtr;

class TCompositeSchedulerElement;
typedef TIntrusivePtr<TCompositeSchedulerElement> TCompositeSchedulerElementPtr;

class TPool;
typedef TIntrusivePtr<TPool> TPoolPtr;

class TRootElement;
typedef TIntrusivePtr<TRootElement> TRootElementPtr;

////////////////////////////////////////////////////////////////////

struct TSchedulableAttributes
{
    TSchedulableAttributes()
        : Rank(0)
        , DominantResource(EResourceType::Cpu)
        , Weight(0.0)
        , DemandRatio(0.0)
        , FairShareRatio(0.0)
        , AdjustedMinShareRatio(0.0)
    { }

    int Rank;
    EResourceType DominantResource;
    double Weight;
    double DemandRatio;
    double FairShareRatio;
    double AdjustedMinShareRatio;
};

////////////////////////////////////////////////////////////////////

struct ISchedulerElement
    : public virtual TRefCounted
{
    virtual void Update(double limitsRatio) = 0;
    virtual void ScheduleJobs(ISchedulingContext* context) = 0;

    virtual const TSchedulableAttributes& Attributes() const = 0;
    virtual TSchedulableAttributes& Attributes() = 0;
};

////////////////////////////////////////////////////////////////////

struct ISchedulableElement
    : public virtual ISchedulerElement
{
    virtual TInstant GetStartTime() const = 0;
    
    virtual double GetWeight() const = 0;
    virtual double GetMinShareRatio() const = 0;
    
    virtual NProto::TNodeResources GetDemand() const = 0;
    virtual NProto::TNodeResources GetUtilization() const = 0;
    virtual NProto::TNodeResources GetEffectiveLimits() const = 0;
};

////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EOperationStatus,
    (Normal)
    (StarvingForMinShare)
    (StarvingForFairShare)
);

class TOperationElement
    : public ISchedulableElement
{
public:
    explicit TOperationElement(
        TFairShareStrategyConfigPtr config,
        ISchedulerStrategyHost* host,
        TOperationPtr operation)
        : Config(config)
        , Host(host)
        , Operation_(operation)
        , EffectiveLimits_(ZeroNodeResources())
        , UtilizationRatio_(0.0)
        , Pool_(NULL)
    { }


    virtual void ScheduleJobs(ISchedulingContext* context) override
    {
        while (context->HasSpareResources()) {
            if (Operation_->GetState() != EOperationState::Running) {
                break;
            }
            auto job = Operation_->GetController()->ScheduleJob(context);
            if (!job) {
                break;
            }
        }
    }

    virtual void Update(double limitsRatio) override
    {
        UNUSED(limitsRatio);

        ComputeEffectiveLimits();
        ComputeUtilizationRatio();
    }

    virtual TInstant GetStartTime() const override
    {
        return Operation_->GetStartTime();
    }

    virtual double GetWeight() const override
    {
        return
            TInstant::Now() < GetStartTime() + Config->NewOperationWeightBoostPeriod
            ? Spec_->Weight * Config->NewOperationWeightBoostFactor
            : Spec_->Weight;
    }

    virtual double GetMinShareRatio() const override
    {
        return Spec_->MinShareRatio;
    }

    virtual NProto::TNodeResources GetDemand() const override
    {
        auto controller = Operation_->GetController();
        return controller->GetUsedResources() + controller->GetNeededResources();
    }

    virtual NProto::TNodeResources GetUtilization() const override
    {
        auto controller = Operation_->GetController();
        return controller->GetUsedResources();
    }

    DEFINE_BYREF_RW_PROPERTY(TSchedulableAttributes, Attributes);
    DEFINE_BYVAL_RO_PROPERTY(TOperationPtr, Operation);
    DEFINE_BYVAL_RO_PROPERTY(NProto::TNodeResources, EffectiveLimits);
    DEFINE_BYVAL_RO_PROPERTY(double, UtilizationRatio);
    DEFINE_BYVAL_RW_PROPERTY(TPooledOperationSpecPtr, Spec);
    DEFINE_BYVAL_RW_PROPERTY(TPool*, Pool);
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, StarvingForMinShareSince);
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, StarvingForFairShareSince);

private:
    TFairShareStrategyConfigPtr Config;
    ISchedulerStrategyHost* Host;


    void ComputeEffectiveLimits()
    {
        if (Operation_->GetState() != EOperationState::Running) {
            EffectiveLimits_ = Host->GetTotalResourceLimits();
            return;
        }

        EffectiveLimits_ = ZeroNodeResources();
        auto quantum = Operation_->GetController()->GetMinNeededResources();

        // Sort jobs by node.
        std::vector<TJobPtr> jobs(Operation_->Jobs().begin(), Operation_->Jobs().end());
        std::sort(
            jobs.begin(),
            jobs.end(),
            [] (const TJobPtr& lhs, const TJobPtr& rhs) {
                return lhs->GetNode() < rhs->GetNode();
            });

        // Sort nodes.
        auto nodes = Host->GetExecNodes();
        sort(nodes.begin(), nodes.end());

        // Merge jobs and nodes.
        auto jobIt = jobs.begin();
        auto nodeIt = nodes.begin();
        while (jobIt != jobs.end() || nodeIt != nodes.end()) {
            auto jobGroupBeginIt = jobIt;
            auto jobGroupEndIt = jobIt;
            while (jobGroupEndIt != jobs.end() && (*jobGroupEndIt)->GetNode() == (*jobGroupBeginIt)->GetNode()) {
                ++jobGroupEndIt;
            }

            while (nodeIt != nodes.end() && (jobGroupBeginIt == jobs.end() || *nodeIt < (*jobGroupBeginIt)->GetNode())) {
                // Node without jobs.
                const auto& node = *nodeIt;
                EffectiveLimits_ += NScheduler::ComputeEffectiveLimits(node->ResourceLimits(), quantum);
                ++nodeIt;
            }

            if (nodeIt != nodes.end()) {
                // Node with jobs.
                const auto& node = *nodeIt;
                YASSERT(node == (*jobGroupBeginIt)->GetNode());
                auto nodeLimits = node->ResourceLimits();
                for (auto jobGroupIt = jobGroupBeginIt; jobGroupIt != jobGroupEndIt; ++jobGroupIt) {
                    const auto& job = *jobGroupIt;
                    nodeLimits -= job->ResourceUtilization();
                    EffectiveLimits_ += job->ResourceUtilization();
                }
                EffectiveLimits_ += NScheduler::ComputeEffectiveLimits(nodeLimits, quantum);
                ++nodeIt;
            }

            jobIt = jobGroupEndIt;
        }
    }

    void ComputeUtilizationRatio()
    {
        i64 utilization = GetResource(GetUtilization(), Attributes_.DominantResource);
        i64 limits = GetResource(EffectiveLimits_, Attributes_.DominantResource);
        UtilizationRatio_ = limits == 0 ? 1.0 : (double) utilization / limits;
    }
};

////////////////////////////////////////////////////////////////////

class TCompositeSchedulerElement
    : public virtual ISchedulerElement
{
public:
    explicit TCompositeSchedulerElement(ISchedulerStrategyHost* host)
        : Host(host)
        , Mode(ESchedulingMode::Fifo)
        , LimitsRatio(0.0)
        , Limits(ZeroNodeResources())
    { }

    virtual void Update(double limitsRatio) override
    {
        LimitsRatio = limitsRatio;
        Limits = Host->GetTotalResourceLimits() * limitsRatio;

        ComputeFairShares();
    }

    virtual void ScheduleJobs(ISchedulingContext* context) override
    {
        auto sortedChildren = GetSortedChildren();
        FOREACH (auto child, sortedChildren) {
            if (!context->HasSpareResources()) {
                break;
            }
            child->ScheduleJobs(context);
        }
    }

    DEFINE_BYREF_RW_PROPERTY(TSchedulableAttributes, Attributes);


    void AddChild(ISchedulableElementPtr child)
    {
        YCHECK(Children.insert(child).second);
    }

    void RemoveChild(ISchedulableElementPtr child)
    {
        YCHECK(Children.erase(child) == 1);
        // Avoid scheduling removed children.
        ComputeFairShares();
    }

    std::vector<ISchedulableElementPtr> GetChildren() const
    {
        return std::vector<ISchedulableElementPtr>(Children.begin(), Children.end());
    }

    bool IsEmpty() const
    {
        return Children.empty();
    }

protected:
    ISchedulerStrategyHost* Host;

    ESchedulingMode Mode;
    
    yhash_set<ISchedulableElementPtr> Children;
    
    double LimitsRatio;
    NProto::TNodeResources Limits;


    void ComputeFairShares()
    {
        // Choose dominant resource types.
        // Precache weights.
        // Precache min share ratios and compute their sum.
        // Compute demand ratios and their sum.
        double demandRatioSum = 0.0;
        double minShareRatioSum = 0.0;
        auto totalLimits = Host->GetTotalResourceLimits();
        FOREACH (auto child, Children) {
            auto& attributes = child->Attributes();

            auto demand = child->GetDemand();
            attributes.DominantResource = GetDominantResource(demand, totalLimits);
            i64 dominantLimits = GetResource(totalLimits, attributes.DominantResource);
            i64 dominantDemand = GetResource(demand, attributes.DominantResource);
            attributes.DemandRatio = dominantLimits == 0 ? LimitsRatio : (double) dominantDemand / dominantLimits;
            demandRatioSum += attributes.DemandRatio;

            attributes.Weight = child->GetWeight();

            if (dominantDemand != 0) {
                attributes.AdjustedMinShareRatio = child->GetMinShareRatio() * LimitsRatio;
                minShareRatioSum += attributes.AdjustedMinShareRatio;
            } else {
                attributes.AdjustedMinShareRatio = 0.0;
            }
        }

        // Scale down weights if needed.
        if (minShareRatioSum > LimitsRatio) {
            FOREACH (auto child, Children) {
                auto& attributes = child->Attributes();
                attributes.AdjustedMinShareRatio *= (LimitsRatio / minShareRatioSum);
            }
        }

        // Check for FIFO mode.
        // Check if we have more resources than totally demanded by children.
        if (Mode == ESchedulingMode::Fifo) {
            // Set fair shares equal to limits ratio. This is done just for convenience.
            SetFifoFairShares();
        } else if (demandRatioSum <= LimitsRatio) {
            // Easy case -- just give everyone what he needs.
            SetDemandedFairShares();
        } else {
            // Hard case -- compute fair shares using fit factor.
            ComputeFairSharesByFitting();
        }

        // Propagate updates to children.
        FOREACH (auto child, Children) {
            auto& attributes = child->Attributes();           
            child->Update(attributes.FairShareRatio);
        }
    }

    void SetFifoFairShares()
    {
        FOREACH (auto child, Children) {
            auto& attributes = child->Attributes();
            attributes.FairShareRatio = LimitsRatio;
        }
    }

    void SetDemandedFairShares()
    {
        FOREACH (auto child, Children) {
            auto& attributes = child->Attributes();
            attributes.FairShareRatio = attributes.DemandRatio;
        }
    }

    void ComputeFairSharesByFitting()
    {
        auto computeFairShareRatio = [&] (double fitFactor, const TSchedulableAttributes& attributes) -> double {
            double result = attributes.Weight * fitFactor;
            // Never give less than promised by min share.
            result = std::max(result, attributes.AdjustedMinShareRatio);
            // Never give more than demanded.
            result = std::min(result, attributes.DemandRatio);
            return result;
        };

        // Run binary search to compute fit factor.
        double fitFactorLo = 0.0;
        double fitFactorHi = 1.0;
        while (fitFactorHi - fitFactorLo > RatioPrecision) {
            double fitFactor = (fitFactorLo + fitFactorHi) / 2.0;
            double fairShareRatioSum = 0.0;
            FOREACH (auto child, Children) {
                fairShareRatioSum += computeFairShareRatio(fitFactor, child->Attributes());
            }
            if (fairShareRatioSum < LimitsRatio) {
                fitFactorLo = fitFactor;
            } else {
                fitFactorHi = fitFactor;
            }
        }

        // Compute fair share ratios.
        double fitFactor = (fitFactorLo + fitFactorHi) / 2.0;
        FOREACH (auto child, Children) {
            auto& attributes = child->Attributes();
            attributes.FairShareRatio = computeFairShareRatio(fitFactor, attributes);
        }
    }


    const std::vector<ISchedulableElementPtr> GetSortedChildren()
    {
        PROFILE_TIMING ("/fair_share_sort_time") {
            std::vector<ISchedulableElementPtr> sortedChildren;
            FOREACH (auto child, Children) {
                sortedChildren.push_back(child);
            }

            switch (Mode) {
                case ESchedulingMode::Fifo:
                    SortChildrenFifo(&sortedChildren);
                    break;
                case ESchedulingMode::FairShare:
                    SortChildrenFairShare(&sortedChildren);
                    break;
                default:
                    YUNREACHABLE();
            }

            // Update ranks.
            for (int rank = 0; rank < static_cast<int>(sortedChildren.size()); ++rank) {
                sortedChildren[rank]->Attributes().Rank = rank;
            }

            return sortedChildren;
        }
    }

    void SortChildrenFifo(std::vector<ISchedulableElementPtr>* sortedChildren)
    {
        // Sort by weight (desc), then by start time (asc).
        std::sort(
            sortedChildren->begin(),
            sortedChildren->end(),
            [] (const ISchedulableElementPtr& lhs, const ISchedulableElementPtr& rhs) -> bool {
                if (lhs->GetWeight() > rhs->GetWeight()) {
                    return true;
                }
                if (lhs->GetWeight() < rhs->GetWeight()) {
                    return false;
                }
                return lhs->GetStartTime() < rhs->GetStartTime();
            });
    }

    void SortChildrenFairShare(std::vector<ISchedulableElementPtr>* sortedChildren)
    {
        std::sort(
            sortedChildren->begin(),
            sortedChildren->end(),
            [&] (const ISchedulableElementPtr& lhs, const ISchedulableElementPtr& rhs) -> bool {
                bool lhsNeedy = IsNeedy(lhs);
                bool rhsNeedy = IsNeedy(rhs);

                if (lhsNeedy && !rhsNeedy) {
                    return true;
                }

                if (!lhsNeedy && rhsNeedy) {
                    return false;
                }

                if (lhsNeedy && rhsNeedy) {
                    double lhsUseToTotalRatio = GetUtilizationToLimitsRatio(lhs);
                    double rhsUseToTotalRatio = GetUtilizationToLimitsRatio(rhs);
                    return lhsUseToTotalRatio < rhsUseToTotalRatio;
                }

                {
                    double lhsUseToWeightRatio = GetUtilizationToLimitsRatio(lhs);
                    double rhsUseToWeightRatio = GetUtilizationToLimitsRatio(rhs);
                    return lhsUseToWeightRatio < rhsUseToWeightRatio;
                }
            });
    }


    bool IsNeedy(ISchedulableElementPtr element)
    {
        const auto& attributes = element->Attributes();
        i64 demand = GetResource(element->GetDemand(), attributes.DominantResource);
        i64 utilization = GetResource(element->GetUtilization(), attributes.DominantResource);
        i64 limits = GetResource(Limits, attributes.DominantResource);
        return utilization < demand && utilization < limits * attributes.AdjustedMinShareRatio;
    }

    double GetUtilizationToLimitsRatio(ISchedulableElementPtr element)
    {
        const auto& attributes = element->Attributes();
        i64 utilization = GetResource(element->GetUtilization(), attributes.DominantResource);
        i64 total = GetResource(Limits, attributes.DominantResource);
        return total == 0 ? 1.0 : (double) utilization / total;
    }

    static double GetUtilizationToWeightRatio(ISchedulableElementPtr element)
    {
        const auto& attributes = element->Attributes();
        i64 utilization = GetResource(element->GetUtilization(), attributes.DominantResource);
        i64 weight = attributes.Weight;
        return weight == 0 ? 1.0 : (double) utilization / weight;
    }


    void SetMode(ESchedulingMode mode)
    {
        if (Mode != mode) {
            Mode = mode;
            ComputeFairShares();
        }
    }

};

////////////////////////////////////////////////////////////////////

class TPool
    : public TCompositeSchedulerElement
    , public virtual ISchedulableElement
{
public:
    TPool(
        ISchedulerStrategyHost* host,
        const Stroka& id)
        : TCompositeSchedulerElement(host)
        , Id(id)
    {
        SetDefaultConfig();
    }


    const Stroka& GetId() const
    {
        return Id;
    }

    bool IsDefaultConfigured() const
    {
        return DefaultConfigured;
    }


    TPoolConfigPtr GetConfig()
    {
        return Config;
    }

    void SetConfig(TPoolConfigPtr newConfig)
    {
        Config = newConfig;
        SetMode(Config->Mode);
        DefaultConfigured = false;
    }

    void SetDefaultConfig()
    {
        SetConfig(New<TPoolConfig>());
        DefaultConfigured = true;
    }


    virtual TInstant GetStartTime() const override
    {
        // Makes no sense for pools since the root is in fair-share mode.
        return TInstant();
    }

    virtual double GetWeight() const override
    {
        return Config->Weight;
    }

    virtual double GetMinShareRatio() const override
    {
        return Config->MinShareRatio;
    }

    virtual NProto::TNodeResources GetDemand() const override
    {
        auto result = ZeroNodeResources();
        FOREACH (auto child, Children) {
            result += child->GetDemand();
        }
        return result;
    }

    virtual NProto::TNodeResources GetUtilization() const override
    {
        auto result = ZeroNodeResources();
        FOREACH (auto child, Children) {
            result += child->GetUtilization();
        }
        return result;
    }

    virtual NProto::TNodeResources GetEffectiveLimits() const override
    {
        auto result = ZeroNodeResources();
        FOREACH (auto child, Children) {
            result += child->GetEffectiveLimits();
        }
        return result;
    }

private:
    Stroka Id;

    TPoolConfigPtr Config;
    bool DefaultConfigured;

};

////////////////////////////////////////////////////////////////////

class TRootElement
    : public TCompositeSchedulerElement
{
public:
    explicit TRootElement(ISchedulerStrategyHost* host)
        : TCompositeSchedulerElement(host)
    {
        SetMode(ESchedulingMode::FairShare);
    }
};

////////////////////////////////////////////////////////////////////

class TFairShareStrategy
    : public ISchedulerStrategy
{
public:
    explicit TFairShareStrategy(
        TFairShareStrategyConfigPtr config,
        ISchedulerStrategyHost* host)
        : Config(config)
        , Host(host)
    {
        Host->SubscribeOperationStarted(BIND(&TFairShareStrategy::OnOperationStarted, this));
        Host->SubscribeOperationFinished(BIND(&TFairShareStrategy::OnOperationFinished, this));

        Host->SubscribeJobStarted(BIND(&TFairShareStrategy::OnJobStarted, this));
        Host->SubscribeJobFinished(BIND(&TFairShareStrategy::OnJobFinished, this));

        auto* masterConnector = Host->GetMasterConnector();
        masterConnector->SubscribeWatcherRequest(BIND(&TFairShareStrategy::OnPoolsRequest, this));
        masterConnector->SubscribeWatcherResponse(BIND(&TFairShareStrategy::OnPoolsResponse, this));

        RootElement = New<TRootElement>(Host);
    }

    virtual void ScheduleJobs(ISchedulingContext* context) override
    {
        auto now = TInstant::Now();

        if (!LastUpdateTime || now > LastUpdateTime.Get() + Config->FairShareUpdatePeriod) {
            PROFILE_TIMING ("/fair_share_update_time") {
                RootElement->Update(1.0);
            }
            LastUpdateTime = now;
        }
        
        if (!LastPreemptionCheckTime || now > LastPreemptionCheckTime.Get() + Config->PreemptionCheckPeriod) {
            PROFILE_TIMING ("/preemption_check_time") {
                PreemptJobs();
            }
            LastPreemptionCheckTime = now;
        }

        return RootElement->ScheduleJobs(context);
    }

    virtual void BuildOperationProgressYson(TOperationPtr operation, IYsonConsumer* consumer) override
    {
        auto element = GetOperationElement(operation);
        auto pool = element->GetPool();
        BuildYsonMapFluently(consumer)
            .Item("pool").Scalar(pool->GetId())
            .Item("start_time").Scalar(element->GetStartTime())
            .Item("effective_resource_limits").Scalar(element->GetEffectiveLimits())
            .Item("utilization_ratio").Scalar(element->GetUtilizationRatio())
            .Item("scheduling_status").Scalar(GetOperationStatus(element))
            .Do(BIND(&TFairShareStrategy::BuildElementYson, pool, element));
    }

    virtual void BuildOrchidYson(NYTree::IYsonConsumer* consumer) override
    {
        BuildYsonMapFluently(consumer)
            .Item("pools").DoMapFor(Pools, [&] (TFluentMap fluent, const TPoolMap::value_type& pair) {
                const auto& id = pair.first;
                auto pool = pair.second;
                auto config = pool->GetConfig();
                fluent
                    .Item(id).BeginMap()
                        .Item("mode").Scalar(config->Mode)
                        .Do(BIND(&TFairShareStrategy::BuildElementYson, RootElement, pool))
                    .EndMap();
            });
    }

private:
    TFairShareStrategyConfigPtr Config;
    ISchedulerStrategyHost* Host;

    typedef yhash_map<Stroka, TPoolPtr> TPoolMap;
    TPoolMap Pools;

    yhash_map<TOperationPtr, TOperationElementPtr> OperationToElement;

    typedef std::list<TJobPtr> TJobList;
    TJobList JobList;
    yhash_map<TJobPtr, TJobList::iterator> JobToIterator;

    TRootElementPtr RootElement;
    TNullable<TInstant> LastUpdateTime;
    TNullable<TInstant> LastPreemptionCheckTime;


    void OnOperationStarted(TOperationPtr operation)
    {
        auto operationElement = New<TOperationElement>(Config, Host, operation);
        YCHECK(OperationToElement.insert(std::make_pair(operation, operationElement)).second);

        TPooledOperationSpecPtr spec;
        try {
            spec = ConvertTo<TPooledOperationSpecPtr>(operation->GetSpec());
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error parsing spec of pooled operation %s, defaults will be used",
                ~ToString(operation->GetOperationId()));
            spec = New<TPooledOperationSpec>();
        }

        auto poolId = spec->Pool ? spec->Pool.Get() : DefaultPoolId;
        auto pool = FindPool(poolId);
        if (!pool) {
            pool = New<TPool>(Host, poolId);
            RegisterPool(pool);
        }

        operationElement->SetSpec(spec);

        operationElement->SetPool(~pool);
        pool->AddChild(operationElement);

        LOG_INFO("Operation added to pool (OperationId: %s, PoolId: %s)",
            ~ToString(operation->GetOperationId()),
            ~pool->GetId());
    }

    void OnOperationFinished(TOperationPtr operation)
    {
        auto element = GetOperationElement(operation);
        auto pool = element->GetPool();

        YCHECK(OperationToElement.erase(operation) == 1);
        pool->RemoveChild(element);

        LOG_INFO("Operation removed from pool (OperationId: %s, PoolId: %s)",
            ~ToString(operation->GetOperationId()),
            ~pool->GetId());

        if (pool->IsEmpty() && pool->IsDefaultConfigured()) {
            UnregisterPool(pool);
        }
    }


    void OnJobStarted(TJobPtr job)
    {
        auto it = JobList.insert(JobList.begin(), job);
        YCHECK(JobToIterator.insert(std::make_pair(job, it)).second);
    }

    void OnJobFinished(TJobPtr job)
    {
        auto it = JobToIterator.find(job);
        YASSERT(it != JobToIterator.end());
        JobList.erase(it->second);
        JobToIterator.erase(it);
    }


    void RegisterPool(TPoolPtr pool)
    {
        YCHECK(Pools.insert(std::make_pair(pool->GetId(), pool)).second);
        RootElement->AddChild(pool);

        LOG_INFO("Pool registered: %s", ~pool->GetId());
    }

    void UnregisterPool(TPoolPtr pool)
    {
        YCHECK(pool->IsEmpty());

        YCHECK(Pools.erase(pool->GetId()) == 1);
        RootElement->RemoveChild(pool);

        LOG_INFO("Pool unregistered: %s", ~pool->GetId());
    }

    TPoolPtr FindPool(const Stroka& id)
    {
        auto it = Pools.find(id);
        return it == Pools.end() ? NULL : it->second;
    }

    TPoolPtr GetPool(const Stroka& id)
    {
        auto pool = FindPool(id);
        YCHECK(pool);
        return pool;
    }


    TOperationElementPtr FindOperationElement(TOperationPtr operation)
    {
        auto it = OperationToElement.find(operation);
        return it == OperationToElement.end() ? NULL : it->second;
    }

    TOperationElementPtr GetOperationElement(TOperationPtr operation)
    {
        auto element = FindOperationElement(operation);
        YCHECK(element);
        return element;
    }


    void OnPoolsRequest(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        LOG_INFO("Updating pools");

        auto req = TYPathProxy::Get("//sys/scheduler/pools");
        static auto poolConfigTemplate = New<TPoolConfig>();
        auto poolConfigKeys = poolConfigTemplate->GetRegisteredKeys();
        TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly, poolConfigKeys);
        *req->mutable_attribute_filter() = ToProto(attributeFilter);
        batchReq->AddRequest(req, "get_pools");
    }

    void OnPoolsResponse(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_pools");
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

        // Build the set of potential orphans.
        yhash_set<Stroka> orphanPoolIds;
        FOREACH (const auto& pair, Pools) {
            YCHECK(orphanPoolIds.insert(pair.first).second);
        }

        auto newPoolsNode = ConvertToNode(TYsonString(rsp->value()));
        auto newPoolsMapNode = newPoolsNode->AsMap();
        FOREACH (const auto& pair, newPoolsMapNode->GetChildren()) {
            const auto& id = pair.first;
            const auto& poolNode = pair.second;

            // Parse config.
            auto configNode = ConvertToNode(poolNode->Attributes());
            TPoolConfigPtr config;
            try {
                config = ConvertTo<TPoolConfigPtr>(configNode);
            } catch (const std::exception& ex) {
                LOG_ERROR(ex, "Error parsing configuration of pool %s, defaults will be used",
                    ~id.Quote());
                config = New<TPoolConfig>();
            }

            auto existingPool = FindPool(id);
            if (existingPool) {
                // Reconfigure existing pool.
                existingPool->SetConfig(config);
                YCHECK(orphanPoolIds.erase(id) == 1);
            } else {
                // Create new pool.
                auto newPool = New<TPool>(Host, id);
                newPool->SetConfig(config);
                RegisterPool(newPool);
            }
        }

        // Unregister orphan pools.
        FOREACH (const auto& id, orphanPoolIds) {
            auto pool = GetPool(id);
            if (pool->IsEmpty()) {
                UnregisterPool(pool);
            } else {
                pool->SetDefaultConfig();
            }
        }

        LOG_INFO("Pools updated");
    }


    EOperationStatus GetOperationStatus(TOperationElementPtr element) const
    {
        auto pool = element->GetPool();
        const auto& attributes = element->Attributes();

        // For operations in FIFO pools only the top one may starve.
        if (pool->GetConfig()->Mode == ESchedulingMode::Fifo && attributes.Rank != 0) {
            return EOperationStatus::Normal;
        }

        double utilizationRatio = element->GetUtilizationRatio();

        if (utilizationRatio < attributes.AdjustedMinShareRatio * Config->MinShareStarvationFactor) {
            return EOperationStatus::StarvingForMinShare;
        }

        if (utilizationRatio < attributes.FairShareRatio * Config->FairShareStarvationFactor) {
            return EOperationStatus::StarvingForFairShare;
        }

        return EOperationStatus::Normal;
    }

    EOperationStatus GetOperationStatus(TOperationPtr operation)
    {
        return GetOperationStatus(GetOperationElement(operation));
    }


    bool CanPreemptJob(TJobPtr job)
    {
        if (job->GetState() != EJobState::Running)
            return false;

        auto operation = job->GetOperation();
        if (operation->GetState() != EOperationState::Running)
            return false;

        auto operationElement = GetOperationElement(operation);

        auto schedulable =
            operationElement->GetPool()->GetConfig()->Mode == ESchedulingMode::Fifo
            ? ISchedulableElementPtr(operationElement->GetPool())
            : ISchedulableElementPtr(operationElement);

        const auto& attributes = schedulable->Attributes();
        i64 utilization = GetResource(schedulable->GetUtilization(), attributes.DominantResource);
        i64 limits = GetResource(schedulable->GetEffectiveLimits(), attributes.DominantResource);
        return limits > 0 && (double) utilization / limits > attributes.FairShareRatio;
    }

    NProto::TNodeResources ComputeResourcesToPreempt(TOperationElementPtr element, double desiredRatio)
    {
        auto desiredResources = ZeroNodeResources();

        auto demand = element->GetDemand();
        auto limits = element->GetEffectiveLimits();
        auto utilization = element->GetUtilization();

        // Don't trust attributes, they might be out of sync.
        double maxDemandRatio = 0.0;

        for (int typeIndex = 0; typeIndex < EResourceType::GetDomainSize(); ++typeIndex) {
            auto type = EResourceType(typeIndex);

            i64 demandComponent = GetResource(demand, type);
            i64 limitsComponent = GetResource(limits, type);
            if (limitsComponent == 0)
                continue;

            double demandComponentRatio = (double) demandComponent / limitsComponent;
            maxDemandRatio = std::max(maxDemandRatio, demandComponentRatio);
            i64 desiredComponent = static_cast<i64>(demandComponentRatio * desiredRatio * limitsComponent);
            SetResource(desiredResources, type, desiredComponent);
        }

        desiredResources *= 1.0 / maxDemandRatio;
        auto resourcesToPreempt = Max(ZeroNodeResources(), desiredResources - utilization);

        LOG_DEBUG("Scheduling resource preemption (OperationId: %s, Demand: {%s}, Utilization: {%s}, Limits: {%s}, DesiredRatio: %ld, DesiredResources: {%s}, Preempt: {%s})",
            ~element->GetOperation()->GetOperationId().ToString(),
            ~FormatResources(demand),
            ~FormatResources(utilization),
            ~FormatResources(limits),
            desiredRatio,
            ~FormatResources(desiredResources),
            ~FormatResources(resourcesToPreempt));

        return resourcesToPreempt;
    }

    void PreemptJobs()
    {
        auto resourcesToPreempt = ZeroNodeResources();

        auto now = TInstant::Now(); 
        FOREACH (const auto& pair, OperationToElement) {
            auto operation = pair.first;
            auto element = pair.second;
            auto status = GetOperationStatus(element);
            const auto& attributes = element->Attributes();

            switch (status) {
                case EOperationStatus::StarvingForMinShare:
                    if (!element->GetStarvingForMinShareSince()) {
                        element->SetStarvingForMinShareSince(now);
                    } else if (element->GetStarvingForMinShareSince().Get() < now - Config->MinSharePreemptionTimeout) {
                        LOG_INFO("Min share starvation timeout (OperationId: %s, Since: %s)",
                            ~ToString(operation->GetOperationId()),
                            ~ToString(element->GetStarvingForMinShareSince().Get()));
                        resourcesToPreempt += ComputeResourcesToPreempt(element, attributes.AdjustedMinShareRatio);
                    }
                    break;

                case EOperationStatus::StarvingForFairShare:
                    if (!element->GetStarvingForFairShareSince()) {
                        element->SetStarvingForFairShareSince(now);
                    } else if (element->GetStarvingForFairShareSince().Get() < now - Config->FairSharePreemptionTimeout) {
                        LOG_INFO("Fair share starvation timeout (OperationId: %s, Since: %s)",
                            ~ToString(operation->GetOperationId()),
                            ~ToString(element->GetStarvingForFairShareSince().Get()));
                        resourcesToPreempt += ComputeResourcesToPreempt(element, attributes.FairShareRatio);
                    }
                    element->SetStarvingForMinShareSince(Null);
                    break;

                case EOperationStatus::Normal:
                    element->SetStarvingForMinShareSince(Null);
                    element->SetStarvingForFairShareSince(Null);
                    break;

                default:
                    YUNREACHABLE();
            }
        }

        if (resourcesToPreempt != ZeroNodeResources())
            return;

        LOG_INFO("Started preempting jobs (ResourcesToPreempt: {%s})",
            ~FormatResources(resourcesToPreempt));

        auto resourcesPreempted = ZeroNodeResources();
        FOREACH (auto job, JobList) {
            if (Dominates(resourcesPreempted, resourcesToPreempt)) {
                break;
            }
            if (CanPreemptJob(job)) {
                resourcesPreempted += job->ResourceUtilization();
                Host->PreeemptJob(job);
            }
        }

        LOG_INFO("Finished preempting jobs (ResourcesPreempted: {%s})",
            ~FormatResources(resourcesPreempted));
    }


    static void BuildElementYson(
        TCompositeSchedulerElementPtr composite,
        ISchedulableElementPtr element,
        IYsonConsumer* consumer)
    {
        const auto& attributes = element->Attributes();
        BuildYsonMapFluently(consumer)
            .Item("scheduling_rank").Scalar(attributes.Rank)
            .Item("resource_demand").Scalar(element->GetDemand())
            .Item("resource_utilization").Scalar(element->GetUtilization())
            .Item("dominant_resource").Scalar(attributes.DominantResource)
            .Item("weight").Scalar(element->GetWeight())
            .Item("min_share_ratio").Scalar(element->GetMinShareRatio())
            .Item("adjusted_min_share_ratio").Scalar(attributes.AdjustedMinShareRatio)
            .Item("demand_ratio").Scalar(attributes.DemandRatio)
            .Item("fair_share_ratio").Scalar(attributes.FairShareRatio);
    }

};

TAutoPtr<ISchedulerStrategy> CreateFairShareStrategy(
    TFairShareStrategyConfigPtr config,
    ISchedulerStrategyHost* host)
{
    return new TFairShareStrategy(config, host);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT


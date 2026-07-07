#include "fair_share_update_mock.h"

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

TJobResources CreateCpuResourceLimits(double cpu)
{
    auto result = TJobResources::Infinite();
    result.SetCpu(cpu);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TElementMock::TElementMock(std::string id)
    : Id_(std::move(id))
    , Logger(FairShareLogger.WithTag("Id: %v", Id_))
{ }

const TJobResources& TElementMock::GetResourceDemand() const
{
    return ResourceDemand_;
}

const TJobResources& TElementMock::GetResourceUsageAtUpdate() const
{
    return ResourceUsage_;
}

const TJobResources& TElementMock::GetResourceLimits() const
{
    return ResourceLimits_;
}

const TJobResourcesConfig* TElementMock::GetStrongGuaranteeResourcesConfig() const
{
    return StrongGuaranteeResourcesConfig_.Get();
}

double TElementMock::GetWeight() const
{
    return Weight_;
}

TSchedulableAttributes& TElementMock::Attributes()
{
    return Attributes_;
}

const TSchedulableAttributes& TElementMock::Attributes() const
{
    return Attributes_;
}

TCompositeElement* TElementMock::GetParentElement() const
{
    return Parent_;
}

std::string TElementMock::GetId() const
{
    return Id_;
}

const NLogging::TLogger& TElementMock::GetLogger() const
{
    return Logger;
}

bool TElementMock::AreDetailedLogsEnabled() const
{
    return true;
}

void TElementMock::AttachParent(TCompositeElementMock* parent)
{
    parent->AddChild(this);
    Parent_ = parent;
}

void TElementMock::PreUpdate(const TJobResources& totalResourceLimits)
{
    TotalResourceLimits_ = totalResourceLimits;
}

void TElementMock::SetWeight(double weight)
{
    Weight_ = weight;
}

void TElementMock::SetStrongGuaranteeResourcesConfig(const TTestJobResourcesConfigPtr& strongGuaranteeResourcesConfig)
{
    StrongGuaranteeResourcesConfig_ = strongGuaranteeResourcesConfig;
}

void TElementMock::SetResourceLimits(const TJobResources& resourceLimits)
{
    ResourceLimits_ = resourceLimits;
}

////////////////////////////////////////////////////////////////////////////////

TCompositeElementMock::TCompositeElementMock(std::string id)
    : TElementMock(std::move(id))
{ }

TElement* TCompositeElementMock::GetChild(int index)
{
    return Children_[index].Get();
}

const TElement* TCompositeElementMock::GetChild(int index) const
{
    return Children_[index].Get();
}

int TCompositeElementMock::GetChildCount() const
{
    return Children_.size();
}

ESchedulingMode TCompositeElementMock::GetMode() const
{
    return Mode_;
}

bool TCompositeElementMock::HasHigherPriorityInFifoMode(const TElement* lhs, const TElement* rhs) const
{
    if (lhs->GetWeight() != rhs->GetWeight()) {
        return lhs->GetWeight() > rhs->GetWeight();
    }
    return false;
}

double TCompositeElementMock::GetSpecifiedBurstRatio() const
{
    if (IntegralGuaranteesConfig_->GuaranteeType == EIntegralGuaranteeType::None) {
        return 0;
    }
    return GetMaxResourceRatio(ToJobResources(*IntegralGuaranteesConfig_->BurstGuaranteeResources, {}), TotalResourceLimits_);
}

double TCompositeElementMock::GetSpecifiedResourceFlowRatio() const
{
    if (IntegralGuaranteesConfig_->GuaranteeType == EIntegralGuaranteeType::None) {
        return 0;
    }
    return GetMaxResourceRatio(ToJobResources(*IntegralGuaranteesConfig_->ResourceFlow, {}), TotalResourceLimits_);
}

bool TCompositeElementMock::IsStepFunctionForGangOperationsEnabled() const
{
    return true;
}

bool TCompositeElementMock::CanAcceptFreeVolume() const
{
    return IntegralGuaranteesConfig_->CanAcceptFreeVolume;
}

bool TCompositeElementMock::ShouldDistributeFreeVolumeAmongChildren() const
{
    return IntegralGuaranteesConfig_->ShouldDistributeFreeVolumeAmongChildren;
}

bool TCompositeElementMock::ShouldComputePromisedGuaranteeFairShare() const
{
    return PromisedGuaranteeFairShareComputationEnabled_;
}

bool TCompositeElementMock::IsPriorityStrongGuaranteeAdjustmentEnabled() const
{
    return false;
}

bool TCompositeElementMock::IsPriorityStrongGuaranteeAdjustmentDonorshipEnabled() const
{
    return false;
}

void TCompositeElementMock::AddChild(TElementMock* child)
{
    Children_.push_back(child);
}

void TCompositeElementMock::PreUpdate(const TJobResources& totalResourceLimits)
{
    ResourceUsage_ = {};
    ResourceDemand_ = {};

    for (const auto& child : Children_) {
        child->PreUpdate(totalResourceLimits);

        ResourceUsage_ += child->GetResourceUsageAtUpdate();
        ResourceDemand_ += child->GetResourceDemand();
    }

    TElementMock::PreUpdate(totalResourceLimits);
}

void TCompositeElementMock::SetMode(ESchedulingMode mode)
{
    Mode_ = mode;
}

void TCompositeElementMock::SetPromisedGuaranteeFairShareComputationEnabled(bool enabled)
{
    PromisedGuaranteeFairShareComputationEnabled_ = enabled;
}

////////////////////////////////////////////////////////////////////////////////

TPoolElementMock::TPoolElementMock(std::string id)
    : TCompositeElementMock(std::move(id))
{ }

TResourceVector TPoolElementMock::GetIntegralShareLimitForRelaxedPool() const
{
    YT_VERIFY(GetIntegralGuaranteeType() == EIntegralGuaranteeType::Relaxed);
    auto multiplier = IntegralGuaranteesConfig_->RelaxedShareMultiplierLimit;
    return TResourceVector::FromDouble(Attributes_.ResourceFlowRatio) * multiplier;
}

const TIntegralResourcesState& TPoolElementMock::IntegralResourcesState() const
{
    return IntegralResourcesState_;
}

TIntegralResourcesState& TPoolElementMock::IntegralResourcesState()
{
    return IntegralResourcesState_;
}

EIntegralGuaranteeType TPoolElementMock::GetIntegralGuaranteeType() const
{
    return IntegralGuaranteesConfig_->GuaranteeType;
}

void TPoolElementMock::InitAccumulatedResourceVolume(TResourceVolume resourceVolume)
{
    YT_VERIFY(IntegralResourcesState_.AccumulatedVolume == TResourceVolume());
    IntegralResourcesState_.AccumulatedVolume = resourceVolume;
}

////////////////////////////////////////////////////////////////////////////////

TOperationElementMock::TOperationElementMock(std::string id)
    : TElementMock(std::move(id))
{ }

void TOperationElementMock::SetResourceDemand(const TJobResources& resourceDemand)
{
    ResourceDemand_ = resourceDemand;
}

void TOperationElementMock::SetResourceUsage(const TJobResources& resourceUsage)
{
    YT_VERIFY(Dominates(ResourceDemand_, resourceUsage));

    ResourceUsage_ = resourceUsage;
}

void TOperationElementMock::IncreaseResourceUsageAndDemand(const TJobResources& delta)
{
    ResourceDemand_ += delta;
    ResourceUsage_ += delta;
}

TResourceVector TOperationElementMock::GetBestAllocationShare() const
{
    return TResourceVector::Ones();
}

bool TOperationElementMock::IsGangLike() const
{
    return IsGang_;
}

void TOperationElementMock::SetGangFlag(bool value)
{
    IsGang_ = value;
}

////////////////////////////////////////////////////////////////////////////////

TRootElementMock::TRootElementMock()
    : TCompositeElementMock("<Root>")
{ }

////////////////////////////////////////////////////////////////////////////////

void ResetFairShareFunctionsRecursively(TCompositeElementMock* compositeElement)
{
    compositeElement->ResetFairShareFunctions();
    for (int childIndex = 0; childIndex < compositeElement->GetChildCount(); ++childIndex) {
        auto* child = compositeElement->GetChild(childIndex);
        if (auto* childPool = dynamic_cast<TCompositeElementMock*>(child)) {
            ResetFairShareFunctionsRecursively(childPool);
        } else {
            child->ResetFairShareFunctions();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf

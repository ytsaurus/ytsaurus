#include "base_element.h"

namespace NYT::NVectorHdrf {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

const TLogger& TBaseElement::GetLogger() const
{
    static const TLogger Logger;
    return Logger;
}

bool TBaseElement::AreDetailedLogsEnabled() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

double TBaseCompositeElement::GetSpecifiedBurstRatio() const
{
    return 0.0;
}

double TBaseCompositeElement::GetSpecifiedResourceFlowRatio() const
{
    return 0.0;
}

bool TBaseCompositeElement::IsFairShareTruncationInFifoPoolEnabled() const
{
    return false;
}

bool TBaseCompositeElement::CanAcceptFreeVolume() const
{
    return false;
}

bool TBaseCompositeElement::ShouldDistributeFreeVolumeAmongChildren() const
{
    return false;
}

bool TBaseCompositeElement::ShouldComputePromisedGuaranteeFairShare() const
{
    return false;
}

bool TBaseCompositeElement::IsPriorityStrongGuaranteeAdjustmentEnabled() const
{
    return false;
}

bool TBaseCompositeElement::IsPriorityStrongGuaranteeAdjustmentDonorshipEnabled() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TResourceVector TBasePool::GetIntegralShareLimitForRelaxedPool() const
{
    return TResourceVector::Zero();
}

const TIntegralResourcesState& TBasePool::IntegralResourcesState() const
{
    return IntegralResourcesState_;
}

TIntegralResourcesState& TBasePool::IntegralResourcesState()
{
    return IntegralResourcesState_;
}

EIntegralGuaranteeType TBasePool::GetIntegralGuaranteeType() const
{
    return EIntegralGuaranteeType::None;
}

////////////////////////////////////////////////////////////////////////////////

bool TBaseOperationElement::IsFairShareTruncationInFifoPoolAllowed() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NVectorHdrf

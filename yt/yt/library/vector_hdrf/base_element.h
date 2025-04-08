#pragma once

#include "public.h"
#include "fair_share_update.h"

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

// These classes provide stub implementations for methods which are used by extra features
// that are quite niche and might not be required for projects outside YT.
class TBaseElement
    : public virtual TElement
{
public:
    const NLogging::TLogger& GetLogger() const override;

    bool AreDetailedLogsEnabled() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TBaseCompositeElement
    : public virtual TBaseElement
    , public virtual TCompositeElement
{
public:
    double GetSpecifiedBurstRatio() const override;

    double GetSpecifiedResourceFlowRatio() const override;

    bool IsFairShareTruncationInFifoPoolEnabled() const override;

    bool CanAcceptFreeVolume() const override;

    bool ShouldDistributeFreeVolumeAmongChildren() const override;

    bool ShouldComputePromisedGuaranteeFairShare() const override;

    bool IsPriorityStrongGuaranteeAdjustmentEnabled() const override;

    bool IsPriorityStrongGuaranteeAdjustmentDonorshipEnabled() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TBasePool
    : public virtual TBaseCompositeElement
    , public TPool
{
public:
    TResourceVector GetIntegralShareLimitForRelaxedPool() const override;

    const TIntegralResourcesState& IntegralResourcesState() const override;

    TIntegralResourcesState& IntegralResourcesState() override;

    EIntegralGuaranteeType GetIntegralGuaranteeType() const override;

private:
    TIntegralResourcesState IntegralResourcesState_;
};

////////////////////////////////////////////////////////////////////////////////

class TBaseRootElement
    : public virtual TBaseCompositeElement
    , public TRootElement
{ };

////////////////////////////////////////////////////////////////////////////////

class TBaseOperationElement
    : public virtual TBaseElement
    , public TOperationElement
{
public:
    bool IsFairShareTruncationInFifoPoolAllowed() const override;
};

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NVectorHdrf

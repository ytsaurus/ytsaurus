#pragma once

#include <yt/yt/flow/library/cpp/common/computation_controller.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TComputationControllerMockBase
    : public IComputationController
{
public:
    TComputationControllerMockBase();

    const TCancelableContextPtr& GetCancelableContext() const override;

private:
    const TCancelableContextPtr CancelableContext_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

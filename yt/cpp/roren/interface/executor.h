#pragma once

#include "fwd.h"

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

class IExecutor
    : public NYT::TRefCounted
{
public:
    virtual void Run(const TPipeline& pipeline) = 0;

    // TODO: should be removed
    virtual bool EnableDefaultPipelineOptimization() const
    {
        return false;
    }
};

DEFINE_REFCOUNTED_TYPE(IExecutor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren

#pragma once

#include "fwd.h"

#include <util/generic/ptr.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

class IExecutor
    : public TThrRefBase
{
public:
    virtual void Run(const TPipeline& pipeline) = 0;

    // TODO: should be removed
    virtual bool EnableDefaultPipelineOptimization() const
    {
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren

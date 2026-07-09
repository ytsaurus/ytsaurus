#pragma once

#include "describe_pipeline.h"

namespace NYT::NFlow::NDescribe {

////////////////////////////////////////////////////////////////////////////////

//! Returns an unrolled copy of |original|.
TPipelineDescription UnrollPipelineDescription(const TPipelineDescription& original);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe

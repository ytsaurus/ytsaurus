#pragma once

#include <util/datetime/base.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

inline const TDuration DefaultWaitPipelineTimeout = TDuration::Seconds(600);
inline const TDuration DefaultWaitPipelineStateRequestTimeout = TDuration::Seconds(60);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

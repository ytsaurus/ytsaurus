#pragma once

#include <yt/yt/core/misc/ref_counted.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TQueueAgentStageChannelConfig)
DECLARE_REFCOUNTED_CLASS(TQueueAgentConnectionConfig)

inline const TString ProductionStage = "production";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient

#pragma once

#include "sink_test_environment.h"

namespace NYT::NFlow::NYdbTesting {

////////////////////////////////////////////////////////////////////////////////

void CheckCheckpointIsWrittenBeforeDistributionCallbacks(const TSinkSpecShape& shape);

void CheckQueuedReconfigurationsApplyTheirOwnBatching(
    const TSinkSpecShape& shape,
    const std::string& parameterName,
    i64 valueA,
    size_t writesUnderA,
    i64 valueB,
    size_t writesUnderB);

void CheckRejectedPreparationTimeoutKeepsAcceptedOptions(const TSinkSpecShape& shape);

void CheckPreparationAttemptUsesOneOptionsSnapshot(const TSinkSpecShape& shape);

void CheckSimultaneousComputationsUseTheirOwnResources(const TSinkSpecShape& shape);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYdbTesting

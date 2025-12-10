#pragma once

#include "helpers.h"
#include "record_consumer.h"

namespace NYT::NSequoiaReconstructor {

////////////////////////////////////////////////////////////////////////////////

void ExecutePathToNodeReduce(
    const std::vector<TPathToNodeChangeRecord>& records,
    TRecordsConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaReconstructor

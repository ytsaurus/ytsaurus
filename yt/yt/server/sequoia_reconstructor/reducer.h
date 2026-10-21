#pragma once

#include "private.h"

#include <yt/cpp/mapreduce/interface/operation.h>

namespace NYT::NSequoiaReconstructor {

////////////////////////////////////////////////////////////////////////////////

void ExecutePathToNodeReduce(
    std::vector<TPathToNodeChangeRecord> records,
    TRecordsConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

::TIntrusivePtr<IReducerBase> CreateYtReducer(std::optional<int> pathToNodeIdTableIndex, std::optional<int> pathForksTableIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaReconstructor

#pragma once

#include <ydb-cpp-sdk/client/types/executor/executor.h>

namespace NYdb::inline V3::NTopic {

IExecutor::TPtr CreateSyncExecutor();

} // namespace NYdb::NTopic

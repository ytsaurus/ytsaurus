#pragma once

#include "registration_manager.h"

namespace NYT::NQueueClient::NDetail {

////////////////////////////////////////////////////////////////////////////////

TQueueConsumerRegistrationManagerBasePtr CreateQueueConsumerRegistrationManagerNewImpl(
    TQueueConsumerRegistrationManagerConfigPtr config,
    TWeakPtr<NApi::NNative::IConnection> connection,
    std::optional<std::string> clusterName,
    IInvokerPtr invoker,
    NProfiling::TProfiler profiler,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NQueueClient::NDetail

#include "event_log.h"

#include <yt/ytlib/api/native/connection.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

NEventLog::IEventLogWriterPtr CreateRemoteEventLogWriter(const TRemoteEventLogConfigPtr& config, const IInvokerPtr& invoker)
{
    NApi::NNative::TConnectionOptions connectionOptions;
    connectionOptions.RetryRequestQueueSizeLimitExceeded = true;
    auto connection = NApi::NNative::CreateConnection(config->Connection, connectionOptions);

    NApi::TClientOptions clientOptions;
    clientOptions.PinnedUser = config->User;
    auto client = connection->CreateNativeClient(clientOptions);

    return New<NEventLog::TEventLogWriter>(
        config->EventLogManager,
        client,
        invoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
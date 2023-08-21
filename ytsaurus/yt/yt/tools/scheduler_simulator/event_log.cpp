#include "event_log.h"

#include <yt/yt/ytlib/api/native/connection.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

NEventLog::IEventLogWriterPtr CreateRemoteEventLogWriter(const TRemoteEventLogConfigPtr& config, const IInvokerPtr& invoker)
{
    NApi::NNative::TConnectionOptions connectionOptions;
    connectionOptions.RetryRequestQueueSizeLimitExceeded = true;
    auto connection = NApi::NNative::CreateConnection(config->Connection, connectionOptions);

    auto clientOptions = NApi::TClientOptions::FromUser(config->User);
    auto client = connection->CreateNativeClient(clientOptions);

    return CreateStaticTableEventLogWriter(
        config->EventLogManager,
        client,
        invoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator

#include "queue_commands.h"
#include "config.h"

#include <yt/yt/client/api/config.h>

namespace NYT::NDriver {

using namespace NConcurrency;
using namespace NApi;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TRegisterQueueConsumerCommand::TRegisterQueueConsumerCommand()
{
    RegisterParameter("queue_path", QueuePath);
    RegisterParameter("consumer_path", ConsumerPath);
    RegisterParameter("vital", Vital);

    RegisterPostprocessor([&] {
        QueuePath = QueuePath.Normalize();
        ConsumerPath = ConsumerPath.Normalize();
    });
}

void TRegisterQueueConsumerCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto asyncResult = client->RegisterQueueConsumer(
        QueuePath,
        ConsumerPath,
        Vital,
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TUnregisterQueueConsumerCommand::TUnregisterQueueConsumerCommand()
{
    RegisterParameter("queue_path", QueuePath);
    RegisterParameter("consumer_path", ConsumerPath);

    RegisterPostprocessor([&] {
        QueuePath = QueuePath.Normalize();
        ConsumerPath = ConsumerPath.Normalize();
    });
}

void TUnregisterQueueConsumerCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto asyncResult = client->UnregisterQueueConsumer(
        QueuePath,
        ConsumerPath,
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver

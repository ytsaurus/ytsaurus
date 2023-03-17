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

TListQueueConsumerRegistrationsCommand::TListQueueConsumerRegistrationsCommand()
{
    RegisterParameter("queue_path", QueuePath)
        .Default();
    RegisterParameter("consumer_path", ConsumerPath)
        .Default();

    RegisterPostprocessor([&] {
        if (QueuePath) {
            QueuePath = QueuePath->Normalize();
        }
        if (ConsumerPath) {
            ConsumerPath = ConsumerPath->Normalize();
        }
    });
}

void TListQueueConsumerRegistrationsCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto asyncResult = client->ListQueueConsumerRegistrations(
        QueuePath,
        ConsumerPath,
        Options);
    auto registrations = WaitFor(asyncResult)
        .ValueOrThrow();

    ProduceOutput(context, [&](NYson::IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .DoListFor(registrations, [=] (TFluentList fluent, const TListQueueConsumerRegistrationsResult& registration) {
                fluent
                    .Item().BeginMap()
                        .Item("queue_path").Value(registration.QueuePath)
                        .Item("consumer_path").Value(registration.ConsumerPath)
                        .Item("vital").Value(registration.Vital)
                    .EndMap();
            });
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver

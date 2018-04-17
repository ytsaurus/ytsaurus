#include "command.h"
#include "config.h"

#include <yt/core/misc/error.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

void ProduceOutput(
    ICommandContextPtr context,
    std::function<void(NYson::IYsonConsumer*)> producerV3,
    std::function<void(NYson::IYsonConsumer*)> producerV4)
{
    TStringStream stream;
    NYson::TYsonWriter writer(&stream, NYson::EYsonFormat::Text);
    switch (context->GetConfig()->ApiVersion) {
        case ApiVersion3:
            producerV3(&writer);
            break;
        case ApiVersion4:
            producerV4(&writer);
            break;
        default:
            THROW_ERROR_EXCEPTION("Unsupported API version: %v", context->GetConfig()->ApiVersion);
    }
    writer.Flush();
    context->ProduceOutputValue(NYson::TYsonString(stream.Str()));
}

void ProduceEmptyOutput(ICommandContextPtr context)
{
    switch (context->GetConfig()->ApiVersion) {
        case ApiVersion3:
            break;
        case ApiVersion4:
            context->ProduceOutputValue(NYTree::BuildYsonStringFluently().BeginMap().EndMap());
            break;
        default:
            THROW_ERROR_EXCEPTION("Unsupported API version %v", context->GetConfig()->ApiVersion);
    }
}

void ProduceSingleOutput(
    ICommandContextPtr context,
    const TStringBuf& name,
    std::function<void(NYson::IYsonConsumer*)> producer)
{
    ProduceOutput(context, producer, [&](NYson::IYsonConsumer* consumer) {
        NYTree::BuildYsonFluently(consumer)
            .BeginMap()
                .Item(name).Do([&](NYTree::TFluentAny fluent) {
                    producer(fluent.GetConsumer());
                })
            .EndMap();
    });
}

////////////////////////////////////////////////////////////////////////////////

TCommandBase::TCommandBase()
{
    SetUnrecognizedStrategy(NYTree::EUnrecognizedStrategy::Keep);
}

void TCommandBase::Execute(ICommandContextPtr context)
{
    const auto& request = context->Request();
    Logger.AddTag("RequestId: %" PRIx64 ", User: %v",
        request.Id,
        request.AuthenticatedUser);
    Deserialize(*this, request.Parameters);
    DoExecute(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

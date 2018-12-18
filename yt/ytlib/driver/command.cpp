#include "command.h"
#include "config.h"

#include <yt/core/misc/error.h>

#include <yt/core/ypath/tokenizer.h>

namespace NYT::NDriver {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

void ProduceOutput(
    ICommandContextPtr context,
    std::function<void(NYson::IYsonConsumer*)> producerV3,
    std::function<void(NYson::IYsonConsumer*)> producerV4)
{
    TStringStream stream;
    NYson::TYsonWriter writer(&stream, NYson::EYsonFormat::Binary);
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
    TStringBuf name,
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

    RegisterParameter("rewrite_operation_path", RewriteOperationPath)
        .Default(true);
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

void TCommandBase::ProduceResponseParameters(
    ICommandContextPtr context,
    std::function<void(NYson::IYsonConsumer*)> producer)
{
    producer(context->Request().ResponseParametersConsumer);
    if (context->Request().ParametersFinishedCallback) {
        context->Request().ParametersFinishedCallback();
    }
}


////////////////////////////////////////////////////////////////////////////////

// Keep sync with yt/ytlib/scheduler/helpers.cpp.
TYPath GetNewOperationPath(TGuid operationId)
{
    int hashByte = operationId.Parts32[0] & 0xff;
    return
        "//sys/operations/" +
        Format("%02x", hashByte) +
        "/" +
        ToYPathLiteral(ToString(operationId));
}

TYPath RewritePath(const TYPath& path, bool rewriteOperationPath)
{
    if (!rewriteOperationPath) {
        return path;
    }

    std::vector<std::pair<ETokenType, TString>> expectedTokens = {
        {ETokenType::Slash, "/"},
        {ETokenType::Slash, "/"},
        {ETokenType::Literal, "sys"},
        {ETokenType::Slash, "/"},
        {ETokenType::Literal, "operations"},
        {ETokenType::Slash, "/"},
    };

    TTokenizer tokenizer(path);
    tokenizer.Advance();

    for (const auto& pair : expectedTokens) {
        auto expectedTokenType = pair.first;
        const auto& expectedTokenValue = pair.second;
        if (expectedTokenType != tokenizer.GetType() ||
            expectedTokenValue != tokenizer.GetToken())
        {
            return path;
        }
        tokenizer.Advance();
    }

    if (tokenizer.GetType() != ETokenType::Literal) {
        return path;
    }

    TGuid operationId;
    if (!TGuid::FromString(tokenizer.GetToken(), &operationId)) {
        return path;
    }

    return GetNewOperationPath(operationId) + tokenizer.GetSuffix();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver

#include "command.h"
#include "config.h"

#include <yt/client/object_client/helpers.h>

#include <yt/client/security_client/helpers.h>

#include <yt/core/misc/error.h>

#include <yt/core/ypath/tokenizer.h>

namespace NYT::NDriver {

using namespace NYPath;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////


void ProduceOutput(
    ICommandContextPtr context,
    const std::function<void(IYsonConsumer*)>& producer)
{
    TStringStream stream;
    TYsonWriter writer(&stream, EYsonFormat::Binary);
    producer(&writer);
    writer.Flush();
    context->ProduceOutputValue(TYsonString(stream.Str()));
}

void ProduceEmptyOutput(ICommandContextPtr context)
{
    switch (context->GetConfig()->ApiVersion) {
        case ApiVersion3:
            break;
        default:
            context->ProduceOutputValue(NYTree::BuildYsonStringFluently().BeginMap().EndMap());
            break;
    }
}

void ProduceSingleOutput(
    ICommandContextPtr context,
    TStringBuf name,
    const std::function<void(IYsonConsumer*)>& producer)
{
    switch (context->GetConfig()->ApiVersion) {
        case ApiVersion3:
            ProduceOutput(context, producer);
            break;
        default:
            ProduceOutput(context, [&] (IYsonConsumer* consumer) {
                NYTree::BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item(name).Do([&] (auto fluent) {
                            producer(fluent.GetConsumer());
                        })
                    .EndMap();
            });
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

TCommandBase::TCommandBase()
{
    SetUnrecognizedStrategy(NYTree::EUnrecognizedStrategy::Keep);

    RegisterParameter("rewrite_operation_path", RewriteOperationPathOption)
        .Default();
}

void TCommandBase::Execute(ICommandContextPtr context)
{
    const auto& request = context->Request();
    Logger.AddTag("RequestId: %" PRIx64 ", User: %v",
        request.Id,
        request.AuthenticatedUser);
    Deserialize(*this, request.Parameters);
    if (!RewriteOperationPathOption) {
        RewriteOperationPathOption = context->GetConfig()->RewriteOperationPath;
    }
    RewriteOperationPath = RewriteOperationPathOption
        ? *RewriteOperationPathOption
        : true;

    DoExecute(context);
}

void TCommandBase::ProduceResponseParameters(
    ICommandContextPtr context,
    const std::function<void(IYsonConsumer*)>& producer)
{
    producer(context->Request().ResponseParametersConsumer);
    if (context->Request().ResponseParametersFinishedCallback) {
        context->Request().ResponseParametersFinishedCallback();
    }
}

bool TCommandBase::ValidateSuperuserPermissions(const ICommandContextPtr& context) const
{
    const auto& userName = context->Request().AuthenticatedUser;
    if (userName == NSecurityClient::RootUserName) {
        return true;
    }

    auto pathToGroupYsonList = NSecurityClient::GetUserPath(userName) + "/@member_of_closure";
    auto groupYsonList = WaitFor(context->GetClient()->GetNode(pathToGroupYsonList)).ValueOrThrow();

    auto groups = ConvertTo<THashSet<TString>>(groupYsonList);
    YT_LOG_DEBUG("User group membership info received (Name: %v, Groups: %v)",
        userName,
        groups);
    return groups.contains(NSecurityClient::SuperusersGroupName);
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

    using NYPath::ETokenType;
    static const std::vector<std::pair<ETokenType, TString>> expectedTokens = {
        {ETokenType::Slash, "/"},
        {ETokenType::Slash, "/"},
        {ETokenType::Literal, "sys"},
        {ETokenType::Slash, "/"},
        {ETokenType::Literal, "operations"},
        {ETokenType::Slash, "/"},
    };

    NYPath::TTokenizer tokenizer(path);
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

#include "stdafx.h"
#include "ypath_detail.h"
#include "rpc.pb.h"

#include <ytlib/actions/action_util.h>
#include <ytlib/bus/message.h>
#include <ytlib/rpc/server_detail.h>
#include <ytlib/rpc/message.h>

namespace NYT {
namespace NYTree {

using namespace NBus;
using namespace NRpc;
using namespace NRpc::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = YTreeLogger;

///////////////////////////////////////////////////////////////////////////////

TYPath YPathRoot("/");

///////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TYPathServiceBase::Resolve(const TYPath& path, const Stroka& verb)
{
    if (IsFinalYPath(path)) {
        return ResolveSelf(path, verb);
    } else if (IsAttributeYPath(path)) {
        return ResolveAttributes(path, verb);
    } else {
        return ResolveRecursive(path, verb);
    }
}

IYPathService::TResolveResult TYPathServiceBase::ResolveSelf(const TYPath& path, const Stroka& verb)
{
    UNUSED(verb);
    return TResolveResult::Here(path);
}

IYPathService::TResolveResult TYPathServiceBase::ResolveAttributes(const TYPath& path, const Stroka& verb)
{
    UNUSED(path);
    UNUSED(verb);
    ythrow yexception() << "YPath resolution for attributes is not supported";
}

IYPathService::TResolveResult TYPathServiceBase::ResolveRecursive(const TYPath& path, const Stroka& verb)
{
    UNUSED(path);
    UNUSED(verb);
    ythrow yexception() << "YPath resolution is not supported";
}

void TYPathServiceBase::Invoke(IServiceContext* context)
{
    try {
        DoInvoke(context);
    } catch (...) {
        context->Reply(TError(
            EYPathErrorCode::GenericError,
            CurrentExceptionMessage()));
    }
}

void TYPathServiceBase::DoInvoke(IServiceContext* context)
{
    UNUSED(context);
    ythrow TServiceException(EErrorCode::NoSuchVerb) <<
        "Verb is not supported";
}

////////////////////////////////////////////////////////////////////////////////

TNodeSetterBase::TNodeSetterBase(INode* node, ITreeBuilder* builder)
    : Node(node)
    , TreeBuilder(builder)
    , NodeFactory(node->CreateFactory())
{ }

void TNodeSetterBase::ThrowInvalidType(ENodeType actualType)
{
    ythrow yexception() << Sprintf("Invalid node type (Expected: %s, Actual: %s)",
        ~GetExpectedType().ToString().Quote(),
        ~actualType.ToString().Quote());
}

void TNodeSetterBase::OnMyStringScalar(const Stroka& value, bool hasAttributes)
{
    UNUSED(value);
    UNUSED(hasAttributes);
    ThrowInvalidType(ENodeType::String);
}

void TNodeSetterBase::OnMyInt64Scalar(i64 value, bool hasAttributes)
{
    UNUSED(value);
    UNUSED(hasAttributes);
    ThrowInvalidType(ENodeType::Int64);
}

void TNodeSetterBase::OnMyDoubleScalar(double value, bool hasAttributes)
{
    UNUSED(value);
    UNUSED(hasAttributes);
    ThrowInvalidType(ENodeType::Double);
}

void TNodeSetterBase::OnMyEntity(bool hasAttributes)
{
    UNUSED(hasAttributes);
    ThrowInvalidType(ENodeType::Entity);
}

void TNodeSetterBase::OnMyBeginList()
{
    ThrowInvalidType(ENodeType::List);
}

void TNodeSetterBase::OnMyBeginMap()
{
    ThrowInvalidType(ENodeType::Map);
}

void TNodeSetterBase::OnMyBeginAttributes()
{
    auto attributes = Node->GetAttributes();
    if (!attributes) {
        Node->SetAttributes(~NodeFactory->CreateMap());
    } else {
        attributes->Clear();
    }
}

void TNodeSetterBase::OnMyAttributesItem(const Stroka& name)
{
    YASSERT(!AttributeBuilder);
    AttributeName = name;
    AttributeBuilder = CreateBuilderFromFactory(~NodeFactory);
    AttributeBuilder->BeginTree();
    ForwardNode(~AttributeBuilder, ~FromMethod(&TThis::OnForwardingFinished, this));
}

void TNodeSetterBase::OnForwardingFinished()
{
    YASSERT(~AttributeBuilder);
    YVERIFY(Node->GetAttributes()->AddChild(~AttributeBuilder->EndTree(), AttributeName));
    AttributeBuilder.Destroy();
    AttributeName.clear();
}

void TNodeSetterBase::OnMyEndAttributes()
{
    if (Node->GetAttributes()->GetChildCount() == 0) {
        Node->SetAttributes(NULL);
    }
}

////////////////////////////////////////////////////////////////////////////////

TYPath ChopYPathRootMarker(const TYPath& path)
{
    if (path.empty()) {
        ythrow yexception() << Sprintf("YPath cannot be empty, use \"%s\" to denote the root", ~YPathRoot);
    }

    if (!path.has_prefix(YPathRoot)) {
        ythrow yexception() << Sprintf("YPath must start with \"%s\"", ~YPathRoot);
    }

    return path.substr(YPathRoot.size());
}

void ChopYPathToken(
    const TYPath& path,
    Stroka* prefix,
    TYPath* suffixPath)
{
    size_t index = path.find_first_of("/@");
    if (index == TYPath::npos) {
        *prefix = path;
        *suffixPath = TYPath(path.end(), static_cast<size_t>(0));
    } else {
        switch (path[index]) {
            case '/':
                *prefix = path.substr(0, index);
                *suffixPath =
                    index == path.length() - 1
                    ? path.substr(index)
                    : path.substr(index + 1);
                break;

            case '@':
                *prefix = path.substr(0, index);
                *suffixPath = path.substr(index);
                break;

            default:
                YUNREACHABLE();
        }
    }
}

TYPath ComputeResolvedYPath(
    const TYPath& wholePath,
    const TYPath& unresolvedPath)
{
    int resolvedLength = static_cast<int>(wholePath.length()) - static_cast<int>(unresolvedPath.length());
    YASSERT(resolvedLength >= 0 && resolvedLength <= static_cast<int>(wholePath.length()));
    YASSERT(wholePath.substr(resolvedLength) == unresolvedPath);
    // Take care of trailing slash but don't reduce / to empty string.
    return
        resolvedLength > 1 && wholePath[resolvedLength - 1] == '/'
        ? wholePath.substr(0, resolvedLength - 1)
        : wholePath.substr(0, resolvedLength);
}

TYPath CombineYPaths(
    const TYPath& prefixPath,
    const TYPath& suffixPath)
{
    if (prefixPath.empty() || suffixPath.empty()) {
        return prefixPath + suffixPath;
    }
    if (prefixPath.back() == '/' && suffixPath[0] == '/') {
        return prefixPath + suffixPath.substr(1);
    }

    if (prefixPath.back() != '/' && suffixPath[0] != '/') {
        return prefixPath + '/' + suffixPath;
    }

    return prefixPath + suffixPath;
}

bool IsEmptyYPath(const TYPath& path)
{
    return path.empty();
}

bool IsFinalYPath(const TYPath& path)
{
    return path.empty() || (path == YPathRoot);
}

bool IsAttributeYPath(const TYPath& path)
{
    return !path.empty() && path[0] == '@';
}

TYPath ChopYPathAttributeMarker(const TYPath& path)
{
    return path.substr(1);
}

bool IsLocalYPath(const TYPath& path)
{
    // The empty path is handled by the virtual node itself.
    // All other paths (including "/") are forwarded to the service.
    // Thus "/virtual" denotes the virtual node while "/virtual/" denotes its content.
    // Same applies to the attributes (cf. "/virtual@" vs "/virtual/@").
    return IsEmptyYPath(path) || IsAttributeYPath(path);
}

////////////////////////////////////////////////////////////////////////////////

class TServiceContext
    : public TServiceContextBase
{
public:
    TServiceContext(
        const TRequestHeader& header,
        NBus::IMessage* requestMessage,
        TYPathResponseHandler* responseHandler,
        const Stroka& loggingCategory)
        : TServiceContextBase(header, requestMessage)
        , ResponseHandler(responseHandler)
        , Logger(loggingCategory)
    { }

protected:
    TYPathResponseHandler::TPtr ResponseHandler;
    NLog::TLogger Logger;

    virtual void DoReply(const TError& error, IMessage* responseMessage)
    {
        UNUSED(error);

        if (ResponseHandler) {
            ResponseHandler->Do(responseMessage);
        }
    }

    virtual void LogRequest()
    {
        Stroka str;
        AppendInfo(str, RequestInfo);
        LOG_DEBUG("%s %s <- %s",
            ~Verb,
            ~Path,
            ~str);
    }

    virtual void LogResponse(const TError& error)
    {
        Stroka str;
        AppendInfo(str, Sprintf("Error: %s", ~error.ToString()));
        AppendInfo(str, ResponseInfo);
        LOG_DEBUG("%s %s -> %s",
            ~Verb,
            ~Path,
            ~str);
    }

    virtual void LogException(const Stroka& message)
    {
        Stroka str;
        AppendInfo(str, Sprintf("Path: %s", ~Path));
        AppendInfo(str, Sprintf("Verb: %s", ~Verb));
        AppendInfo(str, ResponseInfo);
        LOG_FATAL("Unhandled exception in YPath service method (%s)\n%s",
            ~str,
            ~message);
    }

};

////////////////////////////////////////////////////////////////////////////////

void ResolveYPath(
    IYPathService* rootService,
    const TYPath& path,
    const Stroka& verb,
    IYPathService::TPtr* suffixService,
    TYPath* suffixPath)
{
    YASSERT(rootService);
    YASSERT(suffixService);
    YASSERT(suffixPath);

    IYPathService::TPtr currentService = rootService;
    auto currentPath = ChopYPathRootMarker(path);

    while (true) {
        IYPathService::TResolveResult result;
        try {
            result = currentService->Resolve(currentPath, verb);
        } catch (...) {
            ythrow yexception() << Sprintf("Error during YPath resolution (Path: %s, Verb: %s, ResolvedPath: %s)\n%s",
                ~path,
                ~verb,
                ~ComputeResolvedYPath(path, currentPath),
                ~CurrentExceptionMessage());
        }

        if (result.IsHere()) {
            *suffixService = currentService;
            *suffixPath = result.GetPath();
            break;
        }

        currentService = result.GetService();
        currentPath = result.GetPath();
    }
}

////////////////////////////////////////////////////////////////////////////////

void WrapYPathRequest(
    NRpc::TClientRequest* outerRequest,
    NBus::IMessage* innerRequestMessage)
{
    YASSERT(outerRequest);
    YASSERT(innerRequestMessage);

    const auto& parts = innerRequestMessage->GetParts();
    outerRequest->Attachments() = yvector<TSharedRef>(parts.begin(), parts.end());
}

NBus::IMessage::TPtr UnwrapYPathRequest(
    NRpc::IServiceContext* outerContext)
{
    YASSERT(outerContext);

    const auto& parts = outerContext->RequestAttachments();
    YASSERT(parts.ysize() >= 2);

    return CreateMessageFromParts(parts);
}

void WrapYPathResponse(
    NRpc::IServiceContext* outerContext,
    NBus::IMessage* responseMessage)
{
    YASSERT(outerContext);
    YASSERT(responseMessage);

    outerContext->ResponseAttachments() = MoveRV(responseMessage->GetParts());
}

NRpc::IServiceContext::TPtr CreateYPathContext(
    NBus::IMessage* requestMessage,
    const TYPath& path,
    const Stroka& verb,
    const Stroka& loggingCategory,
    TYPathResponseHandler* responseHandler)
{
    YASSERT(requestMessage);

    TRequestHeader header;
    header.set_path(path);
    header.set_verb(verb);
    return New<TServiceContext>(
        header,
        requestMessage,
        responseHandler,
        loggingCategory);
}

NBus::IMessage::TPtr UnwrapYPathResponse(
    TClientResponse* outerResponse)
{
    YASSERT(outerResponse);

    auto parts = outerResponse->Attachments();
    return CreateMessageFromParts(parts);
}

void ReplyYPathWithMessage(
    NRpc::IServiceContext* context,
    NBus::IMessage* responseMessage)
{
    auto parts = responseMessage->GetParts();
    YASSERT(!parts.empty());

    TResponseHeader header;
    if (!DeserializeProtobuf(&header, parts[0])) {
        LOG_FATAL("Error deserializing YPath response header");
    }

    TError error(
        header.error_code(),
        header.has_error_message() ? header.error_message() : "");

    if (error.IsOK()) {
        YASSERT(parts.ysize() >= 2);
        
        context->SetResponseBody(parts[1]);
        
        parts.erase(parts.begin(), parts.begin() + 2);
        context->ResponseAttachments() = MoveRV(parts);
    }

    context->Reply(error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

#include "stdafx.h"
#include "ypath_detail.h"
#include "rpc.pb.h"

#include "../actions/action_util.h"
#include "../bus/message.h"
#include "../rpc/server_detail.h"
#include "../rpc/message.h"

namespace NYT {
namespace NYTree {

using namespace NBus;
using namespace NRpc;
using namespace NRpc::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = YTreeLogger;

////////////////////////////////////////////////////////////////////////////////

TNodeSetterBase::TNodeSetterBase(INode* node, ITreeBuilder* builder)
    : Node(node)
    , Builder(builder)
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

void TNodeSetterBase::OnMyListItem()
{
    YUNREACHABLE();
}

void TNodeSetterBase::OnMyEndList(bool hasAttributes)
{
    UNUSED(hasAttributes);
    YUNREACHABLE();
}

void TNodeSetterBase::OnMyBeginMap()
{
    ThrowInvalidType(ENodeType::Map);
}

void TNodeSetterBase::OnMyMapItem(const Stroka& name)
{
    UNUSED(name);
    YUNREACHABLE();
}

void TNodeSetterBase::OnMyEndMap(bool hasAttributes)
{
    UNUSED(hasAttributes);
    YUNREACHABLE();
}

void TNodeSetterBase::OnMyBeginAttributes()
{
    auto attributes = Node->GetAttributes();
    if (~attributes == NULL) {
        Node->SetAttributes(Node->GetFactory()->CreateMap());
    } else {
        attributes->Clear();
    }
}

void TNodeSetterBase::OnMyAttributesItem(const Stroka& name)
{
    YASSERT(~AttributeBuilder == NULL);
    AttributeName = name;
    AttributeBuilder = CreateBuilderFromFactory(Node->GetFactory());
    AttributeBuilder->BeginTree();
    ForwardNode(~AttributeBuilder, FromMethod(&TThis::OnForwardingFinished, this));
}

void TNodeSetterBase::OnForwardingFinished()
{
    YASSERT(~AttributeBuilder != NULL);
    Node->GetAttributes()->AddChild(AttributeBuilder->EndTree(), AttributeName);
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

void ChopYPathToken(
    TYPath path,
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
    TYPath wholePath,
    TYPath unresolvedPath)
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

TYPath ParseYPathRoot(TYPath path)
{
    if (path.empty()) {
        ythrow yexception() << "YPath cannot be empty, use \"/\" to denote the root";
    }

    if (path[0] != '/') {
        ythrow yexception() << "YPath must start with \"/\"";
    }

    return path.substr(1);
}


bool IsEmptyYPath(TYPath path)
{
    return path.empty();
}

bool IsFinalYPath(TYPath path)
{
    return path.empty() || path == "/";
}

bool HasYPathAttributeMarker(TYPath path)
{
    return !path.empty() && path[0] == '@';
}

TYPath ChopYPathAttributeMarker(TYPath path)
{
    return path.substr(1);
}

////////////////////////////////////////////////////////////////////////////////

class TServiceContext
    : public TServiceContextBase
{
public:
    TServiceContext(
        const Stroka& path,
        const Stroka& verb,
        NBus::IMessage* requestMessage,
        TYPathResponseHandler* responseHandler,
        const Stroka& loggingCategory)
        : TServiceContextBase(TRequestId(), path, verb, requestMessage)
        , ResponseHandler(responseHandler)
        , Logger(loggingCategory)
    { }

protected:
    TYPathResponseHandler::TPtr ResponseHandler;
    NLog::TLogger Logger;

    virtual void DoReply(const TError& error, IMessage* responseMessage)
    {
        if (~ResponseHandler != NULL) {
            TYPathResponseHandlerParam response;
            response.Message = responseMessage;
            response.Error = error;
            ResponseHandler->Do(response);
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
    TYPath path,
    const Stroka& verb,
    IYPathService::TPtr* suffixService,
    TYPath* suffixPath)
{
    YASSERT(rootService != NULL);
    YASSERT(suffixService != NULL);
    YASSERT(suffixPath != NULL);

    IYPathService::TPtr currentService = rootService;
    auto currentPath = ParseYPathRoot(path);

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

IYPathService::TPtr ResolveYPath(
    IYPathService* rootService,
    TYPath path)
{
    YASSERT(rootService != NULL);

    IYPathService::TPtr suffixService;
    TYPath suffixPath;
    // TODO: killme
    ResolveYPath(rootService, path, "Get", &suffixService, &suffixPath);
    return suffixService;
}

////////////////////////////////////////////////////////////////////////////////

void ParseYPathRequestHeader(
    TRef headerData,
    TYPath* path,
    Stroka* verb)
{
    YASSERT(path != NULL);
    YASSERT(verb != NULL);

    TRequestHeader header;
    if (!DeserializeProtobuf(&header, headerData)) {
        LOG_FATAL("Error deserializing YPath request header");
    }

    *path = header.GetPath();
    *verb = header.GetVerb();
}

IMessage::TPtr UpdateYPathRequestHeader(
    IMessage* message,
    TYPath path,
    const Stroka& verb)
{
    YASSERT(message != NULL);

    TRequestHeader header;
    header.SetRequestId(TRequestId().ToProto());
    header.SetPath(path);
    header.SetVerb(verb);

    TBlob headerData;
    if (!SerializeProtobuf(&header, &headerData)) {
        LOG_FATAL("Error serializing YPath request header");
    }

    auto parts = message->GetParts();
    YASSERT(!parts.empty());
    parts[0] = TSharedRef(MoveRV(headerData));

    return CreateMessageFromParts(parts);
}

void WrapYPathRequest(
    NRpc::TClientRequest* outerRequest,
    NBus::IMessage* innerRequestMessage)
{
    YASSERT(outerRequest != NULL);
    YASSERT(innerRequestMessage != NULL);

    auto parts = innerRequestMessage->GetParts();
    auto& attachments = outerRequest->Attachments();
    attachments.clear();
    std::copy(
        parts.begin(),
        parts.end(),
        std::back_inserter(attachments));
}

NBus::IMessage::TPtr UnwrapYPathRequest(
    NRpc::IServiceContext* outerContext)
{
    YASSERT(outerContext != NULL);

    const auto& parts = outerContext->RequestAttachments();
    YASSERT(parts.ysize() >= 2);

    return CreateMessageFromParts(parts);
}

void WrapYPathResponse(
    NRpc::IServiceContext* outerContext,
    NBus::IMessage* responseMessage)
{
    YASSERT(outerContext != NULL);
    YASSERT(responseMessage != NULL);

    outerContext->ResponseAttachments() = MoveRV(responseMessage->GetParts());
}

NRpc::IServiceContext::TPtr CreateYPathContext(
    NBus::IMessage* requestMessage,
    TYPath path,
    const Stroka& verb,
    const Stroka& loggingCategory,
    TYPathResponseHandler* responseHandler)
{
    YASSERT(requestMessage != NULL);

    return New<TServiceContext>(
        path,
        verb,
        requestMessage,
        responseHandler,
        loggingCategory);
}

NBus::IMessage::TPtr UnwrapYPathResponse(
    TClientResponse* outerResponse)
{
    YASSERT(outerResponse != NULL);

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
        EErrorCode(header.GetErrorCode(), header.GetErrorCodeString()),
        header.GetErrorMessage());

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

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

void WrapYPathRequest(TClientRequest* outerRequest, TYPathRequest* innerRequest)
{
    auto message = innerRequest->Serialize();
    auto parts = message->GetParts();
    auto& attachments = outerRequest->Attachments();
    attachments.clear();
    NStl::copy(
        parts.begin(),
        parts.end(),
        NStl::back_inserter(attachments));
}

void UnwrapYPathResponse(TClientResponse* outerResponse, TYPathResponse* innerResponse)
{
    auto parts = outerResponse->Attachments();
    auto message = CreateMessageFromParts(parts);
    innerResponse->Deserialize(~message);
}

void SetYPathErrorResponse(const NRpc::TError& error, TYPathResponse* innerResponse)
{
    innerResponse->SetError(error);
}

////////////////////////////////////////////////////////////////////////////////

void ChopYPathPrefix(
    TYPath path,
    Stroka* prefix,
    TYPath* tailPath)
{
    size_t index = path.find_first_of("/@");
    if (index == TYPath::npos) {
        *prefix = path;
        *tailPath = TYPath(path.end(), static_cast<size_t>(0));
    } else {
        switch (path[index]) {
            case '/':
                *prefix = path.substr(0, index);
                *tailPath =
                    index == path.length() - 1
                    ? path.substr(index)
                    : path.substr(index + 1);
                break;

            case '@':
                *prefix = path.substr(0, index);
                *tailPath = path.substr(index);
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
    // TODO: trim trailing slash
    return wholePath.substr(0, resolvedLength);
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
        TYPathResponseHandlerParam response;
        response.Message = responseMessage;
        response.Error = error;
        ResponseHandler->Do(response);
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

void ParseYPathRequestHeader(
    IServiceContext* outerContext,
    TYPath* path,
    Stroka* verb)
{
    const auto& attachments = outerContext->GetRequestAttachments();
    YASSERT(!attachments.empty());
    
    TRequestHeader header;
    if (!DeserializeMessage(&header, attachments[0])) {
        LOG_FATAL("Error deserializing YPath request header");
    }

    *path = header.GetPath();
    *verb = header.GetVerb();
}

void NavigateYPath(
    IYPathService* rootService,
    TYPath path,
    bool mustExist,
    IYPathService::TPtr* tailService,
    TYPath* tailPath)
{
    IYPathService::TPtr currentService = rootService;
    auto currentPath = ParseYPathRoot(path);

    while (true) {
        IYPathService::TNavigateResult result;
        try {
            result = currentService->Navigate(currentPath, mustExist);
        } catch (...) {
            ythrow yexception() << Sprintf("Error during YPath navigation (Path: %s, ResolvedPath: %s)\n%s",
                ~path,
                ~ComputeResolvedYPath(path, currentPath),
                ~CurrentExceptionMessage());
        }

        if (result.IsHere()) {
            *tailService = currentService;
            *tailPath = result.GetPath();
            break;
        }

        currentService = result.GetService();
        currentPath = result.GetPath();
    }
}

IYPathService::TPtr NavigateYPath(
    IYPathService* rootService,
    TYPath path)
{
    IYPathService::TPtr tailService;
    TYPath tailPath;
    NavigateYPath(rootService, path, true, &tailService, &tailPath);
    return tailService;
}

NRpc::IServiceContext::TPtr UnwrapYPathRequest(
    NRpc::IServiceContext* outerContext,
    TYPath path,
    const Stroka& verb,
    const Stroka& loggingCategory,
    TYPathResponseHandler* responseHandler)
{
    const auto& attachments = outerContext->GetRequestAttachments();
    YASSERT(attachments.ysize() >= 2);

    yvector<TSharedRef> parts;
    parts.reserve(1 + attachments.ysize());

    // Put RPC header part.
    TRequestHeader header;
    header.SetPath(path);
    header.SetVerb(verb);
    TBlob headerBlob;
    if (!SerializeMessage(&header, &headerBlob)) {
        LOG_FATAL("Error serializing YPath request header");
    }
    parts.push_back(TSharedRef(MoveRV(headerBlob)));

    // Put the encapsulated message (skipping the old RPC header).
    NStl::copy(
        attachments.begin() + 1,
        attachments.end(),
        NStl::back_inserter(parts));

    auto innerMessage = CreateMessageFromParts(parts);

    return New<TServiceContext>(
        path,
        verb,
        ~innerMessage,
        responseHandler,
        loggingCategory);
}

NRpc::IServiceContext::TPtr CreateYPathRequest(
    NBus::IMessage* requestMessage,
    TYPath path,
    const Stroka& verb,
    const Stroka& loggingCategory,
    TYPathResponseHandler* responseHandler)
{
    return New<TServiceContext>(
        path,
        verb,
        requestMessage,
        responseHandler,
        loggingCategory);
}

void WrapYPathResponse(
    NRpc::IServiceContext* outerContext,
    NBus::IMessage* responseMessage)
{
    outerContext->SetResponseAttachments(responseMessage->GetParts());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

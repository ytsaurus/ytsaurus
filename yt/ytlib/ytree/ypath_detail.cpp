#include "stdafx.h"
#include "ypath_detail.h"
#include "ypath_client.h"
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

////////////////////////////////////////////////////////////////////////////////

void ChopYPathToken(
    const TYPath& path,
    Stroka* token,
    TYPath* suffixPath)
{
    size_t index = path.find_first_of("/@");
    if (index == TYPath::npos) {
        *token = path;
        *suffixPath = TYPath(path.end(), static_cast<size_t>(0));
    } else {
        switch (path[index]) {
        case '/':
            *token = path.substr(0, index);
            *suffixPath =
                index == path.length() - 1
                ? path.substr(index)
                : path.substr(index + 1);
            break;

        case '@':
            *token = path.substr(0, index);
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
    const TYPath& path1,
    const TYPath& path2)
{
    if (path2.has_prefix("@")) {
        return path1 + path2;
    }

    if (path1.empty() || path2.empty()) {
        return path1 + path2;
    }

    if (path1.back() == '/' && path2[0] == '/') {
        return path1 + path2.substr(1);
    }

    if (path1.back() != '/' && path2[0] != '/') {
        return path1 + '/' + path2;
    }

    return path1 + path2;
}

TYPath CombineYPaths(
    const TYPath& path1,
    const TYPath& path2,
    const TYPath& path3)
{
    return CombineYPaths(CombineYPaths(path1, path2), path3);
}

bool IsEmptyYPath(const TYPath& path)
{
    return path.empty();
}

bool IsFinalYPath(const TYPath& path)
{
    return path.empty() || path == RootMarker;
}

bool IsAttributeYPath(const TYPath& path)
{
    return path.has_prefix(AttributeMarker);
}

TYPath ChopYPathAttributeMarker(const TYPath& path)
{
    auto result = path.substr(AttributeMarker.length());
    if (result.has_prefix(AttributeMarker)) {
        ythrow yexception() << "Repeated attribute marker in YPath";
    }
    return result;
}

bool IsLocalYPath(const TYPath& path)
{
    // The empty path is handled by the virtual node itself.
    // All other paths (including "/") are forwarded to the service.
    // Thus "/virtual" denotes the virtual node while "/virtual/" denotes its content.
    // Same applies to the attributes (cf. "/virtual@" vs "/virtual/@").
    return IsEmptyYPath(path) || IsAttributeYPath(path);
}

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
    auto currentPath = path;

    while (true) {
        IYPathService::TResolveResult result;
        try {
            result = currentService->Resolve(currentPath, verb);
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error during YPath resolution (Path: %s, Verb: %s, ResolvedPath: %s)\n%s",
                ~path,
                ~verb,
                ~ComputeResolvedYPath(path, currentPath),
                ex.what());
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

TYPathServiceBase::TYPathServiceBase(const Stroka& loggingCategory)
    : Logger(loggingCategory)
{ }

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
    } catch (const TServiceException& ex) {
        context->Reply(ex.GetError());
    }
    catch (const std::exception& ex) {
        context->Reply(TError(ex.what()));
    }
}

void TYPathServiceBase::DoInvoke(IServiceContext* context)
{
    UNUSED(context);
    ythrow TServiceException(EErrorCode::NoSuchVerb) <<
        "Verb is not supported";
}

Stroka TYPathServiceBase::GetLoggingCategory() const
{
    return Logger.GetCategory();
}

bool TYPathServiceBase::IsWriteRequest(IServiceContext* context) const
{
    UNUSED(context);
    return false;
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TSupportsGet, Get)
{
    auto path = context->GetPath();
    if (IsFinalYPath(path)) {
        GetSelf(request, response, ~context);
    } else if (IsAttributeYPath(path)) {
        auto attributePath = ChopYPathAttributeMarker(path);
        GetAttribute(attributePath, request, response, ~context);
    } else {
        GetRecursive(path, request, response, ~context);
    }
}

void TSupportsGet::GetSelf(TReqGet* request, TRspGet* response, TCtxGet* context)
{
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);
    ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported";
}

void TSupportsGet::GetRecursive(const TYPath& path, TReqGet* request, TRspGet* response, TCtxGet* context)
{
    UNUSED(path);
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);
    ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported";
}

void TSupportsGet::GetAttribute(const TYPath& path, TReqGet* request, TRspGet* response, TCtxGet* context)
{
    UNUSED(path);
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);
    ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported";
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TSupportsSet, Set)
{
    auto path = context->GetPath();
    if (IsFinalYPath(path)) {
        SetSelf(request, response, ~context);
    } else if (IsAttributeYPath(path)) {
        auto attributePath = ChopYPathAttributeMarker(path);
        SetAttribute(attributePath, request, response, ~context);
    } else {
        SetRecursive(path, request, response, ~context);
    }
}

void TSupportsSet::SetSelf(TReqSet* request, TRspSet* response, TCtxSet* context)
{
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);
    ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported";
}

void TSupportsSet::SetRecursive(const NYTree::TYPath& path, TReqSet* request, TRspSet* response, TCtxSet* context)
{
    UNUSED(path);
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);
    ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported";
}

void TSupportsSet::SetAttribute(const TYPath& path, TReqSet* request, TRspSet* response, TCtxSet* context)
{
    UNUSED(path);
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);
    ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported";
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TSupportsList, List)
{
    auto path = context->GetPath();
    if (IsFinalYPath(path)) {
        ListSelf(request, response, ~context);
    } else if (IsAttributeYPath(path)) {
        auto attributePath = ChopYPathAttributeMarker(path);
        ListAttribute(attributePath, request, response, ~context);
    } else {
        ListRecursive(path, request, response, ~context);
    }
}

void TSupportsList::ListSelf(TReqList* request, TRspList* response, TCtxList* context)
{
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);
    ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported";
}

void TSupportsList::ListRecursive(const NYTree::TYPath& path, TReqList* request, TRspList* response, TCtxList* context)
{
    UNUSED(path);
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);
    ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported";
}

void TSupportsList::ListAttribute(const TYPath& path, TReqList* request, TRspList* response, TCtxList* context)
{
    UNUSED(path);
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);
    ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported";
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TSupportsRemove, Remove)
{
    auto path = context->GetPath();
    if (IsFinalYPath(path)) {
        RemoveSelf(request, response, ~context);
    } else if (IsAttributeYPath(path)) {
        auto attributePath = ChopYPathAttributeMarker(path);
        RemoveAttribute(attributePath, request, response, ~context);
    } else {
        RemoveRecursive(path, request, response, ~context);
    }
}

void TSupportsRemove::RemoveSelf(TReqRemove* request, TRspRemove* response, TCtxRemove* context)
{
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);
    ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported";
}

void TSupportsRemove::RemoveRecursive(const NYTree::TYPath& path, TReqRemove* request, TRspRemove* response, TCtxRemove* context)
{
    UNUSED(path);
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);
    ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported";
}

void TSupportsRemove::RemoveAttribute(const TYPath& path, TReqRemove* request, TRspRemove* response, TCtxRemove* context)
{
    UNUSED(path);
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);
    ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported";
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
    SyncYPathRemove(~Node, RootMarker + AttributeMarker);
}

void TNodeSetterBase::OnMyAttributesItem(const Stroka& name)
{
    YASSERT(!AttributeWriter);
    AttributeName = name;
    AttributeStream = new TStringOutput(AttributeValue);
    AttributeWriter = new TYsonWriter(AttributeStream.Get());
    ForwardNode(~AttributeWriter, ~FromMethod(&TThis::OnForwardingFinished, this));
}

void TNodeSetterBase::OnForwardingFinished()
{
    SyncYPathSet(~Node, RootMarker + AttributeMarker + AttributeName, AttributeValue);
    AttributeWriter.Destroy();
    AttributeStream.Destroy();
    AttributeName.clear();
}

void TNodeSetterBase::OnMyEndAttributes()
{ }

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

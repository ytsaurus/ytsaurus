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

TYPathServiceBase::TYPathServiceBase(const Stroka& loggingCategory)
    : Logger(loggingCategory)
{ }

IYPathService::TResolveResult TYPathServiceBase::Resolve(const TYPath& path, const Stroka& verb)
{
    if (IsFinalYPath(path)) {
        return ResolveSelf(path, verb);
    } else if (IsAttributeYPath(path)) {
        auto attributePath = ChopYPathAttributeMarker(path);
        return ResolveAttributes(attributePath, verb);
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
    ythrow yexception() << "Object cannot have attributes";
}

IYPathService::TResolveResult TYPathServiceBase::ResolveRecursive(const TYPath& path, const Stroka& verb)
{
    UNUSED(path);
    UNUSED(verb);
    ythrow yexception() << "Object cannot have children";
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

#define IMPLEMENT_SUPPORTS_VERB(verb) \
    DEFINE_RPC_SERVICE_METHOD(TSupports##verb, verb) \
    { \
        auto path = context->GetPath(); \
        if (IsFinalYPath(path)) { \
            verb##Self(request, response, ~context); \
        } else if (IsAttributeYPath(path)) { \
            auto attributePath = ChopYPathAttributeMarker(path); \
            verb##Attribute(attributePath, request, response, ~context); \
        } else { \
            verb##Recursive(path, request, response, ~context); \
        } \
    } \
    \
    void TSupports##verb::verb##Self(TReq##verb* request, TRsp##verb* response, TCtx##verb* context) \
    { \
        UNUSED(request); \
        UNUSED(response); \
        UNUSED(context); \
        ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported"; \
    } \
    \
    void TSupports##verb::verb##Recursive(const TYPath& path, TReq##verb* request, TRsp##verb* response, TCtx##verb* context) \
    { \
        UNUSED(path); \
        UNUSED(request); \
        UNUSED(response); \
        UNUSED(context); \
        ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported"; \
    } \
    \
    void TSupports##verb::verb##Attribute(const TYPath& path, TReq##verb* request, TRsp##verb* response, TCtx##verb* context) \
    { \
        UNUSED(path); \
        UNUSED(request); \
        UNUSED(response); \
        UNUSED(context); \
        ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported"; \
    }

IMPLEMENT_SUPPORTS_VERB(Get)
IMPLEMENT_SUPPORTS_VERB(GetNode)
IMPLEMENT_SUPPORTS_VERB(Set)
IMPLEMENT_SUPPORTS_VERB(SetNode)
IMPLEMENT_SUPPORTS_VERB(List)
IMPLEMENT_SUPPORTS_VERB(Remove)

#undef IMPLEMENT_SUPPORTS_VERB

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

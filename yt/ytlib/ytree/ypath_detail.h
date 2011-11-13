#pragma once

#include "common.h"
#include "ypath_client.h"
#include "yson_events.h"
#include "tree_builder.h"
#include "forwarding_yson_events.h"

#include "../actions/action_util.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TNodeSetterBase
    : public TForwardingYsonConsumer
{
protected:
    TNodeSetterBase(INode* node, ITreeBuilder* builder);

    void ThrowInvalidType(ENodeType actualType);
    virtual ENodeType GetExpectedType() = 0;

    virtual void OnMyStringScalar(const Stroka& value, bool hasAttributes);
    virtual void OnMyInt64Scalar(i64 value, bool hasAttributes);
    virtual void OnMyDoubleScalar(double value, bool hasAttributes);
    virtual void OnMyEntity(bool hasAttributes);

    virtual void OnMyBeginList();
    virtual void OnMyListItem();
    virtual void OnMyEndList(bool hasAttributes);


    virtual void OnMyBeginMap();
    virtual void OnMyMapItem(const Stroka& name);
    virtual void OnMyEndMap(bool hasAttributes);

    virtual void OnMyBeginAttributes();
    virtual void OnMyAttributesItem(const Stroka& name);
    virtual void OnMyEndAttributes();

protected:
    typedef TNodeSetterBase TThis;

    INode::TPtr Node;
    ITreeBuilder* Builder;

    Stroka AttributeName;
    TAutoPtr<ITreeBuilder> AttributeBuilder;

    void OnForwardingFinished();

};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TNodeSetter
{ };

#define DECLARE_SCALAR_TYPE(name, type) \
    template <> \
    class TNodeSetter<I##name##Node> \
        : public TNodeSetterBase \
    { \
    public: \
        TNodeSetter(I##name##Node* node, ITreeBuilder* builder) \
            : TNodeSetterBase(node, builder) \
            , Node(node) \
        { } \
    \
    private: \
        I##name##Node::TPtr Node; \
        \
        virtual ENodeType GetExpectedType() \
        { \
            return ENodeType::name; \
        } \
        \
        virtual void On ## name ## Scalar( \
            NDetail::TScalarTypeTraits<type>::TParamType value, \
            bool hasAttributes) \
        { \
            UNUSED(hasAttributes); \
            Node->SetValue(value); \
        } \
    }

DECLARE_SCALAR_TYPE(String, Stroka);
DECLARE_SCALAR_TYPE(Int64,  i64);
DECLARE_SCALAR_TYPE(Double, double);

#undef DECLARE_SCALAR_TYPE

////////////////////////////////////////////////////////////////////////////////

template <>
class TNodeSetter<IMapNode>
    : public TNodeSetterBase
{
public:
    TNodeSetter(IMapNode* map, ITreeBuilder* builder)
        : TNodeSetterBase(map, builder)
        , Map(map)
    { }

private:
    typedef TNodeSetter<IMapNode> TThis;

    IMapNode::TPtr Map;
    Stroka ItemName;

    virtual ENodeType GetExpectedType()
    {
        return ENodeType::Map;
    }

    virtual void OnMyBeginMap()
    {
        Map->Clear();
    }

    virtual void OnMyMapItem(const Stroka& name)
    {
        ItemName = name;
        Builder->BeginTree();
        ForwardNode(Builder, FromMethod(&TThis::OnForwardingFinished, this));
    }

    void OnForwardingFinished()
    {
        Map->AddChild(Builder->EndTree(), ItemName);
        ItemName.clear();
    }

    virtual void OnMyEndMap(bool hasAttributes)
    {
        UNUSED(hasAttributes);
        // Just do nothing.
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
class TNodeSetter<IListNode>
    : public TNodeSetterBase
{
public:
    TNodeSetter(IListNode* list, ITreeBuilder* builder)
        : TNodeSetterBase(list, builder)
        , List(list)
    { }

private:
    typedef TNodeSetter<IListNode> TThis;

    IListNode::TPtr List;

    virtual ENodeType GetExpectedType()
    {
        return ENodeType::List;
    }

    virtual void OnMyBeginList()
    {
        List->Clear();
    }

    virtual void OnMyListItem()
    {
        Builder->BeginTree();
        ForwardNode(Builder, FromMethod(&TThis::OnForwardingFinished, this));
    }

    void OnForwardingFinished()
    {
        List->AddChild(Builder->EndTree());
    }

    virtual void OnMyEndList(bool hasAttributes)
    {
        UNUSED(hasAttributes);
        // Just do nothing.
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
class TNodeSetter<IEntityNode>
    : public TNodeSetterBase
{
public:
    TNodeSetter(IEntityNode* entity, ITreeBuilder* builder)
        : TNodeSetterBase(entity, builder)
    { }

private:
    virtual ENodeType GetExpectedType()
    {
        return ENodeType::Entity;
    }

    virtual void OnMyEntity(bool hasAttributes)
    {
        UNUSED(hasAttributes);
        // Just do nothing.
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TNode>
void SetNodeFromProducer(
    TNode* node,
    TYsonProducer* producer,
    ITreeBuilder* builder)
{
    YASSERT(node != NULL);
    YASSERT(producer != NULL);
    YASSERT(builder != NULL);

    TNodeSetter<TNode> setter(node, builder);
    producer->Do(&setter);
}

////////////////////////////////////////////////////////////////////////////////

void ChopYPathPrefix(
    TYPath path,
    Stroka* prefix,
    TYPath* tailPath);

TYPath ComputeResolvedYPath(
    TYPath wholePath,
    TYPath unresolvedPath);

////////////////////////////////////////////////////////////////////////////////

void NavigateYPath(
    IYPathService* rootService,
    TYPath path,
    bool mustExist,
    IYPathService::TPtr* tailService,
    TYPath* tailPath);

IYPathService::TPtr NavigateYPath(
    IYPathService* rootService,
    TYPath path);

////////////////////////////////////////////////////////////////////////////////

struct TYPathResponseHandlerParam
{
    NRpc::TError Error;
    NBus::IMessage::TPtr Message;
};

typedef IParamAction<const TYPathResponseHandlerParam&> TYPathResponseHandler;

void ParseYPathRequestHeader(
    TRef headerData,
    TYPath* path,
    Stroka* verb);

void WrapYPathRequest(
    NRpc::TClientRequest* outerRequest,
    TYPathRequest* innerRequest);

NRpc::IServiceContext::TPtr CreateYPathContext(
    NRpc::IServiceContext* outerContext,
    TYPath path,
    const Stroka& verb,
    const Stroka& loggingCategory,
    TYPathResponseHandler* responseHandler);

NRpc::IServiceContext::TPtr CreateYPathContext(
    NBus::IMessage* requestMessage,
    TYPath path,
    const Stroka& verb,
    const Stroka& loggingCategory,
    TYPathResponseHandler* responseHandler);

void WrapYPathResponse(
    NRpc::IServiceContext* outerContext,
    NBus::IMessage* responseMessage);

void UnwrapYPathResponse(
    NRpc::TClientResponse* outerResponse,
    TYPathResponse* innerResponse);

void SetYPathErrorResponse(
    const NRpc::TError& error,
    TYPathResponse* innerResponse);

template <class TTypedRequest, class TTypedResponse>
void OnYPathResponse(
    const TYPathResponseHandlerParam& param,
    TIntrusivePtr< TFuture< TIntrusivePtr<TTypedResponse> > > asyncResponse,
    const Stroka& verb,
    TYPath resolvedPath)
{
    auto response = New<TTypedResponse>();
    response->Deserialize(~param.Message);
    if (!response->IsOK()) {
        auto error = response->GetError();
        Stroka message = Sprintf("Error executing YPath operation (Verb: %s, ResolvedPath: %s)\n%s",
            ~verb,
            ~resolvedPath,
            ~error.GetMessage());
        response->SetError(NRpc::TError(error.GetCode(), message));
    }
    asyncResponse->Set(response);
}

template <class TTypedRequest>
TIntrusivePtr< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >
ExecuteYPath(IYPathService* rootService, TTypedRequest* request)
{
    TYPath path = request->GetPath();
    Stroka verb = request->GetVerb();

    IYPathService::TPtr tailService;
    TYPath tailPath;
    NavigateYPath(rootService, path, false, &tailService, &tailPath);

    // TODO: can we avoid this?
    request->SetPath(tailPath);

    auto requestMessage = request->Serialize();
    auto asyncResponse = New< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >();

    auto context = CreateYPathContext(
        ~requestMessage,
        tailPath,
        verb,
        YTreeLogger.GetCategory(),
        ~FromMethod(
            &OnYPathResponse<TTypedRequest, typename TTypedRequest::TTypedResponse>,
            asyncResponse,
            verb,
            ComputeResolvedYPath(path, tailPath)));

    try {
        tailService->Invoke(~context);
    } catch (const NRpc::TServiceException& ex) {
        context->Reply(NRpc::TError(
            EYPathErrorCode(EYPathErrorCode::GenericError),
            ex.what()));
    }

    return asyncResponse;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

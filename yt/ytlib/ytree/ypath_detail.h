#pragma once

#include "common.h"
#include "ypath_service.h"
#include "yson_consumer.h"
#include "tree_builder.h"
#include "yson_writer.h"
#include "forwarding_yson_consumer.h"
#include "ypath.pb.h"
#include "attributes.h"

#include <ytlib/actions/action_util.h>
#include <ytlib/misc/assert.h>
#include <ytlib/logging/log.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYPathServiceBase
    : public virtual IYPathService
{
public:
    TYPathServiceBase(const Stroka& loggingCategory = YTreeLogger.GetCategory());

    virtual void Invoke(NRpc::IServiceContext* context);
    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb);
    virtual Stroka GetLoggingCategory() const;
    virtual bool IsWriteRequest(NRpc::IServiceContext* context) const;

protected:
    NLog::TLogger Logger;

    virtual void DoInvoke(NRpc::IServiceContext* context);
    virtual TResolveResult ResolveSelf(const TYPath& path, const Stroka& verb);
    virtual TResolveResult ResolveAttributes(const TYPath& path, const Stroka& verb);
    virtual TResolveResult ResolveRecursive(const TYPath& path, const Stroka& verb);

};

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_SUPPORTS_VERB(verb) \
    class TSupports##verb \
    { \
    protected: \
        DECLARE_RPC_SERVICE_METHOD(NProto, verb); \
        virtual void verb##Self(TReq##verb* request, TRsp##verb* response, TCtx##verb* context); \
        virtual void verb##Recursive(const TYPath& path, TReq##verb* request, TRsp##verb* response, TCtx##verb* context); \
        virtual void verb##Attribute(const TYPath& path, TReq##verb* request, TRsp##verb* response, TCtx##verb* context); \
    }

DECLARE_SUPPORTS_VERB(Get);
DECLARE_SUPPORTS_VERB(GetNode);
DECLARE_SUPPORTS_VERB(Set);
DECLARE_SUPPORTS_VERB(SetNode);
DECLARE_SUPPORTS_VERB(List);
DECLARE_SUPPORTS_VERB(Remove);

#undef DECLARE_SUPPORTS_VERB

////////////////////////////////////////////////////////////////////////////////

class TSupportsAttributes
    : public virtual TYPathServiceBase
    , public virtual TSupportsGet
    , public virtual TSupportsList
    , public virtual TSupportsSet
    , public virtual TSupportsRemove
{
protected:
    // TODO(roizner): support NULL user attribute dictionary to
    // allow TVirtualMapBase to use this mix-in.

    virtual IAttributeDictionary::TPtr GetUserAttributeDictionary() = 0;

    // Can be NULL.
    virtual ISystemAttributeProvider::TPtr GetSystemAttributeProvider() = 0;

    virtual TResolveResult ResolveAttributes(
        const NYTree::TYPath& path,
        const Stroka& verb);
    
    virtual void GetAttribute(
        const TYPath& path,
        TReqGet* request,
        TRspGet* response,
        TCtxGet* context);

    virtual void ListAttribute(
        const TYPath& path,
        TReqList* request,
        TRspList* response,
        TCtxList* context);

    virtual void SetAttribute(
        const TYPath& path,
        TReqSet* request,
        TRspSet* response,
        TCtxSet* context);

    virtual void RemoveAttribute(
        const TYPath& path,
        TReqRemove* request,
        TRspRemove* response,
        TCtxRemove* context);
};

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

    virtual void OnMyBeginMap();

    virtual void OnMyBeginAttributes();
    virtual void OnMyAttributesItem(const Stroka& name);
    virtual void OnMyEndAttributes();

protected:
    typedef TNodeSetterBase TThis;

    INode::TPtr Node;
    ITreeBuilder* TreeBuilder;
    INodeFactory::TPtr NodeFactory;

    Stroka AttributeName;
    TYson AttributeValue;
    TAutoPtr<TStringOutput> AttributeStream;
    TAutoPtr<TYsonWriter> AttributeWriter;

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
        TreeBuilder->BeginTree();
        ForwardNode(TreeBuilder, ~FromMethod(&TThis::OnForwardingFinished, this));
    }

    void OnForwardingFinished()
    {
        YVERIFY(Map->AddChild(~TreeBuilder->EndTree(), ItemName));
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
        TreeBuilder->BeginTree();
        ForwardNode(TreeBuilder, ~FromMethod(&TThis::OnForwardingFinished, this));
    }

    void OnForwardingFinished()
    {
        List->AddChild(~TreeBuilder->EndTree());
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
    YASSERT(node);
    YASSERT(producer);
    YASSERT(builder);

    TNodeSetter<TNode> setter(node, builder);
    producer->Do(&setter);
}

////////////////////////////////////////////////////////////////////////////////

typedef IParamAction<NBus::IMessage::TPtr> TYPathResponseHandler;

NRpc::IServiceContext::TPtr CreateYPathContext(
    NBus::IMessage* requestMessage,
    const TYPath& path,
    const Stroka& verb,
    const Stroka& loggingCategory,
    TYPathResponseHandler* responseHandler);

////////////////////////////////////////////////////////////////////////////////

#define DISPATCH_YPATH_SERVICE_METHOD(method) \
    if (context->GetVerb() == #method) { \
        method##Thunk(context); \
        return; \
    }

#define DECLARE_YPATH_SERVICE_WRITE_METHOD(method) \
    if (context->GetVerb() == #method) { \
        return true; \
    }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

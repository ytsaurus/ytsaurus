#pragma once

#include "common.h"
#include "ypath_service.h"
#include "yson_consumer.h"
#include "tree_builder.h"
#include "forwarding_yson_consumer.h"

#include <ytlib/actions/action_util.h>
#include <ytlib/misc/assert.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

extern TYPath RootMarker;

////////////////////////////////////////////////////////////////////////////////

class TYPathServiceBase
    : public virtual IYPathService
{
public:
    virtual void Invoke(NRpc::IServiceContext* context);
    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb);

protected:
    virtual void DoInvoke(NRpc::IServiceContext* context);
    virtual TResolveResult ResolveSelf(const TYPath& path, const Stroka& verb);
    virtual TResolveResult ResolveAttributes(const TYPath& path, const Stroka& verb);
    virtual TResolveResult ResolveRecursive(const TYPath& path, const Stroka& verb);

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

void ChopYPathToken(
    const TYPath& path,
    Stroka* prefix,
    TYPath* suffixPath);

TYPath ComputeResolvedYPath(
    const TYPath& wholePath,
    const TYPath& unresolvedPath);

TYPath CombineYPaths(
    const TYPath& path1,
    const TYPath& path2);

TYPath CombineYPaths(
    const TYPath& path1,
    const TYPath& path2,
    const TYPath& path3);

bool IsEmptyYPath(const TYPath& path);

bool IsFinalYPath(const TYPath& path);

bool IsAttributeYPath(const TYPath& path);

// TODO: choose a better name
bool IsLocalYPath(const TYPath& path);

TYPath ChopYPathAttributeMarker(const TYPath& path);

////////////////////////////////////////////////////////////////////////////////

void ResolveYPath(
    IYPathService* rootService,
    const TYPath& path,
    const Stroka& verb,
    IYPathService::TPtr* suffixService,
    TYPath* suffixPath);

////////////////////////////////////////////////////////////////////////////////

typedef IParamAction<NBus::IMessage::TPtr> TYPathResponseHandler;

void WrapYPathRequest(
    NRpc::TClientRequest* outerRequest,
    NBus::IMessage* innerRequestMessage);

NBus::IMessage::TPtr UnwrapYPathRequest(
    NRpc::IServiceContext* outerContext);
    
NRpc::IServiceContext::TPtr CreateYPathContext(
    NBus::IMessage* requestMessage,
    const TYPath& path,
    const Stroka& verb,
    const Stroka& loggingCategory,
    TYPathResponseHandler* responseHandler);

void WrapYPathResponse(
    NRpc::IServiceContext* outerContext,
    NBus::IMessage* responseMessage);

NBus::IMessage::TPtr UnwrapYPathResponse(
    NRpc::TClientResponse* outerResponse);

void ReplyYPathWithMessage(
    NRpc::IServiceContext* context,
    NBus::IMessage* responseMessage);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

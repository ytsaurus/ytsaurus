#pragma once

#include "ypath_service.h"
#include "yson_consumer.h"
#include "tree_builder.h"
#include "yson_writer.h"
#include "forwarding_yson_consumer.h"
#include <ytlib/ytree/ypath.pb.h>
#include "attributes.h"

#include <ytlib/misc/assert.h>
#include <ytlib/logging/log.h>
#include <ytlib/rpc/service.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYPathServiceBase
    : public virtual IYPathService
{
public:
    typedef TIntrusivePtr<TYPathServiceBase> TPtr;

    virtual void Invoke(NRpc::IServiceContextPtr context);
    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb);
    virtual Stroka GetLoggingCategory() const;
    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const;

    void GuardedInvoke(NRpc::IServiceContextPtr context);

protected:
    NLog::TLogger Logger;

    virtual void DoInvoke(NRpc::IServiceContextPtr context);
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
    class TCombinedAttributeDictionary;
    THolder<IAttributeDictionary> CombinedAttributes_;

    //! Represents the combined attributes collection containing both
    //! user and system attributes (see #GetUserAttributes and #GetSystemAttributeProvider).
    IAttributeDictionary& CombinedAttributes();

    //! Can be NULL.
    virtual IAttributeDictionary* GetUserAttributes();

    //! Can be NULL.
    virtual ISystemAttributeProvider* GetSystemAttributeProvider();

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

    // This method is called before the attribute with the corresponding key
    // is updated (added, removed or changed).
    virtual void OnUpdateAttribute(
        const Stroka& key,
        const TNullable<NYTree::TYson>& oldValue,
        const TNullable<NYTree::TYson>& newValue);
};

////////////////////////////////////////////////////////////////////////////////

class TNodeSetterBase
    : public TForwardingYsonConsumer
{
protected:
    TNodeSetterBase(INode* node, ITreeBuilder* builder);

    void ThrowInvalidType(ENodeType actualType);
    virtual ENodeType GetExpectedType() = 0;

    virtual void OnMyStringScalar(const TStringBuf& value);
    virtual void OnMyIntegerScalar(i64 value);
    virtual void OnMyDoubleScalar(double value);
    virtual void OnMyEntity();

    virtual void OnMyBeginList();

    virtual void OnMyBeginMap();

    virtual void OnMyBeginAttributes();
    virtual void OnMyKeyedItem(const TStringBuf& key);
    virtual void OnMyEndAttributes();

protected:
    typedef TNodeSetterBase TThis;

    INodePtr Node;
    ITreeBuilder* TreeBuilder;
    INodeFactoryPtr NodeFactory;

    Stroka AttributeKey;
    TYson AttributeValue;
    TStringOutput AttributeStream;
    TYsonWriter AttributeWriter;

    void OnFinished();

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
        typedef type TType; \
        \
        I##name##NodePtr Node; \
        \
        virtual ENodeType GetExpectedType() \
        { \
            return ENodeType::name; \
        } \
        \
        virtual void On##name##Scalar( \
            NDetail::TScalarTypeTraits<type>::TParamType value) \
        { \
            Node->SetValue(TType(value)); \
        } \
    }

DECLARE_SCALAR_TYPE(String, Stroka);
DECLARE_SCALAR_TYPE(Integer,  i64);
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

    IMapNodePtr Map;
    Stroka ItemKey;

    virtual ENodeType GetExpectedType()
    {
        return ENodeType::Map;
    }

    virtual void OnMyBeginMap()
    {
        Map->Clear();
    }

    virtual void OnMyKeyedItem(const TStringBuf& key)
    {
        ItemKey = key;
        TreeBuilder->BeginTree();
        Forward(TreeBuilder, BIND(&TThis::OnForwardingFinished, this));
    }

    void OnForwardingFinished()
    {
        YVERIFY(Map->AddChild(~TreeBuilder->EndTree(), ItemKey));
        ItemKey.clear();
    }

    virtual void OnMyEndMap()
    {

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

    IListNodePtr List;

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
        Forward(TreeBuilder, BIND(&TThis::OnForwardingFinished, this));
    }

    void OnForwardingFinished()
    {
        List->AddChild(~TreeBuilder->EndTree());
    }

    virtual void OnMyEndList()
    {

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

    virtual void OnMyEntity()
    {

        // Just do nothing.
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TNode>
void SetNodeFromProducer(
    TNode* node,
    TYsonProducer producer,
    ITreeBuilder* builder)
{
    YASSERT(node);
    YASSERT(!producer.IsNull());
    YASSERT(builder);

    TNodeSetter<TNode> setter(node, builder);
    producer.Run(&setter);
}

////////////////////////////////////////////////////////////////////////////////

typedef TCallback<void(NBus::IMessage::TPtr)> TYPathResponseHandler;

NRpc::IServiceContextPtr CreateYPathContext(
    NBus::IMessage* requestMessage,
    const TYPath& path,
    const Stroka& verb,
    const Stroka& loggingCategory,
    TYPathResponseHandler responseHandler);

IYPathServicePtr CreateRootService(IYPathServicePtr underlyingService);

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

#pragma once

#include "ypath_service.h"
#include "yson_producer.h"
#include "tree_builder.h"
#include "forwarding_yson_consumer.h"
#include "attributes.h"
#include "permission.h"

#include <core/misc/assert.h>

#include <core/yson/consumer.h>
#include <core/yson/writer.h>

#include <core/ytree/node.h>
#include <core/ytree/ypath.pb.h>

#include <core/logging/log.h>

#include <core/rpc/service_detail.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_YPATH_SERVICE_METHOD(ns, method) \
    typedef ::NYT::NRpc::TTypedServiceContext<ns::TReq##method, ns::TRsp##method> TCtx##method; \
    typedef ::NYT::TIntrusivePtr<TCtx##method> TCtx##method##Ptr; \
    typedef TCtx##method::TTypedRequest  TReq##method; \
    typedef TCtx##method::TTypedResponse TRsp##method; \
    \
    void method##Thunk( \
        const ::NYT::NRpc::IServiceContextPtr& context, \
        const ::NYT::NRpc::THandlerInvocationOptions& options) \
    { \
        auto typedContext = ::NYT::New<TCtx##method>(context, options); \
        if (!typedContext->DeserializeRequest()) \
            return; \
        this->method( \
            &typedContext->Request(), \
            &typedContext->Response(), \
            typedContext); \
    } \
    \
    void method( \
        TReq##method* request, \
        TRsp##method* response, \
        const TCtx##method##Ptr& context)

#define DEFINE_YPATH_SERVICE_METHOD(type, method) \
    void type::method( \
        TReq##method* request, \
        TRsp##method* response, \
        const TCtx##method##Ptr& context)

////////////////////////////////////////////////////////////////////////////////

#define DISPATCH_YPATH_SERVICE_METHOD(method) \
    if (context->GetMethod() == #method) { \
        ::NYT::NRpc::THandlerInvocationOptions options; \
        method##Thunk(context, options); \
        return true; \
    }

#define DISPATCH_YPATH_HEAVY_SERVICE_METHOD(method) \
    if (context->GetMethod() == #method) { \
        ::NYT::NRpc::THandlerInvocationOptions options; \
        options.HeavyResponse = true; \
        options.ResponseCodec = NCompression::ECodec::Lz4; \
        method##Thunk(context, options); \
        return true; \
    }

////////////////////////////////////////////////////////////////////////////////

class TYPathServiceBase
    : public virtual IYPathService
{
public:
    TYPathServiceBase();

    virtual void Invoke(NRpc::IServiceContextPtr context) override;
    virtual TResolveResult Resolve(const TYPath& path, NRpc::IServiceContextPtr context) override;
    virtual NLog::TLogger GetLogger() const override;
    virtual void SerializeAttributes(
        NYson::IYsonConsumer* consumer,
        const TAttributeFilter& filter,
        bool sortKeys) override;

protected:
    mutable NLog::TLogger Logger;
    mutable bool LoggerCreated;

    virtual void BeforeInvoke(NRpc::IServiceContextPtr context);
    virtual bool DoInvoke(NRpc::IServiceContextPtr context);
    virtual void AfterInvoke(NRpc::IServiceContextPtr context);

    void EnsureLoggerCreated() const;
    virtual bool IsLoggingEnabled() const;
    virtual NLog::TLogger CreateLogger() const;

    virtual TResolveResult ResolveSelf(const TYPath& path, NRpc::IServiceContextPtr context);
    virtual TResolveResult ResolveAttributes(const TYPath& path, NRpc::IServiceContextPtr context);
    virtual TResolveResult ResolveRecursive(const TYPath& path, NRpc::IServiceContextPtr context);

};

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_SUPPORTS_METHOD(method, base) \
    class TSupports##method \
        : public base \
    { \
    protected: \
        DECLARE_YPATH_SERVICE_METHOD(NProto, method); \
        virtual void method##Self(TReq##method* request, TRsp##method* response, TCtx##method##Ptr context); \
        virtual void method##Recursive(const TYPath& path, TReq##method* request, TRsp##method* response, TCtx##method##Ptr context); \
        virtual void method##Attribute(const TYPath& path, TReq##method* request, TRsp##method* response, TCtx##method##Ptr context); \
    }

class TSupportsExistsBase
    : public virtual TRefCounted
{
protected:
    typedef NRpc::TTypedServiceContext<NProto::TReqExists, NProto::TRspExists> TCtxExists;
    typedef TIntrusivePtr<TCtxExists> TCtxExistsPtr;

    void Reply(TCtxExistsPtr context, bool value);

};

DECLARE_SUPPORTS_METHOD(GetKey, virtual TRefCounted);
DECLARE_SUPPORTS_METHOD(Get, virtual TRefCounted);
DECLARE_SUPPORTS_METHOD(Set, virtual TRefCounted);
DECLARE_SUPPORTS_METHOD(List, virtual TRefCounted);
DECLARE_SUPPORTS_METHOD(Remove, virtual TRefCounted);
DECLARE_SUPPORTS_METHOD(Exists, TSupportsExistsBase);

#undef DECLARE_SUPPORTS_VERB

////////////////////////////////////////////////////////////////////////////////

class TSupportsPermissions
{
protected:
    virtual ~TSupportsPermissions();

    virtual void ValidatePermission(
        EPermissionCheckScope scope,
        EPermission permission);

};

////////////////////////////////////////////////////////////////////////////////

class TSupportsAttributes
    : public virtual TYPathServiceBase
    , public virtual TSupportsGet
    , public virtual TSupportsList
    , public virtual TSupportsSet
    , public virtual TSupportsRemove
    , public virtual TSupportsExists
    , public virtual TSupportsPermissions
{
protected:
    //! Can be |nullptr|.
    virtual IAttributeDictionary* GetCustomAttributes();

    //! Can be |nullptr|.
    virtual ISystemAttributeProvider* GetBuiltinAttributeProvider();

    virtual TResolveResult ResolveAttributes(
        const NYPath::TYPath& path,
        NRpc::IServiceContextPtr context) override;

    virtual void GetAttribute(
        const TYPath& path,
        TReqGet* request,
        TRspGet* response,
        TCtxGetPtr context) override;

    virtual void ListAttribute(
        const TYPath& path,
        TReqList* request,
        TRspList* response,
        TCtxListPtr context) override;

    virtual void ExistsAttribute(
        const TYPath& path,
        TReqExists* request,
        TRspExists* response,
        TCtxExistsPtr context) override;

    virtual void SetAttribute(
        const TYPath& path,
        TReqSet* request,
        TRspSet* response,
        TCtxSetPtr context) override;

    virtual void RemoveAttribute(
        const TYPath& path,
        TReqRemove* request,
        TRspRemove* response,
        TCtxRemovePtr context) override;

    //! Called before attribute #key is updated (added, removed or changed).
    virtual void ValidateCustomAttributeUpdate(
        const Stroka& key,
        const TNullable<NYTree::TYsonString>& oldValue,
        const TNullable<NYTree::TYsonString>& newValue);

    //! Same as #ValidateCustomAttributeUpdate but wraps the exceptions.
    void GuardedValidateCustomAttributeUpdate(
        const Stroka& key,
        const TNullable<TYsonString>& oldValue,
        const TNullable<TYsonString>& newValue);

    //! Called after some custom attributes are changed.
    virtual void OnCustomAttributesUpdated();

private:
    TFuture< TErrorOr<TYsonString> > DoFindAttribute(const Stroka& key);

    static TErrorOr<TYsonString> DoGetAttributeFragment(const TYPath& path, TErrorOr<TYsonString> wholeYsonOrError);
    TFuture< TErrorOr<TYsonString> > DoGetAttribute(const TYPath& path);

    static bool DoExistsAttributeFragment(const TYPath& path, TErrorOr<TYsonString> wholeYsonOrError);
    TFuture<bool> DoExistsAttribute(const TYPath& path);

    static TErrorOr<TYsonString> DoListAttributeFragment(const TYPath& path, TErrorOr<TYsonString> wholeYsonOrError);
    TFuture< TErrorOr<TYsonString> > DoListAttribute(const TYPath& path);

    void DoSetAttribute(const TYPath& path, const TYsonString& newYson);

    void DoRemoveAttribute(const TYPath& path);

    void GuardedSetBuiltinAttribute(const Stroka& key, const TYsonString& value);

};

////////////////////////////////////////////////////////////////////////////////

class TNodeSetterBase
    : public TForwardingYsonConsumer
{
public:
    void Commit();

protected:
    TNodeSetterBase(INode* node, ITreeBuilder* builder);
    ~TNodeSetterBase();

    void ThrowInvalidType(ENodeType actualType);
    virtual ENodeType GetExpectedType() = 0;

    virtual void OnMyStringScalar(const TStringBuf& value) override;
    virtual void OnMyInt64Scalar(i64 value) override;
    virtual void OnMyUint64Scalar(ui64 value) override;
    virtual void OnMyDoubleScalar(double value) override;
    virtual void OnMyBooleanScalar(bool value) override;
    virtual void OnMyEntity() override;

    virtual void OnMyBeginList() override;

    virtual void OnMyBeginMap() override;

    virtual void OnMyBeginAttributes() override;
    virtual void OnMyEndAttributes() override;

protected:
    class TAttributesSetter;

    INode* Node;
    ITreeBuilder* TreeBuilder;
    INodeFactoryPtr NodeFactory;
    std::unique_ptr<TAttributesSetter> AttributesSetter;

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
        I##name##NodePtr Node; \
        \
        virtual ENodeType GetExpectedType() override \
        { \
            return ENodeType::name; \
        } \
        \
        virtual void On##name##Scalar(NDetail::TScalarTypeTraits<type>::TConsumerType newWholeYson) override \
        { \
            Node->SetValue(type(newWholeYson)); \
        } \
    }

DECLARE_SCALAR_TYPE(String, Stroka);
DECLARE_SCALAR_TYPE(Int64,  i64);
DECLARE_SCALAR_TYPE(Uint64,  ui64);
DECLARE_SCALAR_TYPE(Double, double);
DECLARE_SCALAR_TYPE(Boolean, bool);

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

    virtual ENodeType GetExpectedType() override
    {
        return ENodeType::Map;
    }

    virtual void OnMyBeginMap() override
    {
        Map->Clear();
    }

    virtual void OnMyKeyedItem(const TStringBuf& key) override
    {
        ItemKey = key;
        TreeBuilder->BeginTree();
        Forward(TreeBuilder, BIND(&TThis::OnForwardingFinished, this));
    }

    void OnForwardingFinished()
    {
        YCHECK(Map->AddChild(TreeBuilder->EndTree(), ItemKey));
        ItemKey.clear();
    }

    virtual void OnMyEndMap() override
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

    virtual ENodeType GetExpectedType() override
    {
        return ENodeType::List;
    }

    virtual void OnMyBeginList() override
    {
        List->Clear();
    }

    virtual void OnMyListItem() override
    {
        TreeBuilder->BeginTree();
        Forward(TreeBuilder, BIND(&TThis::OnForwardingFinished, this));
    }

    void OnForwardingFinished()
    {
        List->AddChild(TreeBuilder->EndTree());
    }

    virtual void OnMyEndList() override
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
    virtual ENodeType GetExpectedType() override
    {
        return ENodeType::Entity;
    }

    virtual void OnMyEntity() override
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
    YCHECK(node);
    YCHECK(builder);

    TNodeSetter<TNode> setter(node, builder);
    producer.Run(&setter);
    setter.Commit();
}

////////////////////////////////////////////////////////////////////////////////

NRpc::IServiceContextPtr CreateYPathContext(
    TSharedRefArray requestMessage,
    NLog::TLogger logger = NLog::TLogger());

NRpc::IServiceContextPtr CreateYPathContext(
    std::unique_ptr<NRpc::NProto::TRequestHeader> requestHeader,
    TSharedRefArray requestMessage,
    NLog::TLogger logger = NLog::TLogger());

IYPathServicePtr CreateRootService(IYPathServicePtr underlyingService);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

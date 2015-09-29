#include "stdafx.h"
#include "virtual.h"
#include "fluent.h"
#include "node_detail.h"
#include "ypath_detail.h"
#include "ypath_client.h"
#include "ephemeral_attribute_owner.h"

#include <core/yson/tokenizer.h>
#include <core/yson/async_writer.h>

#include <core/ypath/tokenizer.h>

namespace NYT {
namespace NYTree {

using namespace NRpc;
using namespace NYson;
using namespace NYTree;
using namespace NYPath;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TVirtualMapBase::TVirtualMapBase(INodePtr owningNode)
    : OwningNode_(owningNode)
{ }

bool TVirtualMapBase::DoInvoke(IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    DISPATCH_YPATH_SERVICE_METHOD(List);
    DISPATCH_YPATH_SERVICE_METHOD(Exists);
    return TSupportsAttributes::DoInvoke(context);
}

IYPathService::TResolveResult TVirtualMapBase::ResolveRecursive(
    const TYPath& path,
    IServiceContextPtr context)
{
    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::Literal);
    auto key = tokenizer.GetLiteralValue();
    auto service = FindItemService(key);
    if (!service) {
        if (context->GetMethod() == "Exists") {
            return TResolveResult::Here(path);
        }
        // TODO(babenko): improve diagnostics
        THROW_ERROR_EXCEPTION("Node has no child with key %Qv",
            ToYPathLiteral(key));
    }

    return TResolveResult::There(service, tokenizer.GetSuffix());
}

void TVirtualMapBase::GetSelf(TReqGet* request, TRspGet* response, TCtxGetPtr context)
{
    YASSERT(!NYson::TTokenizer(GetRequestYPath(context)).ParseNext());

    auto attributeFilter = request->has_attribute_filter()
        ? FromProto<TAttributeFilter>(request->attribute_filter())
        : TAttributeFilter::None;

    i64 limit = request->limit();

    auto keys = GetKeys(limit);
    size_t size = GetSize();

    TAsyncYsonWriter writer;

    writer.OnBeginAttributes();
    if (keys.size() != size) {
        writer.OnKeyedItem("incomplete");
        writer.OnBooleanScalar(true);
    }
    if (OwningNode_) {
        OwningNode_->WriteAttributesFragment(&writer, attributeFilter, false);
    }
    writer.OnEndAttributes();

    writer.OnBeginMap();
    for (const auto& key : keys) {
        auto service = FindItemService(key);
        if (service) {
            writer.OnKeyedItem(key);
            service->WriteAttributes(&writer, attributeFilter, false);
            writer.OnEntity();
        }
    }
    writer.OnEndMap();

    writer.Finish().Subscribe(BIND([=] (const TErrorOr<TYsonString>& resultOrError) {
        if (resultOrError.IsOK()) {
            response->set_value(resultOrError.Value().Data());
            context->Reply();
        } else {
            context->Reply(resultOrError);
        }
    }));
}

void TVirtualMapBase::ListSelf(TReqList* request, TRspList* response, TCtxListPtr context)
{
    auto attributeFilter = request->has_attribute_filter()
        ? FromProto<TAttributeFilter>(request->attribute_filter())
        : TAttributeFilter::None;

    i64 limit = request->limit();

    auto keys = GetKeys(limit);
    size_t size = GetSize();

    TAsyncYsonWriter writer;

    if (keys.size() != size) {
        writer.OnBeginAttributes();
        writer.OnKeyedItem("incomplete");
        writer.OnBooleanScalar(true);
        writer.OnEndAttributes();
    }

    writer.OnBeginList();
    for (const auto& key : keys) {
        auto service = FindItemService(key);
        if (service) {
            writer.OnListItem();
            service->WriteAttributes(&writer, attributeFilter, false);
            writer.OnStringScalar(key);
        }
    }
    writer.OnEndList();

    writer.Finish().Subscribe(BIND([=] (const TErrorOr<TYsonString>& resultOrError) {
        if (resultOrError.IsOK()) {
            response->set_value(resultOrError.Value().Data());
            context->Reply();
        } else {
            context->Reply(resultOrError);
        }
    }));
}

void TVirtualMapBase::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    descriptors->push_back("count");
}

bool TVirtualMapBase::GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer)
{
    if (key == "count") {
        BuildYsonFluently(consumer)
            .Value(GetSize());
        return true;
    }

    return false;
}

TFuture<TYsonString> TVirtualMapBase::GetBuiltinAttributeAsync(const Stroka& /*key*/)
{
    return Null;
}

ISystemAttributeProvider* TVirtualMapBase::GetBuiltinAttributeProvider()
{
    return this;
}

bool TVirtualMapBase::SetBuiltinAttribute(const Stroka& /*key*/, const TYsonString& /*value*/)
{
    return false;
}

TFuture<void> TVirtualMapBase::SetBuiltinAttributeAsync(const Stroka& /*key*/, const TYsonString& /*value*/)
{
    return Null;
}

bool TVirtualMapBase::RemoveBuiltinAttribute(const Stroka& /*key*/)
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualEntityNode
    : public TNodeBase
    , public TSupportsAttributes
    , public IEntityNode
    , public TEphemeralAttributeOwner
{
public:
    YTREE_NODE_TYPE_OVERRIDES(Entity)

public:
    explicit TVirtualEntityNode(IYPathServicePtr underlyingService)
        : UnderlyingService_(underlyingService)
    { }

    virtual INodeFactoryPtr CreateFactory() const override
    {
        YASSERT(Parent_);
        return Parent_->CreateFactory();
    }

    virtual INodeResolverPtr GetResolver() const override
    {
        YASSERT(Parent_);
        return Parent_->GetResolver();
    }

    virtual ICompositeNodePtr GetParent() const override
    {
        return Parent_;
    }

    virtual void SetParent(ICompositeNodePtr parent) override
    {
        Parent_ = parent.Get();
    }

    virtual TResolveResult Resolve(
        const TYPath& path,
        IServiceContextPtr /*context*/) override
    {
        // TODO(babenko): handle ugly face
        return TResolveResult::There(UnderlyingService_, path);
    }

    virtual void WriteAttributesFragment(
        IAsyncYsonConsumer* /*consumer*/,
        const TAttributeFilter& /*filter*/,
        bool /*sortKeys*/) override
    { }

private:
    const IYPathServicePtr UnderlyingService_;

    ICompositeNode* Parent_ = nullptr;

    // TSupportsAttributes members

    virtual IAttributeDictionary* GetCustomAttributes() override
    {
        return MutableAttributes();
    }
};

INodePtr CreateVirtualNode(IYPathServicePtr service)
{
    return New<TVirtualEntityNode>(service);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

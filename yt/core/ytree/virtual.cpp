#include "virtual.h"
#include "ephemeral_attribute_owner.h"
#include "fluent.h"
#include "node_detail.h"
#include "ypath_client.h"
#include "ypath_detail.h"
#include "ypath_service.h"

#include <yt/core/yson/tokenizer.h>
#include <yt/core/yson/async_writer.h>

#include <yt/core/ypath/tokenizer.h>
#include <yt/core/yson/writer.h>

#include <util/generic/hash.h>

namespace NYT {
namespace NYTree {

using namespace NRpc;
using namespace NYson;
using namespace NYTree;
using namespace NYPath;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TVirtualMapBase::TVirtualMapBase(INodePtr owningNode)
    : OwningNode_(std::move(owningNode))
{ }

bool TVirtualMapBase::DoInvoke(const IServiceContextPtr& context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    DISPATCH_YPATH_SERVICE_METHOD(List);
    DISPATCH_YPATH_SERVICE_METHOD(Exists);
    return TSupportsAttributes::DoInvoke(context);
}

IYPathService::TResolveResult TVirtualMapBase::ResolveRecursive(
    const TYPath& path,
    const IServiceContextPtr& context)
{
    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::Literal);
    auto key = tokenizer.GetLiteralValue();
    auto service = FindItemService(key);
    if (!service) {
        if (context->GetMethod() == "Exists") {
            return TResolveResultHere{path};
        }
        // TODO(babenko): improve diagnostics
        THROW_ERROR_EXCEPTION("Node has no child with key %Qv",
            ToYPathLiteral(key));
    }

    return TResolveResultThere{std::move(service), TYPath(tokenizer.GetSuffix())};
}

void TVirtualMapBase::GetSelf(
    TReqGet* request,
    TRspGet* response,
    const TCtxGetPtr& context)
{
    Y_ASSERT(!NYson::TTokenizer(GetRequestYPath(context->RequestHeader())).ParseNext());

    auto attributeKeys = request->has_attributes()
        ? MakeNullable(NYT::FromProto<std::vector<TString>>(request->attributes().keys()))
        : Null;

    i64 limit = request->has_limit()
        ? request->limit()
        : DefaultVirtualChildLimit;

    context->SetRequestInfo("Limit: %v", limit);

    auto keys = GetKeys(limit);
    i64 size = GetSize();

    TAsyncYsonWriter writer;

    // NB: we do not want empty attributes (<>) to appear in the result in order to comply
    // with current behaviour for some paths (like //sys/scheduler/orchid/scheduler/operations).
    if (keys.size() != size || OwningNode_) {
        writer.OnBeginAttributes();
        if (keys.size() != size) {
            writer.OnKeyedItem("incomplete");
            writer.OnBooleanScalar(true);
        }
        if (OwningNode_) {
            OwningNode_->WriteAttributesFragment(&writer, attributeKeys, false);
        }
        writer.OnEndAttributes();
    }

    writer.OnBeginMap();
    for (const auto& key : keys) {
        auto service = FindItemService(key);
        if (service) {
            writer.OnKeyedItem(key);
            service->WriteAttributes(&writer, attributeKeys, false);
            writer.OnEntity();
        }
    }
    writer.OnEndMap();

    writer.Finish().Subscribe(BIND([=] (const TErrorOr<TYsonString>& resultOrError) {
        if (resultOrError.IsOK()) {
            response->set_value(resultOrError.Value().GetData());
            context->Reply();
        } else {
            context->Reply(resultOrError);
        }
    }));
}

void TVirtualMapBase::ListSelf(
    TReqList* request,
    TRspList* response,
    const TCtxListPtr& context)
{
    auto attributeKeys = request->has_attributes()
        ? MakeNullable(FromProto<std::vector<TString>>(request->attributes().keys()))
        : Null;

    i64 limit = request->has_limit()
        ? request->limit()
        : DefaultVirtualChildLimit;

    context->SetRequestInfo("Limit: %v", limit);

    auto keys = GetKeys(limit);
    i64 size = GetSize();

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
            service->WriteAttributes(&writer, attributeKeys, false);
            writer.OnStringScalar(key);
        }
    }
    writer.OnEndList();

    writer.Finish().Subscribe(BIND([=] (const TErrorOr<TYsonString>& resultOrError) {
        if (resultOrError.IsOK()) {
            response->set_value(resultOrError.Value().GetData());
            context->Reply();
        } else {
            context->Reply(resultOrError);
        }
    }));
}

void TVirtualMapBase::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    descriptors->push_back(CountInternedAttribute);
}

const THashSet<TInternedAttributeKey>& TVirtualMapBase::GetBuiltinAttributeKeys()
{
    return BuiltinAttributeKeysCache_.GetBuiltinAttributeKeys(this);
}

bool TVirtualMapBase::GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer)
{
    switch (key) {
        case CountInternedAttribute:
            BuildYsonFluently(consumer)
                .Value(GetSize());
            return true;
        default:
            return false;
    }
}

TFuture<TYsonString> TVirtualMapBase::GetBuiltinAttributeAsync(TInternedAttributeKey /*key*/)
{
    return Null;
}

ISystemAttributeProvider* TVirtualMapBase::GetBuiltinAttributeProvider()
{
    return this;
}

bool TVirtualMapBase::SetBuiltinAttribute(TInternedAttributeKey /*key*/, const TYsonString& /*value*/)
{
    return false;
}

bool TVirtualMapBase::RemoveBuiltinAttribute(TInternedAttributeKey /*key*/)
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

class TCompositeMapService::TImpl
    : public TIntrinsicRefCounted
{
public:
    std::vector<TString> GetKeys(i64 limit) const
    {
        std::vector<TString> keys;
        int index = 0;
        auto it = Services_.begin();
        while (it != Services_.end() && index < limit) {
            keys.push_back(it->first);
            ++it;
            ++index;
        }
        return keys;
    }

    i64 GetSize() const
    {
        return Services_.size();
    }

    IYPathServicePtr FindItemService(const TStringBuf& key) const
    {
        auto it = Services_.find(key);
        return it != Services_.end() ? it->second : nullptr;
    }

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) const
    {
        for (const auto& it : Attributes_) {
            descriptors->push_back(TAttributeDescriptor(it.first));
        }
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) const
    {
        auto it = Attributes_.find(key);
        if (it != Attributes_.end()) {
            it->second.Run(consumer);
            return true;
        }

        return false;
    }

    void AddChild(const TString& key, IYPathServicePtr service)
    {
        YCHECK(Services_.insert(std::make_pair(key, service)).second);
    }

    void AddAttribute(TInternedAttributeKey key, TYsonCallback producer)
    {
        YCHECK(Attributes_.insert(std::make_pair(key, producer)).second);
    }

private:
    THashMap<TString, IYPathServicePtr> Services_;
    THashMap<TInternedAttributeKey, TYsonCallback> Attributes_;

};

////////////////////////////////////////////////////////////////////////////////

TCompositeMapService::TCompositeMapService()
    : Impl_(New<TImpl>())
{ }

std::vector<TString> TCompositeMapService::GetKeys(i64 limit) const
{
    return Impl_->GetKeys(limit);
}

i64 TCompositeMapService::GetSize() const
{
    return Impl_->GetSize();
}

IYPathServicePtr TCompositeMapService::FindItemService(const TStringBuf& key) const
{
   return Impl_->FindItemService(key);
}

void TCompositeMapService::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    Impl_->ListSystemAttributes(descriptors);

    TVirtualMapBase::ListSystemAttributes(descriptors);
}

bool TCompositeMapService::GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer)
{
    if (Impl_->GetBuiltinAttribute(key, consumer)) {
        return true;
    }

    return TVirtualMapBase::GetBuiltinAttribute(key, consumer);
}

TIntrusivePtr<TCompositeMapService> TCompositeMapService::AddChild(const TString& key, IYPathServicePtr service)
{
    Impl_->AddChild(key, std::move(service));
    return this;
}

TIntrusivePtr<TCompositeMapService> TCompositeMapService::AddAttribute(TInternedAttributeKey key, TYsonCallback producer)
{
    Impl_->AddAttribute(key, producer);
    return this;
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
        : UnderlyingService_(std::move(underlyingService))
    { }

    virtual std::unique_ptr<ITransactionalNodeFactory> CreateFactory() const override
    {
        Y_ASSERT(Parent_);
        return Parent_->CreateFactory();
    }

    virtual ICompositeNodePtr GetParent() const override
    {
        return Parent_;
    }

    virtual void SetParent(const ICompositeNodePtr& parent) override
    {
        Parent_ = parent.Get();
    }

    virtual TResolveResult Resolve(
        const TYPath& path,
        const IServiceContextPtr& /*context*/) override
    {
        // TODO(babenko): handle ugly face
        return TResolveResultThere{UnderlyingService_, path};
    }

    virtual void DoWriteAttributesFragment(
        IAsyncYsonConsumer* /*consumer*/,
        const TNullable<std::vector<TString>>& /*attributeKeys*/,
        bool /*stable*/) override
    { }

    virtual bool ShouldHideAttributes() override
    {
        return UnderlyingService_->ShouldHideAttributes();
    }

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

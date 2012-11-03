#include "stdafx.h"
#include "virtual.h"
#include "fluent.h"
#include "node_detail.h"
#include "yson_writer.h"
#include "ypath_detail.h"
#include "ypath_client.h"
#include "ephemeral_attribute_owner.h"
#include "tokenizer.h"

#include <ytlib/ypath/tokenizer.h>

namespace NYT {
namespace NYTree {

using namespace NRpc;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

static const size_t DefaultMaxSize = 1000;

////////////////////////////////////////////////////////////////////////////////

void TVirtualMapBase::DoInvoke(IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    DISPATCH_YPATH_SERVICE_METHOD(List);
    TSupportsAttributes::DoInvoke(context);
}

IYPathService::TResolveResult TVirtualMapBase::ResolveRecursive(
    const TYPath& path,
    IServiceContextPtr context)
{
    UNUSED(context);

    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::Literal);
    auto key = tokenizer.GetLiteralValue();
    auto service = GetItemService(key);
    if (!service) {
        // TODO(babenko): improve diagnostics
        THROW_ERROR_EXCEPTION("Node has no child with key: %s",
            ~ToYPathLiteral(key));
    }

    return TResolveResult::There(service, tokenizer.GetSuffix());
}

void TVirtualMapBase::GetSelf(TReqGet* request, TRspGet* response, TCtxGetPtr context)
{
    YASSERT(!TTokenizer(context->GetPath()).ParseNext());

    size_t max_size = request->Attributes().Get<int>("max_size", DefaultMaxSize);

    auto keys = GetKeys(max_size);
    size_t size = GetSize();

    TStringStream stream;
    TYsonWriter writer(&stream, EYsonFormat::Binary);
    // TODO(MRoizner): use fluent
    BuildYsonFluently(&writer);

    if (keys.size() != size) {
        writer.OnBeginAttributes();
        writer.OnKeyedItem("incomplete");
        writer.OnStringScalar("true");
        writer.OnEndAttributes();
    }

    writer.OnBeginMap();
    FOREACH (const auto& key, keys) {
        writer.OnKeyedItem(key);
        writer.OnEntity();
    }
    writer.OnEndMap();

    response->set_value(stream.Str());
    context->Reply();
}

void TVirtualMapBase::ListSelf(TReqList* request, TRspList* response, TCtxListPtr context)
{
    size_t maxSize = request->Attributes().Get<int>("max_size", DefaultMaxSize);

    auto keys = GetKeys(maxSize);
    size_t size = GetSize();

    TStringStream stream;
    TYsonWriter writer(&stream, EYsonFormat::Binary);
    BuildYsonFluently(&writer);

    if (keys.size() != size) {
        writer.OnBeginAttributes();
        writer.OnKeyedItem("incomplete");
        writer.OnStringScalar("true");
        writer.OnEndAttributes();
    }

    writer.OnBeginList();
    FOREACH (const auto& key, keys) {
        writer.OnListItem();
        writer.OnStringScalar(key);
    }
    writer.OnEndList();

    response->set_keys(stream.Str());
    context->Reply();
}

void TVirtualMapBase::ListSystemAttributes(std::vector<TAttributeInfo>* attributes) const
{
    attributes->push_back("count");
}

bool TVirtualMapBase::GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer) const
{
    if (key == "count") {
        BuildYsonFluently(consumer)
            .Scalar(static_cast<i64>(GetSize()));
        return true;
    }

    return false;
}

TAsyncError TVirtualMapBase::GetSystemAttributeAsync(const Stroka& key, IYsonConsumer* consumer) const
{
    UNUSED(key);
    UNUSED(consumer);
    return Null;
}

ISystemAttributeProvider* TVirtualMapBase::GetSystemAttributeProvider()
{
    return this;
}

bool TVirtualMapBase::SetSystemAttribute(const Stroka& key, const TYsonString& value)
{
    UNUSED(key);
    UNUSED(value);
    return false;
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualEntityNode
    : public TNodeBase
    , public TSupportsAttributes
    , public IEntityNode
    , public TEphemeralAttributeOwner
{
    YTREE_NODE_TYPE_OVERRIDES(Entity)

public:
    explicit TVirtualEntityNode(IYPathServicePtr underlyingService)
        : UnderlyingService(underlyingService)
    { }

    virtual INodeFactoryPtr CreateFactory() const override
    {
        YASSERT(Parent);
        return Parent->CreateFactory();
    }

    virtual IYPathResolverPtr GetResolver() const override
    {
        YASSERT(Parent);
        return Parent->GetResolver();
    }

    virtual ICompositeNodePtr GetParent() const override
    {
        return Parent;
    }

    virtual void SetParent(ICompositeNodePtr parent) override
    {
        Parent = ~parent;
    }

    virtual TResolveResult Resolve(
        const TYPath& path,
        IServiceContextPtr context) override
    {
        UNUSED(context);

        // TODO(babenko): handle ugly face
        return TResolveResult::There(UnderlyingService, path);
    }

    virtual void SerializeAttributes(IYsonConsumer* consumer, const TAttributeFilter& filter) const
    {
        UNUSED(consumer);
        UNUSED(filter);
    }

private:
    IYPathServicePtr UnderlyingService;
    ICompositeNode* Parent;
    
    // TSupportsAttributes members

    virtual IAttributeDictionary* GetUserAttributes() override
    {
        return &Attributes();
    }
};

INodePtr CreateVirtualNode(IYPathServicePtr service)
{
    return New<TVirtualEntityNode>(service);
}

NYT::NYTree::INodePtr CreateVirtualNode(TYPathServiceProducer producer)
{
    return CreateVirtualNode(IYPathService::FromProducer(producer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

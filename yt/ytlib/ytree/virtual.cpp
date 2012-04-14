#include "stdafx.h"
#include "virtual.h"
#include "fluent.h"
#include "node_detail.h"
#include "yson_writer.h"
#include "ypath_detail.h"
#include "ypath_client.h"
#include "attribute_provider_detail.h"
#include "lexer.h"

#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NYTree {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

const int DefaultMaxSize = 1000;

void TVirtualMapBase::DoInvoke(IServiceContext* context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    DISPATCH_YPATH_SERVICE_METHOD(List);
    TSupportsAttributes::DoInvoke(context);
}

IYPathService::TResolveResult TVirtualMapBase::ResolveRecursive(const TYPath& path, const Stroka& verb)
{
    UNUSED(verb);

    TYPath suffixPath;
    auto token = ChopStringToken(path, &suffixPath);

    auto service = GetItemService(token);
    if (!service) {
        ythrow yexception() << Sprintf("Key %s is not found", ~token.Quote());
    }

    return TResolveResult::There(service, suffixPath);
}

void TVirtualMapBase::GetSelf(TReqGet* request, TRspGet* response, TCtxGet* context)
{
    YASSERT(IsEmpty(context->GetPath()));

    int max_size = request->Attributes().Get<int>("max_size", DefaultMaxSize);

    TStringStream stream;
    TYsonWriter writer(&stream, EYsonFormat::Binary);
    auto keys = GetKeys(max_size);
    auto size = GetSize();

    // TODO(MRoizner): use fluent
    BuildYsonFluently(&writer);
    writer.OnBeginMap();
    FOREACH (const auto& key, keys) {
        writer.OnMapItem(key);
        writer.OnEntity(false);
    }

    bool incomplete = keys.ysize() != size;
    writer.OnEndMap(incomplete);
    if (incomplete) {
        writer.OnBeginAttributes();
        writer.OnAttributesItem("incomplete");
        writer.OnStringScalar("true");
        writer.OnEndAttributes();
    }

    response->set_value(stream.Str());
    context->Reply();
}

void TVirtualMapBase::ListSelf(TReqList* request, TRspList* response, TCtxList* context)
{
    UNUSED(request);
    YASSERT(IsEmpty(context->GetPath()));

    auto keys = GetKeys();
    NYT::ToProto(response->mutable_keys(), keys);
    context->Reply();
}

void TVirtualMapBase::GetSystemAttributes(std::vector<TAttributeInfo>* attributes)
{
    attributes->push_back("count");
}

bool TVirtualMapBase::GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer)
{
    if (key == "count") {
        BuildYsonFluently(consumer)
            .Scalar(static_cast<i64>(GetSize()));
        return true;
    }

    return false;
}

bool TVirtualMapBase::SetSystemAttribute(const Stroka& key, TYsonProducer producer)
{
    UNUSED(key);
    UNUSED(producer);
    return false;
}

ISystemAttributeProvider* TVirtualMapBase::GetSystemAttributeProvider()
{
    return this;
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualEntityNode
    : public TNodeBase
    , public TSupportsAttributes
    , public IEntityNode
    , public TEphemeralAttributeProvider
{
    YTREE_NODE_TYPE_OVERRIDES(Entity)

public:
    TVirtualEntityNode(IYPathServicePtr underlyingService)
        : UnderlyingService(underlyingService)
    { }

    virtual INodeFactoryPtr CreateFactory() const
    {
        YASSERT(Parent);
        return Parent->CreateFactory();
    }

    virtual ICompositeNodePtr GetParent() const
    {
        return Parent;
    }

    virtual void SetParent(ICompositeNode* parent)
    {
        Parent = parent;
    }

    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb)
    {
        // TODO(babenko): handle ugly face
        return TResolveResult::There(UnderlyingService, path);
    }

private:
    IYPathServicePtr UnderlyingService;
    ICompositeNode* Parent;
    
    // TSupportsAttributes members

    virtual IAttributeDictionary* GetUserAttributes()
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

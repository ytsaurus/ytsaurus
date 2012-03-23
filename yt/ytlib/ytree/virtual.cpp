#include "stdafx.h"
#include "virtual.h"
#include "fluent.h"
#include "node_detail.h"
#include "yson_writer.h"
#include "ypath_detail.h"
#include "ypath_client.h"
#include "attribute_provider_detail.h"

#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NYTree {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TAttributedYPathServiceBase::ResolveAttributes(
    const TYPath& path,
    const Stroka& verb)
{
    UNUSED(path);
    UNUSED(verb);
    return TResolveResult::Here(AttributeMarker + path);
}

void TAttributedYPathServiceBase::DoInvoke(NRpc::IServiceContext* context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    DISPATCH_YPATH_SERVICE_METHOD(List);
    TYPathServiceBase::DoInvoke(context);
}

void TAttributedYPathServiceBase::GetSystemAttributes(std::vector<TAttributeInfo>* attributes)
{
    UNUSED(attributes);
}

bool TAttributedYPathServiceBase::GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer)
{
    return false;
}

bool TAttributedYPathServiceBase::SetSystemAttribute(const Stroka& key, TYsonProducer producer)
{
    return false;
}

void TAttributedYPathServiceBase::GetAttribute(const NYTree::TYPath& path, TReqGet* request, TRspGet* response, TCtxGet* context)
{
    TStringStream stream;
    TYsonWriter writer(&stream, EYsonFormat::Binary);
        
    if (IsFinalYPath(path)) {
        std::vector<TAttributeInfo> systemAttributes;
        GetSystemAttributes(&systemAttributes);

        writer.OnBeginMap();
        FOREACH (const auto& attribute, systemAttributes) {
            if (attribute.IsPresent) {
                writer.OnMapItem(attribute.Key);
                if (attribute.IsOpaque) {
                    writer.OnEntity();
                } else {
                    YVERIFY(GetSystemAttribute(attribute.Key, &writer));
                }
            }
        }
        writer.OnEndMap();

        response->set_value(stream.Str());
    } else {
        Stroka token;
        TYPath suffixPath;
        ChopYPathToken(path, &token, &suffixPath);

        if (!GetSystemAttribute(token, &writer)) {
            ythrow yexception() << Sprintf("Attribute %s is not found", ~token.Quote());
        }
        
        if (IsFinalYPath(suffixPath)) {
            response->set_value(stream.Str());
        } else {
            auto wholeValue = DeserializeFromYson(stream.Str());
            auto value = SyncYPathGet(~wholeValue, suffixPath);
            response->set_value(value);
        }
    }

    context->Reply();
}

void TAttributedYPathServiceBase::ListAttribute(const NYTree::TYPath& path, TReqList* request, TRspList* response, TCtxList* context)
{
    yvector<Stroka> keys;

    if (IsFinalYPath(path)) {
        std::vector<TAttributeInfo> systemAttributes;
        GetSystemAttributes(&systemAttributes);
        FOREACH (const auto& attribute, systemAttributes) {
            if (attribute.IsPresent) {
                keys.push_back(attribute.Key);
            }
        }
    } else {
        Stroka token;
        TYPath suffixPath;
        ChopYPathToken(path, &token, &suffixPath);

        TStringStream stream;
        TYsonWriter writer(&stream, EYsonFormat::Binary);
        if (!GetSystemAttribute(token, &writer)) {
            ythrow yexception() << Sprintf("Attribute %s is not found", ~token.Quote());
        }

        auto wholeValue = DeserializeFromYson(stream.Str());
        keys = SyncYPathList(~wholeValue, suffixPath);
    }

    NYT::ToProto(response->mutable_keys(), keys);
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TVirtualMapBase::ResolveRecursive(const TYPath& path, const Stroka& verb)
{
    UNUSED(verb);

    Stroka token;
    TYPath suffixPath;
    ChopYPathToken(path, &token, &suffixPath);

    auto service = GetItemService(token);
    if (!service) {
        ythrow yexception() << Sprintf("Key %s is not found", ~token.Quote());
    }

    return TResolveResult::There(~service, suffixPath);
}

void TVirtualMapBase::GetSelf(TReqGet* request, TRspGet* response, TCtxGet* context)
{
    YASSERT(IsFinalYPath(context->GetPath()));

    int max_size = request->Attributes().Get<int>("max_size", Max<int>());

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
    YASSERT(IsFinalYPath(context->GetPath()));

    auto keys = GetKeys();
    NYT::ToProto(response->mutable_keys(), keys);
    context->Reply();
}

void TVirtualMapBase::GetSystemAttributes(std::vector<TAttributeInfo>* attributes)
{
    attributes->push_back("size");
    TAttributedYPathServiceBase::GetSystemAttributes(attributes);
}

bool TVirtualMapBase::GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer)
{
    if (key == "count") {
        BuildYsonFluently(consumer)
            .Scalar(static_cast<i64>(GetSize()));
        return true;
    }

    return TAttributedYPathServiceBase::GetSystemAttribute(key, consumer);
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
    TVirtualEntityNode(IYPathService* underlyingService)
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
        if (IsLocalYPath(path)) {
            return TNodeBase::Resolve(path, verb);
        } else {
            return TResolveResult::There(~UnderlyingService, path);
        }
    }

private:
    IYPathServicePtr UnderlyingService;
    ICompositeNode* Parent;
    
    // TSupportsAttributes members

    virtual IAttributeDictionary* GetUserAttributes()
    {
        return &Attributes();
    }

    virtual ISystemAttributeProvider* GetSystemAttributeProvider() 
    {
        return NULL;
    }
};

INodePtr CreateVirtualNode(IYPathService* service)
{
    return New<TVirtualEntityNode>(service);
}

NYT::NYTree::INodePtr CreateVirtualNode(TYPathServiceProducer producer)
{
    return CreateVirtualNode(~IYPathService::FromProducer(producer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

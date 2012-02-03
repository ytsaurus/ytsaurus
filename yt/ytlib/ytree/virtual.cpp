#include "stdafx.h"
#include "virtual.h"
#include "fluent.h"
#include "node_detail.h"
#include "yson_writer.h"
#include "ypath_detail.h"
#include "ypath_client.h"

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

bool TAttributedYPathServiceBase::GetSystemAttribute(const Stroka& name, IYsonConsumer* consumer)
{
    return false;
}

bool TAttributedYPathServiceBase::SetSystemAttribute(const Stroka& name, TYsonProducer* producer)
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
                writer.OnMapItem(attribute.Name);
                if (attribute.IsOpaque) {
                    writer.OnEntity();
                } else {
                    YVERIFY(GetSystemAttribute(attribute.Name, &writer));
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
            auto value = SyncYPathGet(~wholeValue, RootMarker + suffixPath);
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
                keys.push_back(attribute.Name);
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
        keys = SyncYPathList(~wholeValue, RootMarker + suffixPath);
    }

    ToProto(*response->mutable_keys(), keys);
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

struct TGetConfig
    : public TConfigurable
{
    int MaxSize;

    TGetConfig()
    {
        Register("max_size", MaxSize)
            .GreaterThanOrEqual(0)
            .Default(100);
    }
};

void TVirtualMapBase::GetSelf(TReqGet* request, TRspGet* response, TCtxGet* context)
{
    YASSERT(IsFinalYPath(context->GetPath()));

    auto config = New<TGetConfig>();
    if (request->has_options()) {
        auto options = DeserializeFromYson(request->options());
        config->Load(~options);
    }
    config->Validate();
    
    TStringStream stream;
    TYsonWriter writer(&stream, EYsonFormat::Binary);
    auto keys = GetKeys(config->MaxSize);
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
    ToProto(*response->mutable_keys(), keys);
    context->Reply();
}

void TVirtualMapBase::GetSystemAttributes(std::vector<TAttributeInfo>* attributes)
{
    attributes->push_back("size");
    TAttributedYPathServiceBase::GetSystemAttributes(attributes);
}

bool TVirtualMapBase::GetSystemAttribute(const Stroka& name, IYsonConsumer* consumer)
{
    if (name == "size") {
        BuildYsonFluently(consumer)
            .Scalar(static_cast<i64>(GetSize()));
        return true;
    }

    return TAttributedYPathServiceBase::GetSystemAttribute(name, consumer);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualEntityNode
    : public TNodeBase
    , public TSupportsAttributes
    , public IEntityNode
{
    YTREE_NODE_TYPE_OVERRIDES(Entity)

public:
    TVirtualEntityNode(TYPathServiceProvider* builder)
        : Provider(builder)
    { }

    virtual INodeFactory::TPtr CreateFactory() const
    {
        YASSERT(Parent);
        return Parent->CreateFactory();
    }

    virtual ICompositeNode::TPtr GetParent() const
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
            auto service = Provider->Do();
            return TResolveResult::There(~service, path);
        }
    }

    virtual IAttributeDictionary::TPtr GetAttributes()
    {
        return GetUserAttributeDictionary();
    }

protected:
    // TSupportsAttributes members

    virtual IAttributeDictionary::TPtr GetUserAttributeDictionary()
    {
        if (!Attributes) {
            Attributes = CreateInMemoryAttributeDictionary();
        }
        return Attributes;
    }

    virtual ISystemAttributeProvider::TPtr GetSystemAttributeProvider() 
    {
        return NULL;
    }

private:
    TYPathServiceProvider::TPtr Provider;
    ICompositeNode* Parent;
    IAttributeDictionary::TPtr Attributes;
};

INode::TPtr CreateVirtualNode(TYPathServiceProvider* provider)
{
    return New<TVirtualEntityNode>(provider);
}

INode::TPtr CreateVirtualNode(IYPathService* service)
{
    IYPathService::TPtr service_ = service;
    return CreateVirtualNode(~FromFunctor([=] () -> NYTree::IYPathService::TPtr
        {
            return service_;
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

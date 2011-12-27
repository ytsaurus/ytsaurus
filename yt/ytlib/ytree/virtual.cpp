#include "stdafx.h"
#include "virtual.h"
#include "fluent.h"
#include "node_detail.h"
#include "yson_writer.h"
#include "ypath_detail.h"
#include "ypath_client.h"

#include "../misc/configurable.h"

namespace NYT {
namespace NYTree {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TVirtualMapBase::ResolveRecursive(const TYPath& path, const Stroka& verb)
{
    UNUSED(verb);

    Stroka prefix;
    TYPath suffixPath;
    ChopYPathToken(path, &prefix, &suffixPath);

    auto service = GetItemService(prefix);
    if (!service) {
        ythrow yexception() << Sprintf("Key %s is not found", ~prefix.Quote());
    }

    return TResolveResult::There(~service, suffixPath);
}

void TVirtualMapBase::DoInvoke(NRpc::IServiceContext* context)
{
    Stroka verb = context->GetVerb();
    if (verb == "Get") {
        GetThunk(context);
    } else {
        TYPathServiceBase::DoInvoke(context);
    }
}

struct TGetConfig
    : public TConfigurable
{
    int MaxSize;

    TGetConfig()
    {
        Register("max_size", MaxSize).GreaterThanOrEqual(0).Default(100);
    }
};

DEFINE_RPC_SERVICE_METHOD(TVirtualMapBase, Get)
{
    if (!IsFinalYPath(context->GetPath())) {
        ythrow yexception() << "Resolution error: path must be final";
    }

    auto config = New<TGetConfig>();
    if (request->has_options()) {
        auto options = DeserializeFromYson(request->options());
        config->Load(~options);
    }
    config->Validate();
    
    TStringStream stream;
    TYsonWriter writer(&stream, TYsonWriter::EFormat::Binary);
    auto keys = GetKeys(config->MaxSize);
    auto size = GetSize();

    // TODO(MRoizner): use fluent
    writer.OnBeginMap();
    FOREACH (const auto& key, keys) {
        writer.OnMapItem(key);
        auto service = GetItemService(key);
        YASSERT(service);
        writer.OnRaw(SyncYPathGet(~service, NYTree::YPathRoot));
    }
    writer.OnEndMap(true);
    writer.OnBeginAttributes();
    writer.OnAttributesItem("size");
    writer.OnInt64Scalar(size);
    if (keys.ysize() != size) {
        writer.OnAttributesItem("incomplete");
        writer.OnStringScalar("True");
    }
    writer.OnEndAttributes();

    response->set_value(stream.Str());
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualEntityNode
    : public TNodeBase
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

    virtual IMapNode::TPtr GetAttributes() const
    {
        return Attributes;
    }

    virtual void SetAttributes(IMapNode* attributes)
    {
        Attributes = attributes;
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

private:
    TYPathServiceProvider::TPtr Provider;

    ICompositeNode* Parent;
    IMapNode::TPtr Attributes;

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

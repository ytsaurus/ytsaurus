#include "stdafx.h"
#include "virtual.h"
#include "fluent.h"
#include "node_detail.h"
#include "yson_writer.h"
#include "ypath_detail.h"
#include "ypath_client.h"

namespace NYT {
namespace NYTree {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TVirtualMapBase::Resolve(TYPath path, const Stroka& verb)
{
    UNUSED(verb);

    if (IsFinalYPath(path)) {
        return TResolveResult::Here(path);
    } else if (IsAttributeYPath(path)) {
        ythrow yexception() << "Virtual map has no attributes";
    } else {
        Stroka prefix;
        TYPath suffixPath;
        ChopYPathToken(path, &prefix, &suffixPath);

        auto service = GetItemService(prefix);
        if (~service == NULL) {
            ythrow yexception() << Sprintf("Key %s is not found", ~prefix.Quote());
        }

        return TResolveResult::There(~service, suffixPath);
    }
}

void TVirtualMapBase::Invoke(NRpc::IServiceContext* context)
{
    try {
        Stroka verb = context->GetVerb();
        if (verb == "Get") {
            GetThunk(context);
        } else {
            ythrow TTypedServiceException<EYPathErrorCode>(EYPathErrorCode::NoSuchVerb) <<
                "Verb is not supported";
        }
    } catch (...) {
        context->Reply(TError(
            EYPathErrorCode::GenericError,
            CurrentExceptionMessage()));
    }
}

RPC_SERVICE_METHOD_IMPL(TVirtualMapBase, Get)
{
    UNUSED(request);

    if (!IsFinalYPath(context->GetPath())) {
        ythrow yexception() << "Resolution error: path must be final";
    }

    TStringStream stream;
    TYsonWriter writer(&stream, TYsonWriter::EFormat::Binary);
    auto keys = GetKeys();
    writer.OnBeginMap();
    FOREACH (const auto& key, keys) {
        writer.OnMapItem(key);
        auto service = GetItemService(key);
        YASSERT(~service != NULL);
        writer.OnRaw(SyncExecuteYPathGet(~service, "/"));
    }
    writer.OnEndMap(false);

    response->SetValue(stream.Str());
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualEntityNode
    : public TNodeBase
    , public IEntityNode
{
    YTREE_NODE_TYPE_OVERRIDES(Entity)

public:
    TVirtualEntityNode(
        TYPathServiceProducer* builder,
        INodeFactory* factory)
        : Producer(builder)
        , Factory(factory)
    { }

    virtual INodeFactory* GetFactory() const
    {
        return Factory;
    }

    virtual ICompositeNode::TPtr GetParent() const
    {
        return Parent;
    }

    virtual void SetParent(ICompositeNode::TPtr parent)
    {
        Parent = ~parent;
    }

    virtual IMapNode::TPtr GetAttributes() const
    {
        return Attributes;
    }

    virtual void SetAttributes(IMapNode::TPtr attributes)
    {
        Attributes = attributes;
    }

    virtual TResolveResult Resolve(TYPath path, const Stroka& verb)
    {
        if (IsLocalYPath(path)) {
            return TNodeBase::Resolve(path, verb);
        } else {
            auto service = Producer->Do();
            return TResolveResult::There(~service, path);
        }
    }

private:
    TYPathServiceProducer::TPtr Producer;
    INodeFactory* Factory;

    ICompositeNode* Parent;
    IMapNode::TPtr Attributes;

};

INode::TPtr CreateVirtualNode(
    TYPathServiceProducer* producer,
    INodeFactory* factory)
{
    return New<TVirtualEntityNode>(producer, factory);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

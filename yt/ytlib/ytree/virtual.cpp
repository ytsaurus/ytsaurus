#include "stdafx.h"
#include "virtual.h"
#include "fluent.h"
#include "node_detail.h"

namespace NYT {
namespace NYTree {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TVirtualMapBase::Resolve(TYPath path, const Stroka& verb)
{
    UNUSED(path);
    UNUSED(verb);
    ythrow yexception() << "Resolution is not supported";
}

void TVirtualMapBase::Invoke(NRpc::IServiceContext* context)
{
    UNUSED(context);
}

//IYPathService::TGetResult TVirtualMapBase::Get(TYPath path, IYsonConsumer* consumer)
//{
//    // TODO: attributes?
//
//    if (path.Empty()) {
//        auto keys = GetKeys();
//        // TODO: refactor using fluent API
//        consumer->OnBeginMap();
//        FOREACH (const auto& key, keys) {
//            consumer->OnMapItem(key);
//            auto service = GetItemService(key);
//            YASSERT(~service != NULL);
//            // TODO: use constant for /
//            GetYPath(service, "/", consumer);
//        }
//        consumer->OnEndMap(false);
//    } else {
//        Stroka prefix;
//        TYPath suffixPath;
//        ChopYPathPrefix(path, &prefix, &suffixPath);
//
//        auto service = GetItemService(prefix);
//        if (~service == NULL) {
//            ythrow TYTreeException() << Sprintf("Key %s is not found",
//                ~prefix.Quote());
//        }
//
//        return TGetResult::CreateRecurse(service, suffixPath);
//    }
//    return TGetResult::CreateDone();
//}

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

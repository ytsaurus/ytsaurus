#include "node_detail.h"
#include "ypath.h"
#include "tree_visitor.h"
#include "tree_builder.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

IYPathService::TGetResult TNodeBase::Get(
    TYPath path,
    IYsonConsumer* consumer)
{
    if (path.empty()) {
        return GetSelf(consumer);
    }

    if (path[0] == '@') {
        auto attributes = GetAttributes();

        if (path == "@") {
            consumer->OnBeginMap();
            auto names = GetVirtualAttributeNames();
            FOREACH (const auto& name, names) {
                consumer->OnMapItem(name);
                YVERIFY(GetVirtualAttribute(name, consumer));
            }
            
            if (~attributes != NULL) {
                auto children = attributes->GetChildren();
                FOREACH (const auto& pair, children) {
                    consumer->OnMapItem(pair.First());
                    TTreeVisitor visitor(consumer);
                    visitor.Visit(pair.Second());
                }
            }

            consumer->OnEndMap();

            return TGetResult::CreateDone();
        } else {
            Stroka prefix;
            TYPath tailPath;
            ChopYPathPrefix(TYPath(path.begin() + 1, path.end()), &prefix, &tailPath);

            if (GetVirtualAttribute(prefix, consumer))
                return TGetResult::CreateDone();

            if (~attributes == NULL) {
                throw TYTreeException() << "Node has no custom attributes";
            }

            auto child = attributes->FindChild(prefix);
            if (~child == NULL) {
                throw TYTreeException() << Sprintf("Attribute %s it not found",
                    ~prefix.Quote());
            }

            TTreeVisitor visitor(consumer);
            visitor.Visit(child);
            return TGetResult::CreateDone();
        }
    } else {
        return Navigate(path);
    }
}

IYPathService::TNavigateResult TNodeBase::Navigate(TYPath path)
{
    if (path.empty()) {
        return TNavigateResult::CreateDone(this);
    }

    if (path[0] == '@') {
        auto attributes = GetAttributes();
        if (~attributes == NULL) {
            throw TYTreeException() << "Node has no custom attributes";
        }

        return TNavigateResult::CreateRecurse(
            AsYPath(~attributes),
            TYPath(path.begin() + 1, path.end()));
    }

    return DoNavigate(path);
}

IYPathService::TNavigateResult TNodeBase::DoNavigate(TYPath path)
{
    UNUSED(path);
    throw TYTreeException() << "Navigation is not supported";
}

IYPathService::TSetResult TNodeBase::Set(
    TYPath path, 
    TYsonProducer::TPtr producer)
{
    if (path.empty()) {
        return SetSelf(producer);
    }

    if (path[0] == '@') {
        auto attributes = GetAttributes();
        if (~attributes == NULL) {
            attributes = ~GetFactory()->CreateMap();
            SetAttributes(attributes);
        }

        // TODO: should not be able to override a virtual attribute

        return IYPathService::TSetResult::CreateRecurse(
            AsYPath(~attributes),
            TYPath(path.begin() + 1, path.end()));
    } else {
        return Navigate(path);
    }
}

IYPathService::TRemoveResult TNodeBase::Remove(TYPath path)
{
    if (path.empty()) {
        return RemoveSelf();
    }

    if (path[0] == '@') {
        auto attributes = GetAttributes();
        if (~attributes == NULL) {
            throw TYTreeException() << "Node has no custom attributes";
        }

        return IYPathService::TRemoveResult::CreateRecurse(
            AsYPath(~attributes),
            TYPath(path.begin() + 1, path.end()));
    } else {
        return Navigate(path);
    }
}

IYPathService::TLockResult TNodeBase::Lock(TYPath path)
{
    UNUSED(path);
    throw TYTreeException() << "Locking is not supported";
}

IYPathService::TRemoveResult TNodeBase::RemoveSelf()
{
    auto parent = GetParent();

    if (~parent == NULL) {
        throw TYTreeException() << "Cannot remove the root";
    }

    parent->AsComposite()->RemoveChild(this);
    return TRemoveResult::CreateDone();
}

IYPathService::TGetResult TNodeBase::GetSelf(IYsonConsumer* consumer)
{
    TTreeVisitor visitor(consumer, false);
    visitor.Visit(this);
    return TGetResult::CreateDone();
}

IYPathService::TSetResult TNodeBase::SetSelf(TYsonProducer::TPtr producer)
{
    throw TYTreeException() << "Cannot modify the node";
}

yvector<Stroka> TNodeBase::GetVirtualAttributeNames()
{
    return yvector<Stroka>();
}

bool TNodeBase::GetVirtualAttribute(const Stroka& name, IYsonConsumer* consumer)
{
    UNUSED(name);
    UNUSED(consumer);
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT


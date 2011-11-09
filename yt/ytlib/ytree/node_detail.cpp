#include "stdafx.h"
#include "node_detail.h"
#include "ypath_detail.h"
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
            // TODO: use fluent API

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

            consumer->OnEndMap(false);

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
                throw TYTreeException() << Sprintf("Attribute %s is not found",
                    ~prefix.Quote());
            }

            TTreeVisitor visitor(consumer);
            visitor.Visit(child);
            return TGetResult::CreateDone();
        }
    } else {
        return GetRecursive(path, consumer);
    }
}

IYPathService::TGetResult TNodeBase::GetSelf(IYsonConsumer* consumer)
{
    TTreeVisitor visitor(consumer, false);
    visitor.Visit(this);
    return TGetResult::CreateDone();
}

IYPathService::TGetResult TNodeBase::GetRecursive(TYPath path, IYsonConsumer* consumer)
{
    UNUSED(consumer);
    return Navigate(path);
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
            IYPathService::FromNode(~attributes),
            TYPath(path.begin() + 1, path.end()));
    }

    return NavigateRecursive(path);
}

IYPathService::TNavigateResult TNodeBase::NavigateRecursive(TYPath path)
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
            IYPathService::FromNode(~attributes),
            TYPath(path.begin() + 1, path.end()));
    } else {
        return SetRecursive(path, producer);
    }
}

IYPathService::TSetResult TNodeBase::SetSelf(TYsonProducer::TPtr producer)
{
    UNUSED(producer);
    throw TYTreeException() << "Cannot modify the node";
}

IYPathService::TSetResult TNodeBase::SetRecursive(TYPath path, TYsonProducer::TPtr producer)
{
    UNUSED(producer);
    return Navigate(path);
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
            IYPathService::FromNode(~attributes),
            TYPath(path.begin() + 1, path.end()));
    } else {
        return RemoveRecursive(path);
    }
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

IYPathService::TRemoveResult TNodeBase::RemoveRecursive(TYPath path)
{
    return Navigate(path);
}

IYPathService::TLockResult TNodeBase::Lock(TYPath path)
{
    if (path.empty()) {
        return LockSelf();
    }
    
    if (path[0] == '@') {
        throw TYTreeException() << "Locking attributes is not supported";
    }

    return LockRecursive(path);
}

IYPathService::TLockResult TNodeBase::LockSelf()
{
    throw TYTreeException() << "Locking is not supported";
}

IYPathService::TLockResult TNodeBase::LockRecursive(TYPath path)
{
    return Navigate(path);
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

IYPathService::TNavigateResult TMapNodeMixin::NavigateRecursive(TYPath path)
{
    Stroka prefix;
    TYPath tailPath;
    ChopYPathPrefix(path, &prefix, &tailPath);

    auto child = FindChild(prefix);
    if (~child == NULL) {
        throw TYTreeException() << Sprintf("Key %s is not found",
            ~prefix.Quote());
    }

    return IYPathService::TNavigateResult::CreateRecurse(IYPathService::FromNode(~child), tailPath);
}

IYPathService::TSetResult TMapNodeMixin::SetRecursive(
    TYPath path,
    TYsonProducer* producer,
    ITreeBuilder* builder)
{
    Stroka prefix;
    TYPath tailPath;
    ChopYPathPrefix(path, &prefix, &tailPath);

    auto child = FindChild(prefix);
    if (~child != NULL) {
        return IYPathService::TSetResult::CreateRecurse(IYPathService::FromNode(~child), tailPath);
    }

    if (tailPath.empty()) {
        builder->BeginTree();
        producer->Do(builder);
        auto newChild = builder->EndTree();
        AddChild(newChild, prefix);
        return IYPathService::TSetResult::CreateDone(newChild);
    } else {
        auto newChild = GetFactory()->CreateMap();
        AddChild(newChild, prefix);
        return IYPathService::TSetResult::CreateRecurse(IYPathService::FromNode(~newChild), tailPath);
    }
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TNavigateResult TListNodeMixin::NavigateRecursive(TYPath path)
{
    Stroka prefix;
    TYPath tailPath;
    ChopYPathPrefix(path, &prefix, &tailPath);

    int index;
    try {
        index = FromString<int>(prefix);
    } catch (...) {
        throw TYTreeException() << Sprintf("Failed to parse child index %s",
            ~prefix.Quote());
    }

    return GetYPathChild(index, tailPath);
}

IYPathService::TSetResult TListNodeMixin::SetRecursive(
    TYPath path,
    TYsonProducer* producer,
    ITreeBuilder* builder)
{
    Stroka prefix;
    TYPath tailPath;
    ChopYPathPrefix(path, &prefix, &tailPath);

    if (prefix.empty()) {
        throw TYTreeException() << "Empty child index";
    }

    if (prefix == "+") {
        return CreateYPathChild(GetChildCount(), tailPath, producer, builder);
    } else if (prefix == "-") {
        return CreateYPathChild(0, tailPath, producer, builder);
    }

    char lastPrefixCh = prefix[prefix.length() - 1];
    TStringBuf indexString =
        lastPrefixCh == '+' || lastPrefixCh == '-'
        ? TStringBuf(prefix.begin() + 1, prefix.end())
        : prefix;

    int index;
    try {
        index = FromString<int>(indexString);
    } catch (...) {
        throw TYTreeException() << Sprintf("Failed to parse child index %s",
            ~Stroka(indexString).Quote());
    }

    if (lastPrefixCh == '+') {
        return CreateYPathChild(index + 1, tailPath, producer, builder);
    } else if (lastPrefixCh == '-') {
        return CreateYPathChild(index, tailPath, producer, builder);
    } else {
        auto navigateResult = GetYPathChild(index, tailPath);
        YASSERT(navigateResult.Code == IYPathService::ECode::Recurse);
        return IYPathService::TSetResult::CreateRecurse(navigateResult.RecurseService, navigateResult.RecursePath);
    }
}

IYPathService::TSetResult TListNodeMixin::CreateYPathChild(
    int beforeIndex,
    TYPath tailPath,
    TYsonProducer* producer,
    ITreeBuilder* builder)
{
    if (tailPath.empty()) {
        builder->BeginTree();
        producer->Do(builder);
        auto newChild = builder->EndTree();
        AddChild(newChild, beforeIndex);
        return IYPathService::TSetResult::CreateDone(newChild);
    } else {
        auto newChild = GetFactory()->CreateMap();
        AddChild(newChild, beforeIndex);
        return IYPathService::TSetResult::CreateRecurse(IYPathService::FromNode(~newChild), tailPath);
    }
}

IYPathService::TNavigateResult TListNodeMixin::GetYPathChild(
    int index,
    TYPath tailPath) const
{
    int count = GetChildCount();
    if (count == 0) {
        throw TYTreeException() << "List is empty";
    }

    if (index < 0 || index >= count) {
        throw TYTreeException() << Sprintf("Invalid child index %d, expecting value in range 0..%d",
            index,
            count - 1);
    }

    auto child = FindChild(index);
    return IYPathService::TNavigateResult::CreateRecurse(IYPathService::FromNode(~child), tailPath);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT


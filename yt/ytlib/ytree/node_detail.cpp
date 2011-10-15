#include "node_detail.h"
#include "ypath.h"
#include "tree_visitor.h"
#include "tree_builder.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

IYPathService::TGetResult TNodeBase::Get(
    TYPath path,
    IYsonConsumer* events)
{
    auto navigateResult = Navigate(path);
    switch (navigateResult.Code) {
        case IYPathService::ECode::Done: {
            TTreeVisitor visitor(events);
            visitor.Visit(navigateResult.Value);
            return TGetResult::CreateDone();
        }

        case IYPathService::ECode::Recurse:
            return navigateResult;

        default:
            YUNREACHABLE();
    }
}

IYPathService::TNavigateResult TNodeBase::Navigate(
    TYPath path)
{
    if (!path.empty()) {
        throw TYPathException() << "Cannot navigate from the node";
    }

    return TNavigateResult::CreateDone(this);
}

IYPathService::TSetResult TNodeBase::Set(
    TYPath path, 
    TYsonProducer::TPtr producer)
{
    if (!path.empty()) {
        return Navigate(path);
    }

    return SetSelf(producer);
}

IYPathService::TRemoveResult TNodeBase::Remove(
    TYPath path)
{
    if (!path.empty()) {
        return Navigate(path);
    }

    return RemoveSelf();
}

IYPathService::TLockResult TNodeBase::Lock(TYPath path)
{
    UNUSED(path);
    throw TYPathException() << "Locking is not supported";
}

IYPathService::TRemoveResult TNodeBase::RemoveSelf()
{
    auto parent = GetParent();

    if (~parent == NULL) {
        ythrow TYPathException() << "Cannot remove the root";
    }

    parent->AsComposite()->RemoveChild(this);
    return TRemoveResult::CreateDone();
}

IYPathService::TSetResult TNodeBase::SetSelf(TYsonProducer::TPtr producer)
{
    throw TYPathException() << "Cannot modify the node";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT


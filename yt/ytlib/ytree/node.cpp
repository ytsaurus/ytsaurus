#include "node.h"
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
            return TGetResult::CreateRecurse(
                navigateResult.RecurseService,
                navigateResult.RecursePath);

        case IYPathService::ECode::Error:
            return TGetResult::CreateError(
                navigateResult.ErrorMessage);

        default:
            YASSERT(false);
            return TGetResult();
    }
}

IYPathService::TNavigateResult TNodeBase::Navigate(
    TYPath path)
{
    if (!path.empty()) {
        return TNavigateResult::CreateError("Cannot navigate from the node");
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
    return TLockResult::CreateError("Locking is not supported");
}

IYPathService::TRemoveResult TNodeBase::RemoveSelf()
{
    auto parent = GetParent();

    if (~parent == NULL) {
        return TRemoveResult::CreateError("Cannot remove the root");
    }

    parent->AsComposite()->RemoveChild(this);

    return TRemoveResult::CreateDone();
}

IYPathService::TSetResult TNodeBase::SetSelf(TYsonProducer::TPtr producer)
{
    auto parent = GetParent();

    if (~parent == NULL) {
        return TSetResult::CreateError("Cannot update the root");
    }

    TTreeBuilder builder(GetFactory());
    producer->Do(&builder);
    parent->AsComposite()->ReplaceChild(this, builder.GetRoot());

    return TSetResult::CreateDone();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT


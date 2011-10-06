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
            visitor.Visit(navigateResult.Value->AsImmutable());
            return TGetResult::CreateDone(TVoid());
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
    UNUSED(path);
    if (path.empty()) {
        return TNavigateResult::CreateDone(AsImmutable());
    } else {
        return TNavigateResult::CreateError("Cannot navigate from the node");
    }
}

IYPathService::TSetResult TNodeBase::Set(
    TYPath path, 
    TYsonProducer::TPtr producer)
{
    if (path.empty()) {
        return SetSelf(producer);
    }

    auto navigateResult = Navigate(path);
    switch (navigateResult.Code) {
        case IYPathService::ECode::Recurse:
            return TSetResult::CreateRecurse(
                navigateResult.RecurseService,
                navigateResult.RecursePath);

        case IYPathService::ECode::Error:
            return TSetResult::CreateError(
                navigateResult.ErrorMessage);

        default:
            YASSERT(false);
            return TSetResult();
    }
}

IYPathService::TRemoveResult TNodeBase::Remove(
    TYPath path)
{
    if (path.empty()) {
        return RemoveSelf();
    }

    auto navigateResult = Navigate(path);
    switch (navigateResult.Code) {
        case IYPathService::ECode::Recurse:
            return TRemoveResult::CreateRecurse(
                navigateResult.RecurseService,
                navigateResult.RecursePath);

        case IYPathService::ECode::Error:
            return TRemoveResult::CreateError(
                navigateResult.ErrorMessage);

        default:
            YASSERT(false);
            return TRemoveResult();
    }
}

IYPathService::TRemoveResult TNodeBase::RemoveSelf()
{
    auto parent = GetParent();

    if (~parent == NULL) {
        return TRemoveResult::CreateError("Cannot remove the root");
    }

    auto mutableParent = parent->AsMutable()->AsComposite();
    mutableParent->RemoveChild(this);

    return TRemoveResult::CreateDone(TVoid());
}

IYPathService::TSetResult TNodeBase::SetSelf(TYsonProducer::TPtr producer)
{
    auto parent = GetParent();

    if (~parent == NULL) {
        return TSetResult::CreateError("Cannot update the root");
    }

    auto mutableParent = parent->AsMutable()->AsComposite();

    TTreeBuilder builder(GetFactory());
    producer->Do(&builder);
    mutableParent->ReplaceChild(this, builder.GetRoot());

    return TSetResult::CreateDone(TVoid());
}

TNodeBase::TPtr TNodeBase::AsMutableImpl() const
{
    return const_cast<TNodeBase*>(this);
}

TNodeBase::TConstPtr TNodeBase::AsImmutableImpl() const
{
    return const_cast<TNodeBase*>(this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT


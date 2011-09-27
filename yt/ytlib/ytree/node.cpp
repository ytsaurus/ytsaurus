#include "node.h"
#include "ypath.h"
#include "tree_visitor.h"
#include "tree_builder.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

IYPathService::TGetResult TNodeBase::Get(
    const TYPath& path,
    IYsonConsumer* events) const
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
                navigateResult.RecurseNode,
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
    const TYPath& path) const
{
    UNUSED(path);
    if (path.empty()) {
        return TNavigateResult::CreateDone(AsImmutable());
    } else {
        return TNavigateResult::CreateError("Cannot navigate from the node");
    }
}

IYPathService::TSetResult TNodeBase::Set(
    const TYPath& path, 
    TYsonProducer::TPtr producer)
{
    if (path.empty()) {
        return SetSelf(producer);
    }

    auto navigateResult = Navigate(path);
    switch (navigateResult.Code) {
        case IYPathService::ECode::Recurse:
            return TSetResult::CreateRecurse(
                navigateResult.RecurseNode,
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
    const TYPath& path)
{
    if (path.empty()) {
        return RemoveSelf();
    }

    auto navigateResult = Navigate(path);
    switch (navigateResult.Code) {
        case IYPathService::ECode::Recurse:
            return TRemoveResult::CreateRecurse(
                navigateResult.RecurseNode,
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
    if (~Parent == NULL) {
        return TRemoveResult::CreateError("Cannot remove the root");
    }

    Parent->RemoveChild(this);
    return TRemoveResult::CreateDone(TVoid());
}

IYPathService::TSetResult TNodeBase::SetSelf(TYsonProducer::TPtr producer)
{
    if (~Parent == NULL) {
        return TSetResult::CreateError("Cannot update the root");
    }

    TTreeBuilder builder(GetFactory());
    producer->Do(&builder);
    Parent->ReplaceChild(this, builder.GetRoot());
    return TSetResult::CreateDone(TVoid());
}

TNodeBase* TNodeBase::AsMutableImpl() const
{
    return const_cast<TNodeBase*>(this);
}

const TNodeBase* TNodeBase::AsImmutableImpl() const
{
    return this;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT


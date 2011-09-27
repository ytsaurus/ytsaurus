#include "ytree.h"
#include "tree_visitor.h"
#include "tree_builder.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

INode::TGetResult TNodeBase::YPathGet(
    const TYPath& path,
    IYsonConsumer* events) const
{
    auto navigateResult = YPathNavigate(path);
    switch (navigateResult.Code) {
        case INode::EYPathCode::Done: {
            TTreeVisitor visitor(events);
            visitor.Visit(navigateResult.Value->AsImmutable());
            return TGetResult::CreateDone(TVoid());
        }

        case INode::EYPathCode::Recurse:
            return TGetResult::CreateRecurse(
                navigateResult.RecurseNode,
                navigateResult.RecursePath);

        case INode::EYPathCode::Error:
            return TGetResult::CreateError(
                navigateResult.ErrorMessage);

        default:
            YASSERT(false);
            return TGetResult();
    }
}

INode::TNavigateResult TNodeBase::YPathNavigate(
    const TYPath& path) const
{
    UNUSED(path);
    if (path.empty()) {
        return TNavigateResult::CreateDone(AsImmutable());
    } else {
        return TNavigateResult::CreateError("Cannot navigate from the node");
    }
}

INode::TSetResult TNodeBase::YPathSet(
    const TYPath& path, 
    TYsonProducer::TPtr producer)
{
    if (path.empty()) {
        return YPathSetSelf(producer);
    }

    auto navigateResult = YPathNavigate(path);
    switch (navigateResult.Code) {
        case INode::EYPathCode::Recurse:
            return TSetResult::CreateRecurse(
                navigateResult.RecurseNode,
                navigateResult.RecursePath);

        case INode::EYPathCode::Error:
            return TSetResult::CreateError(
                navigateResult.ErrorMessage);

        default:
            YASSERT(false);
            return TSetResult();
    }
}

INode::TRemoveResult TNodeBase::YPathRemove(
    const TYPath& path)
{
    if (path.empty()) {
        return YPathRemoveSelf();
    }

    auto navigateResult = YPathNavigate(path);
    switch (navigateResult.Code) {
        case INode::EYPathCode::Recurse:
            return TRemoveResult::CreateRecurse(
                navigateResult.RecurseNode,
                navigateResult.RecursePath);

        case INode::EYPathCode::Error:
            return TRemoveResult::CreateError(
                navigateResult.ErrorMessage);

        default:
            YASSERT(false);
            return TRemoveResult();
    }
}

INode::TRemoveResult TNodeBase::YPathRemoveSelf()
{
    if (~Parent == NULL) {
        return TRemoveResult::CreateError("Cannot remove the root");
    }

    Parent->RemoveChild(this);
    return TRemoveResult::CreateDone(TVoid());
}

INode::TSetResult TNodeBase::YPathSetSelf(TYsonProducer::TPtr producer)
{
    if (~Parent == NULL) {
        return TSetResult::CreateError("Cannot update the root");
    }

    TTreeBuilder builder(GetFactory());
    producer->Do(&builder);
    Parent->ReplaceChild(this, builder.GetRoot());
    return TSetResult::CreateDone(TVoid());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT


#include "ytree.h"
#include "tree_visitor.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

INode::TGetResult TNodeBase::YPathGet(
    const TYPath& path,
    TIntrusivePtr<IYsonEvents> events ) const
{
    auto navigateResult = YPathNavigate(path);
    switch (navigateResult.Code) {
        case INode::ECode::Done: {
            TTreeVisitor visitor(events);
            visitor.Visit(navigateResult.Value->AsImmutable());
            return TGetResult::CreateDone(TVoid());
        }

        case INode::ECode::Recurse:
            return TGetResult::CreateRecurse(
                navigateResult.RecurseNode,
                navigateResult.RecursePath);

        case INode::ECode::Error:
            return TGetResult::CreateError(
                navigateResult.ErrorMessage);

        default:
            YASSERT(false);
            return INode::TGetResult();
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
    TIntrusiveConstPtr<INode> value)
{
    if (path.empty()) {
        if (GetType() == value->GetType()) {
            return DoAssign(value);
        } else {
            return TSetResult::CreateError(Sprintf("Cannot change node type from %s to %s during update",
                ~GetType().ToString().Quote(),
                ~value->GetType().ToString().Quote()));
        }
    }

    auto navigateResult = YPathNavigate(path);
    switch (navigateResult.Code) {
        case INode::ECode::Recurse:
            return TSetResult::CreateRecurse(
                navigateResult.RecurseNode,
                navigateResult.RecursePath);

        case INode::ECode::Error:
            return TGetResult::CreateError(
                navigateResult.ErrorMessage);

        default:
            YASSERT(false);
            return INode::TGetResult();
    }
}

INode::TRemoveResult TNodeBase::YPathRemove(
    const TYPath& path)
{
    UNUSED(path);
    return TRemoveResult::CreateError("Cannot remove the node");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT


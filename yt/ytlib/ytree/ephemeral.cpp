#include "ephemeral.h"
#include "ypath.h"

namespace NYT {
namespace NYTree {
namespace NEphemeral {

////////////////////////////////////////////////////////////////////////////////

INodeFactory* TNodeBase::GetFactory() const
{
    return TNodeFactory::Get();
}

////////////////////////////////////////////////////////////////////////////////

INode::TNavigateResult TMapNode::YPathNavigate(
    const TYPath& path) const
{
    if (path.empty()) {
        return TNavigateResult::CreateDone(AsImmutable());
    }

    Stroka prefix;
    TYPath tailPath;
    ChopYPathPrefix(path, &prefix, &tailPath);

    auto child = FindChild(prefix);
    if (~child == NULL) {
        return TNavigateResult::CreateError(Sprintf("child %s it not found",
            ~prefix.Quote()));
    } else {
        return TNavigateResult::CreateRecurse(child, tailPath);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NEphemeral
} // namespace NYTree
} // namespace NYT


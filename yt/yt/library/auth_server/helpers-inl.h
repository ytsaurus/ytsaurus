#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TErrorOr<T> GetByYPath(const NYTree::INodePtr& node, const NYPath::TYPath& path)
{
    try {
        auto child = NYTree::FindNodeByYPath(node, path);
        if (!child) {
            return TError("Missing %v", path);
        }
        return NYTree::ConvertTo<T>(std::move(child));
    } catch (const std::exception& ex) {
        return TError("Unable to extract %v", path) << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

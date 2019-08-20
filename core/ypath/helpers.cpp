#include "helpers.h"

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

std::optional<TYPath> TryComputeYPathSuffix(const TYPath& path, const TYPath& prefix)
{
    // TODO(babenko): this check is pessimistic; consider using tokenizer
    if (!path.StartsWith(prefix)) {
        return std::nullopt;
    }

    if (path.length() == prefix.length()) {
        return TYPath();
    }

    if (path[prefix.length()] != '/') {
        return std::nullopt;
    }

    return path.substr(prefix.length());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath

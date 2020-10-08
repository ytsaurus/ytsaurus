#include <util/generic/string.h> // should be before #include "ypath_join.h"
#include "ypath_join.h"

#include <util/generic/yexception.h>

namespace NYT {

namespace {

constexpr TStringBuf SLASH = "/";

bool EndsWithSlash(const TYPath& path)
{
    if (path.EndsWith('/')) {
        Y_ENSURE(!path.EndsWith("\\/"), R"(Path with \\/ found, failed to join it)");
        return true;
    }
    return false;
}

} // unnamed namespace

namespace NDetail {

void AppendToYPath(TYPath& path) noexcept
{
    Y_UNUSED(path);
}

void AppendToYPathImpl(TYPath& path, TStringBuf arg)
{
    if (Y_UNLIKELY(arg.StartsWith("//") || arg == SLASH)) {
        path.clear();
    }

    ui32 slashCount = 0;
    if (arg != SLASH) {
        if (arg.StartsWith('/')) {
            ++slashCount;
        }
        if (!path.empty() && EndsWithSlash(path)) {
            ++slashCount;
        }
    }

    if (slashCount == 2) {
        path += TStringBuf{arg.cbegin() + 1, arg.cend()};
    } else {
        if ((slashCount == 0 && !path.empty()) || path == SLASH) {
            path += SLASH;
        }
        path += arg;
    }
}

} // namespace NDetail

} // namespace NYT

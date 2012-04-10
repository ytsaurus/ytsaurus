#include "preprocess.h"

#include <ytlib/misc/foreach.h>

#include <ytlib/ytree/lexer.h>
#include <ytlib/ytree/ypath_client.h>

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYPath PreprocessYPath(const TYPath& path) {
    Stroka suffix;
    auto token = ChopToken(path, &suffix);
    if (token.GetType() == ETokenType::Tilde) {
        auto userName = Stroka(getenv("USERNAME"));
        TYPath userDirectory = Stroka("//home/") + EscapeYPath(userName);
        return userDirectory + suffix;
    }
    return path;
}

std::vector<TYPath> PreprocessYPaths(const std::vector<TYPath>& paths) {
    std::vector<TYPath> result;
    result.reserve(paths.size());
    FOREACH (const auto& path, paths) {
        result.push_back(PreprocessYPath(path));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

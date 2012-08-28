#include "preprocess.h"

#include <ytlib/misc/foreach.h>

#include <ytlib/ytree/tokenizer.h>
#include <ytlib/ytree/ypath_format.h>
#include <ytlib/ytree/ypath_client.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TRichYPath PreprocessYPath(const TRichYPath& path)
{
    TTokenizer tokenizer(path.GetPath());
    tokenizer.ParseNext();
    if (tokenizer.GetCurrentType() == HomeDirToken) {
        auto userName = Stroka(getenv("USERNAME"));
        TYPath userDirectory = Stroka("//home/") + EscapeYPathToken(userName);
        return TRichYPath(
            userDirectory + tokenizer.GetCurrentSuffix(),
            path.Attributes());
    }
    return path;
}

std::vector<TRichYPath> PreprocessYPaths(const std::vector<TRichYPath>& paths)
{
    std::vector<TRichYPath> result;
    result.reserve(paths.size());
    FOREACH (const auto& path, paths) {
        result.push_back(PreprocessYPath(path));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

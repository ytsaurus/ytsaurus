#include "preprocess.h"

#include <ytlib/misc/foreach.h>

#include <ytlib/ypath/tokenizer.h>

namespace NYT {
namespace NDriver {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TRichYPath PreprocessYPath(const TRichYPath& path)
{
    if (!path.GetPath().has_prefix("~")) {
        return path;
    }
    
    NYPath::TTokenizer tokenizer(path.GetPath());
    YCHECK(tokenizer.Advance() == ETokenType::Literal);
    auto userName = tokenizer.GetLiteralValue().substr(1);
    if (userName.empty()) {
        userName = getenv("USERNAME");
    }
    return TRichYPath(
        Stroka("//home/") + ToYPathLiteral(userName) + tokenizer.GetSuffix(),
        path.Attributes());
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

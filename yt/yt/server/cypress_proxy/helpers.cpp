#include "helpers.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ypath/token.h>
#include <yt/yt/core/ypath/tokenizer.h>

namespace NYT::NCypressProxy {

using namespace NCypressClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

std::vector<TYPathBuf> TokenizeUnresolvedSuffix(const TYPath& unresolvedSuffix)
{
    constexpr auto TypicalPathTokenCount = 3;
    std::vector<TYPathBuf> pathTokens;
    pathTokens.reserve(TypicalPathTokenCount);

    TTokenizer tokenizer(unresolvedSuffix);
    tokenizer.Advance();

    while (tokenizer.GetType() != ETokenType::EndOfStream) {
        tokenizer.Expect(ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(ETokenType::Literal);
        pathTokens.push_back(tokenizer.GetToken());
        tokenizer.Advance();
    };

    return pathTokens;
}

////////////////////////////////////////////////////////////////////////////////

bool IsSupportedSequoiaType(EObjectType type)
{
    return IsSequoiaCompositeNodeType(type) || IsScalarType(type);
}

bool IsSequoiaCompositeNodeType(EObjectType type)
{
    return type == EObjectType::SequoiaMapNode || type == EObjectType::Scion;
}

////////////////////////////////////////////////////////////////////////////////

TYPath GetJoinedNestedNodesPath(const TYPath& parentPath, const std::vector<TYPathBuf>& childKeys)
{
    TStringBuilder builder;

    auto nestedLength = std::transform_reduce(
        childKeys.begin(),
        childKeys.end(),
        0,
        std::plus<size_t>{},
        [] (TYPathBuf key) {
            return key.size() + 1;
        });
    builder.Reserve(parentPath.size() + nestedLength);

    builder.AppendString(parentPath);
    for (const auto& childKey : childKeys) {
        builder.AppendChar('/');
        builder.AppendString(childKey);
    }
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy

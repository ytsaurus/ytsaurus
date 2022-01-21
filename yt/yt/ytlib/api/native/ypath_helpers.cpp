#include "ypath_helpers.h"
#include "config.h"

#include <yt/yt/core/ypath/tokenizer.h>

namespace NYT::NApi::NNative {

using namespace NYPath;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

bool TryParseObjectId(
    const TYPath& path,
    TObjectId* objectId,
    TYPath* pathSuffix)
{
    TTokenizer tokenizer(path);
    if (tokenizer.Advance() != NYPath::ETokenType::Literal) {
        return false;
    }

    auto token = tokenizer.GetToken();
    if (!token.StartsWith(ObjectIdPathPrefix)) {
        return false;
    }

    *objectId = TObjectId::FromString(token.SubString(
        ObjectIdPathPrefix.length(),
        token.length() - ObjectIdPathPrefix.length()));

    if (pathSuffix) {
        *pathSuffix = tokenizer.GetSuffix();
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

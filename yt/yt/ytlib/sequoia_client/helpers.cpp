#include "helpers.h"

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ypath/tokenizer.h>

namespace NYT::NSequoiaClient {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TMangledSequoiaPath MangleSequoiaPath(NYPath::TYPathBuf path)
{
    YT_VERIFY(!path.empty());
    YT_VERIFY(path == "/" || path.back() != '/');
    return TMangledSequoiaPath(NYPath::TYPath(path) + "/");
}

NYPath::TYPath DemangleSequoiaPath(const TMangledSequoiaPath& mangledPath)
{
    YT_VERIFY(!mangledPath.Underlying().empty());
    YT_VERIFY(mangledPath.Underlying().back() == '/');
    return mangledPath.Underlying().substr(0, mangledPath.Underlying().size() - 1);
}

TMangledSequoiaPath MakeLexicographicallyMaximalMangledSequoiaPathForPrefix(const TMangledSequoiaPath& prefix)
{
    return TMangledSequoiaPath(prefix.Underlying() + '\xFF');
}

TString ToStringLiteral(TStringBuf key)
{
    TStringBuilder builder;
    TTokenizer tokenizer(key);
    tokenizer.Advance();
    tokenizer.Expect(ETokenType::Literal);
    auto literal = tokenizer.GetLiteralValue();
    tokenizer.Advance();
    tokenizer.Expect(ETokenType::EndOfStream);
    return literal;
}

////////////////////////////////////////////////////////////////////////////////

constexpr TErrorCode RetriableSequoiaErrors[] = {
    NTabletClient::EErrorCode::TransactionLockConflict,
};

bool IsRetriableSequoiaError(const TError& error)
{
    return AnyOf(RetriableSequoiaErrors, [&] (auto errorCode) {
        return error.FindMatching(errorCode);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

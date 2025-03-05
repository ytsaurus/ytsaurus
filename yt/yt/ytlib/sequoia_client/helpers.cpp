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

bool IsRetriableSequoiaError(const TError& error)
{
    if (error.IsOK()) {
        return false;
    }

    return AnyOf(RetriableSequoiaErrorCodes, [&] (auto errorCode) {
        return error.FindMatching(errorCode);
    });
}

bool IsRetriableSequoiaReplicasError(
    const TError& error,
    const std::vector<TErrorCode>& retriableErrorCodes)
{
    if (error.IsOK()) {
        return false;
    }

    return AnyOf(retriableErrorCodes, [&] (auto errorCode) {
        return error.FindMatching(errorCode);
    });
}

bool IsMethodShouldBeHandledByMaster(const std::string& method)
{
    return
        method == "Fetch" ||
        method == "BeginUpload" ||
        method == "GetUploadParams" ||
        method == "EndUpload";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

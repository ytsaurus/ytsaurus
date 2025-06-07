#include "helpers.h"

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ypath/tokenizer.h>

namespace NYT::NSequoiaClient {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TMangledSequoiaPath MangleSequoiaPath(NYPath::TYPathBuf path)
{
    TString mangledPath;
    mangledPath.reserve(path.size());

    TTokenizer tokenizer(path);
    tokenizer.Advance();

    tokenizer.Expect(ETokenType::Slash);
    mangledPath += tokenizer.GetToken();
    tokenizer.Advance();

    for (; tokenizer.Skip(ETokenType::Slash); tokenizer.Advance()) {
        tokenizer.Expect(ETokenType::Literal);
        mangledPath += MangledPathSeparator;
        mangledPath += tokenizer.GetLiteralValue();
    }

    tokenizer.Expect(ETokenType::EndOfStream);

    mangledPath += MangledPathSeparator;

    return TMangledSequoiaPath(std::move(mangledPath));
}

NYPath::TYPath DemangleSequoiaPath(const TMangledSequoiaPath& mangledPath)
{
    const auto& rawMangledPath = mangledPath.Underlying();
    YT_VERIFY(rawMangledPath.StartsWith("/"));
    YT_VERIFY(rawMangledPath.EndsWith(MangledPathSeparator));

    constexpr int ExpectedSystemCharacterMaxCount = 5;

    NYPath::TYPath path;
    path.reserve(rawMangledPath.size() + ExpectedSystemCharacterMaxCount);

    for (int from = 0, to = 0; to < ssize(rawMangledPath); ++to) {
        if (rawMangledPath[to] != MangledPathSeparator) {
            continue;
        }

        auto interval = TStringBuf(rawMangledPath, from, to - from);
        if (from == 0) {
            path += interval;
        } else {
            path += "/";
            path += ToYPathLiteral(interval);
        }
        from = to + 1;
    }

    return path;
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

    if (error.FindMatching([] (const TError& error) {
            return std::ranges::find(RetriableSequoiaErrorCodes, error.GetCode()) != std::end(RetriableSequoiaErrorCodes);
        }))
    {
        return true;
    }

    return
        error.FindMatching(NTransactionClient::EErrorCode::ParticipantFailedToPrepare) &&
        !error.FindMatching(EErrorCode::TransactionActionFailedOnMasterCell);
}

bool IsRetriableSequoiaReplicasError(
    const TError& error,
    const std::vector<TErrorCode>& /*retriableErrorCodes*/)
{
    if (error.IsOK()) {
        return false;
    }

    return !error.FindMatching(EErrorCode::TransactionActionFailedOnMasterCell);
}

void ThrowOnSequoiaReplicasError(
    const TError& error,
    const std::vector<TErrorCode>& retriableErrorCodes)
{
    if (IsRetriableSequoiaReplicasError(error, retriableErrorCodes)) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransientFailure,
            "Sequoia retriable error")
            << std::move(error);
    }
    error.ThrowOnError();
}

bool IsMethodShouldBeHandledByMaster(const std::string& method)
{
    return
        method == "Fetch" ||
        method == "BeginUpload" ||
        method == "GetUploadParams" ||
        method == "EndUpload" ||
        method == "GetMountInfo";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

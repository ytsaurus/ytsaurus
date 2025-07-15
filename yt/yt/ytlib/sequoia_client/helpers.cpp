#include "helpers.h"

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ypath/tokenizer.h>

namespace NYT::NSequoiaClient {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TMangledSequoiaPath MangleSequoiaPath(const TRealPath& path)
{
    TString mangledPath;
    mangledPath.reserve(path.Underlying().size());

    TTokenizer tokenizer(path.Underlying());
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

TRealPath DemangleSequoiaPath(const TMangledSequoiaPath& mangledPath)
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

    return TRealPath(std::move(path));
}

TString ToStringLiteral(TYPathBuf key)
{
    if (key.empty()) {
        return {};
    }

    TTokenizer tokenizer(key);
    tokenizer.Advance();
    tokenizer.Expect(ETokenType::Literal);
    auto literal = tokenizer.GetLiteralValue();
    tokenizer.Advance();
    tokenizer.Expect(ETokenType::EndOfStream);

    return literal;
}

inline bool IsForbiddenYPathSymbol(char ch)
{
    return ch == MangledPathSeparator;
}

TYPath ValidateAndMakeYPath(TRawYPath&& path)
{
    for (auto ch : path.Underlying()) {
        if (IsForbiddenYPathSymbol(ch)) {
            THROW_ERROR_EXCEPTION("Path contains a forbidden symbol %x", ch);
        }
    }
    return std::move(path.Underlying());
}

////////////////////////////////////////////////////////////////////////////////

bool IsRetriableSequoiaError(const TError& error)
{
    if (error.IsOK()) {
        return false;
    }

    if (error.FindMatching(EErrorCode::TransactionActionFailedOnMasterCell)) {
        return false;
    }

    // TODO(shakurov): don't treat dynamic table error as retriable if error is
    // not related to ground cluster.

    if (error.FindMatching([] (const TError& error) {
            return std::ranges::find(RetriableSequoiaErrorCodes, error.GetCode()) != std::end(RetriableSequoiaErrorCodes);
        }))
    {
        return true;
    }

    return error
        .FindMatching(NTransactionClient::EErrorCode::ParticipantFailedToPrepare)
        .has_value();
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

bool IsMethodHandledByMaster(const std::string& method)
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

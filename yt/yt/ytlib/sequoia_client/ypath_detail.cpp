#include "ypath_detail.h"

namespace NYT::NSequoiaClient {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

void AppendAsCanonicalRelativePathOrThrow(
    TStringBuilderBase* builder,
    TStringBuf path)
{
    TTokenizer tokenizer(path);
    tokenizer.Advance();

    tokenizer.Skip(ETokenType::Ampersand);

    while (tokenizer.GetType() != ETokenType::EndOfStream) {
        tokenizer.Expect(ETokenType::Slash);

        tokenizer.Advance();
        tokenizer.Expect(ETokenType::Literal);

        builder->AppendChar(TRelativePath::Separator);
        builder->AppendString(tokenizer.GetToken());

        tokenizer.Advance();
        tokenizer.Skip(ETokenType::Ampersand);
    }
}

TRelativePath TRelativePath::MakeCanonicalPathOrThrow(TStringBuf path)
{
    TStringBuilder builder;
    builder.Reserve(path.size());

    try {
        AppendAsCanonicalRelativePathOrThrow(&builder, path);
    } catch (const TErrorException& ex) {
        THROW_ERROR_EXCEPTION(
            "Could not canonicalize relative path %v. The path is malformed or invalid.",
            path)
            << ex;
    }

    return TRelativePath(builder.Flush());
}

TRelativePath TRelativePath::UnsafeMakeCanonicalPath(TString&& path) noexcept
{
    return TRelativePath(std::move(path));
}

////////////////////////////////////////////////////////////////////////////////

TAbsolutePath::TAbsolutePath(const TMangledSequoiaPath& mangledPath)
    : TBase(DemangleSequoiaPath(mangledPath).Underlying())
{ }

TAbsolutePath TAbsolutePath::MakeCanonicalPathOrThrow(TStringBuf path)
{
    TStringBuilder builder;
    builder.Reserve(path.size());

    try {
        TTokenizer tokenizer(path);
        tokenizer.Advance();
        tokenizer.Expect(ETokenType::Slash);
        builder.AppendString(tokenizer.GetToken());
        AppendAsCanonicalRelativePathOrThrow(&builder, tokenizer.GetSuffix());
    } catch (const TErrorException& ex) {
        THROW_ERROR_EXCEPTION(
            "Could not canonicalize absolute path %v. The path is malformed or invalid.",
            path)
            << ex;
    }

    return TAbsolutePath(builder.Flush());
}

TAbsolutePath TAbsolutePath::UnsafeMakeCanonicalPath(TString&& path) noexcept
{
    return TAbsolutePath(std::move(path));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

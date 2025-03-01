#include "path_visitor.h"

#include "helpers.h"

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

void TPathVisitorMixin::SkipSlash()
{
    if (Tokenizer_.Skip(NYPath::ETokenType::Slash)) {
        return;
    }

    if (LeadingSlashOptional_ && Tokenizer_.GetInput() == Tokenizer_.GetPath()) {
        return;
    }

    THROW_ERROR_EXCEPTION(NAttributes::EErrorCode::MalformedPath,
        "Expected slash but got %Qv",
        Tokenizer_.GetToken());
}

void TPathVisitorMixin::Push(TToken token)
{
    Visit(token,
        [this] (int index) { CurrentPath_.Push(index); },
        [this] (ui64 index) { CurrentPath_.Push(static_cast<int>(index)); },
        [this] (TStringBuf key) { CurrentPath_.Push(key); });
}

void TPathVisitorMixin::AdvanceOver(TToken token)
{
    if (!PathComplete()) {
        Tokenizer_.Advance();
    }
    Push(token);
}

void TPathVisitorMixin::AdvanceOverAsterisk()
{
    if (!AllowAsterisk_) {
        THROW_ERROR_EXCEPTION(NAttributes::EErrorCode::Unimplemented, "Cannot handle asterisks");
    }

    Tokenizer_.Advance();
}

TPathVisitorMixin::TCheckpoint::TCheckpoint(NYPath::TTokenizer& tokenizer)
    : TokenizerCheckpoint_(tokenizer)
{ }

TPathVisitorMixin::TCheckpoint::~TCheckpoint()
{
    if (Defer_) {
        Defer_();
    }
}

void TPathVisitorMixin::TCheckpoint::Defer(std::function<void()> defer)
{
    if (Defer_) {
        Defer_ = [oldDefer = std::move(Defer_), defer = std::move(defer)] () {
            oldDefer();
            defer();
        };
    } else {
        Defer_ = defer;
    }
}

TPathVisitorMixin::TCheckpoint TPathVisitorMixin::CheckpointBranchedTraversal(TToken token)
{
    TCheckpoint result(Tokenizer_);
    Push(token);
    result.Defer([this] () { CurrentPath_.Pop(); });
    return result;
}

NYPath::ETokenType TPathVisitorMixin::GetTokenizerType() const
{
    return Tokenizer_.GetType();
}

TStringBuf TPathVisitorMixin::GetTokenizerInput() const
{
    return Tokenizer_.GetInput();
}

TStringBuf TPathVisitorMixin::GetToken() const
{
    return Tokenizer_.GetToken();
}

const TString& TPathVisitorMixin::GetLiteralValue() const
{
    return Tokenizer_.GetLiteralValue();
}

const NYPath::TYPath& TPathVisitorMixin::GetCurrentPath() const
{
    return CurrentPath_.GetPath();
}

void TPathVisitorMixin::Expect(NYPath::ETokenType type) const
{
    if (Tokenizer_.GetType() == type) {
        return;
    }

    TError error(NAttributes::EErrorCode::MalformedPath,
        "Expected %Qlv but got %Qlv",
        type,
        Tokenizer_.GetType());

    if (Tokenizer_.GetPreviousType() == NYPath::ETokenType::Slash) {
        error <<= TErrorAttribute("note", "the path cannot normally end with a slash");
    }

    THROW_ERROR_EXCEPTION(error);
}

bool TPathVisitorMixin::PathComplete() const
{
    return Tokenizer_.GetType() == NYPath::ETokenType::EndOfStream;
}

void TPathVisitorMixin::Reset(NYPath::TYPathBuf path)
{
    Tokenizer_.Reset(path);
    Tokenizer_.Advance();
    CurrentPath_.Reset();
    StopIteration_ = false;
}

TErrorOr<TIndexParseResult> TPathVisitorMixin::ParseCurrentListIndex(int size) const
{
    Expect(NYPath::ETokenType::Literal);
    auto indexParseResult = ParseListIndex(Tokenizer_.GetToken(), size);

    if (indexParseResult.IsOutOfBounds(size)) {
        return TError(NAttributes::EErrorCode::OutOfBounds,
            "Index %Qv out of bounds for repeated field of size %v",
            Tokenizer_.GetToken(),
            size);
    }

    return indexParseResult;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes

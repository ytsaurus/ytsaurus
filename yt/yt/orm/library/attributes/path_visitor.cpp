#include "path_visitor.h"

#include "helpers.h"

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

void TPathVisitorUtil::SkipSlash()
{
    if (Tokenizer_.Skip(NYPath::ETokenType::Slash)) {
        return;
    }

    if (LeadingSlashOptional_ && Tokenizer_.GetInput() == Tokenizer_.GetPath()) {
        return;
    }

    Throw(EErrorCode::MalformedPath, "Expected slash but got %Qv", Tokenizer_.GetToken());
}

void TPathVisitorUtil::Push(TToken token)
{
    Visit(token,
        [this] (int index) { CurrentPath_.Push(index); },
        [this] (ui64 index) { CurrentPath_.Push(static_cast<int>(index)); },
        [this] (TStringBuf key) { CurrentPath_.Push(key); });
}

void TPathVisitorUtil::AdvanceOver(TToken token)
{
    if (!PathComplete()) {
        Tokenizer_.Advance();
    }
    Push(token);
}

void TPathVisitorUtil::AdvanceOverAsterisk()
{
    if (!AllowAsterisk_) {
        Throw(EErrorCode::Unimplemented, "Cannot handle asterisks");
    }

    Tokenizer_.Advance();
}

TPathVisitorUtil::TCheckpoint::TCheckpoint(NYPath::TTokenizer& tokenizer)
    : TokenizerCheckpoint_(tokenizer)
{ }

TPathVisitorUtil::TCheckpoint::~TCheckpoint()
{
    if (Defer_) {
        Defer_();
    }
}

void TPathVisitorUtil::TCheckpoint::Defer(std::function<void()> defer)
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

TPathVisitorUtil::TCheckpoint TPathVisitorUtil::CheckpointBranchedTraversal(TToken token)
{
    TCheckpoint result(Tokenizer_);
    Push(token);
    result.Defer([this] () { CurrentPath_.Pop(); });
    return result;
}

NYPath::ETokenType TPathVisitorUtil::GetTokenizerType() const
{
    return Tokenizer_.GetType();
}

TStringBuf TPathVisitorUtil::GetTokenizerInput() const
{
    return Tokenizer_.GetInput();
}

TStringBuf TPathVisitorUtil::GetToken() const
{
    return Tokenizer_.GetToken();
}

const TString& TPathVisitorUtil::GetLiteralValue() const
{
    return Tokenizer_.GetLiteralValue();
}

const NYPath::TYPath& TPathVisitorUtil::GetCurrentPath() const
{
    return CurrentPath_.GetPath();
}

void TPathVisitorUtil::Expect(NYPath::ETokenType type) const
{
    if (Tokenizer_.GetType() == type) {
        return;
    }

    TError error(EErrorCode::MalformedPath,
        "Expected %Qlv but got %Qlv",
        type,
        Tokenizer_.GetType());

    if (Tokenizer_.GetPreviousType() == NYPath::ETokenType::Slash) {
        error <<= TErrorAttribute("note", "the path cannot normally end with a slash");
    }

    Throw(error);
}

bool TPathVisitorUtil::PathComplete() const
{
    return Tokenizer_.GetType() == NYPath::ETokenType::EndOfStream;
}

void TPathVisitorUtil::Reset(NYPath::TYPathBuf path)
{
    Tokenizer_.Reset(path);
    Tokenizer_.Advance();
    CurrentPath_.Reset();
    StopIteration_ = false;
}

TErrorOr<TIndexParseResult> TPathVisitorUtil::ParseCurrentListIndex(int size) const
{
    Expect(NYPath::ETokenType::Literal);
    auto indexParseResult = ParseListIndex(Tokenizer_.GetToken(), size);

    if (indexParseResult.IsOutOfBounds(size)) {
        return TError(EErrorCode::OutOfBounds,
            "Index %Qv out of bounds for repeated field of size %v",
            Tokenizer_.GetToken(),
            size);
    }

    return indexParseResult;
}

void TPathVisitorUtil::ThrowOnError(TError error) const
{
    if (!error.IsOK()) {
        Throw(std::move(error));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes

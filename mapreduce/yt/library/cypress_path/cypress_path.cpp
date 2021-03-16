#include "cypress_path.h"

#include <yt/yt/core/ypath/tokenizer.h>
#include <yt/yt/core/misc/error.h>

#include <util/stream/output.h>

namespace NYT {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TCypressPath::TCypressPath() noexcept
{ }

bool TCypressPath::CheckTokenMatch(size_t start, std::initializer_list<ETokenType> pattern) const noexcept
{
    if (start + pattern.size() > Tokens_.size()) {
        return false;
    }
    for (size_t i = 0; i < pattern.size(); ++i) {
        if (Tokens_[start + i].Type != pattern.begin()[i]) {
            return false;
        }
    }
    return true;
}

TCypressPath::TCypressPath(TString path)
    : Path_(std::move(path))
{
    if (Path_.empty()) {
        return;
    }

    auto tokenizer = TTokenizer(Path_);
    tokenizer.Advance();

    while (tokenizer.GetType() != ETokenType::EndOfStream)
    {
        if (tokenizer.GetType() == ETokenType::Range) {
            THROW_ERROR_EXCEPTION("Ranges are not allowed in TCypressPath: %v", Path_);
        }
        Tokens_.push_back({
            tokenizer.GetType(),
            TStringBuf(Path_.begin() + tokenizer.GetPrefix().size(), tokenizer.GetToken().size())});
        tokenizer.Advance();
    }

    int tokenIndex = 0;
    switch(Tokens_[0].Type) {
        case ETokenType::Literal:
            if (Path_[0] != '#') {
                THROW_ERROR_EXCEPTION("Invalid cypress path beginning: %v", Path_);
            }
            ++tokenIndex;
            break;
        case ETokenType::Slash:
            if (Tokens_.size() == 1 || (Tokens_.size() > 1 && Tokens_[1].Type == ETokenType::Slash)) {
                ++tokenIndex;
            }
            break;
        default:
            THROW_ERROR_EXCEPTION("Invalid cypress path beginning: %v", Path_);
    }

    int tokensSize = Tokens_.size();
    while (tokenIndex < tokensSize) {
        const auto& checkAndAdvance = [&](std::initializer_list<ETokenType> pattern) -> bool {
            if (CheckTokenMatch(tokenIndex, pattern)) {
                tokenIndex += pattern.size();
                return true;
            }
            return false;
        };

        if (checkAndAdvance({ETokenType::Slash, ETokenType::Literal, ETokenType::Ampersand})) {
            // Do nothing
        } else if (checkAndAdvance({ETokenType::Slash, ETokenType::At, ETokenType::Literal})) {
            // Do nothing
        } else if (checkAndAdvance({ETokenType::Slash, ETokenType::Literal})) {
            // Do nothing
        } else if (tokenIndex + 2 == tokensSize && checkAndAdvance({ETokenType::Slash, ETokenType::Asterisk})) {
            // Do nothing
        } else {
            size_t position = 0;
            for (int i = 0; i < tokenIndex; ++i) {
                position += Tokens_[i].Value.size();
            }
            THROW_ERROR_EXCEPTION("Invalid cypress path after position %v: %v", position, Path_);
        }
    }
}

TCypressPath::TCypressPath(TStringBuf path)
    : TCypressPath(TString(path))
{ }

TCypressPath::TCypressPath(const char* path)
    : TCypressPath(TString(path))
{ }

////////////////////////////////////////////////////////////////////////////////

TCypressPath& TCypressPath::operator/=(const TCypressPath& other)
{
    if (!other.IsRelative()) {
        THROW_ERROR_EXCEPTION("Right operand of / must be relative path: %v", other.GetPath());
    }
    Tokens_.insert(Tokens_.end(), other.Tokens_.begin(), other.Tokens_.end());
    Path_ += other.Path_;
    return *this;
}

TCypressPath& TCypressPath::operator/=(const TStringBuf& other)
{
    if (!other.empty() && other[0] != '/' && other[0] != '#') {
       return operator/=(TCypressPath(TString("/") + other));
    }
    return operator/=(TCypressPath(other));
}

TCypressPath& TCypressPath::operator/=(const TString& other)
{
    return operator/=(TStringBuf(other));
}

TCypressPath& TCypressPath::operator/=(const char* other)
{
    return operator/=(TStringBuf(other));
}

void TCypressPath::Validate(TStringBuf path)
{
    TCypressPath cypressPath(path);
}

bool TCypressPath::IsAbsolute() const noexcept
{
    return !IsRelative();
}

bool TCypressPath::IsRelative() const noexcept
{
    return
        Tokens_.empty()
        || (Tokens_.size() >= 2 && Tokens_[0].Type == ETokenType::Slash && Tokens_[1].Type != ETokenType::Slash);
}

size_t TCypressPath::GetBasenameSize() const noexcept
{
    size_t basenameSize = 0;
    for (auto it = Tokens_.rbegin(); it != Tokens_.rend(); ++it) {
        basenameSize += it->Value.size();
        if (it->Type == ETokenType::Slash) {
            break;
        }
    }
    return basenameSize;
}

TCypressPath TCypressPath::GetBasename() const noexcept
{
    return TStringBuf(Path_).Last(GetBasenameSize());
}

TCypressPath TCypressPath::GetParent() const noexcept
{
    if (Tokens_.size() <= 1) {
        return GetBasename();
    }
    return Path_.substr(0, Path_.size() - GetBasenameSize());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

template <>
void Out<NYT::TCypressPath>(IOutputStream& os, const NYT::TCypressPath& path) {
    os << path.GetPath();
}

////////////////////////////////////////////////////////////////////////////////

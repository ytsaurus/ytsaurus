#include "attribute_path.h"

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/yson/tokenizer.h>

#include <library/cpp/yt/error/error.h>

namespace NYT::NOrm::NAttributes {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

namespace {

void SkipCommonTokens(TTokenizer& lhs, TTokenizer& rhs)
{
    while (lhs.Advance() != ETokenType::EndOfStream && rhs.Advance() != ETokenType::EndOfStream) {
        lhs.Expect(ETokenType::Slash);
        rhs.Expect(ETokenType::Slash);

        lhs.Advance();
        rhs.Advance();

        lhs.Expect(ETokenType::Literal);
        rhs.Expect(ETokenType::Literal);

        if (lhs.GetLiteralValue() != rhs.GetLiteralValue()) {
            return;
        }
    }
}

void SkipCommonTokensWithPattern(TTokenizer& patternTokenizer, TTokenizer& pathTokenizer)
{
    while (true) {
        patternTokenizer.Advance();
        pathTokenizer.Advance();
        if (patternTokenizer.GetType() == ETokenType::EndOfStream ||
            pathTokenizer.GetType() == ETokenType::EndOfStream)
        {
            break;
        }

        patternTokenizer.Expect(ETokenType::Slash);
        pathTokenizer.Expect(ETokenType::Slash);

        auto patternToken = patternTokenizer.Advance();
        if (patternToken != ETokenType::Literal && patternToken != ETokenType::Asterisk) {
            THROW_ERROR_EXCEPTION("Expected %Qlv or %Qlv in pattern attribute path but found %Qlv",
                ETokenType::Literal,
                ETokenType::Asterisk,
                patternToken);
        }

        pathTokenizer.Advance();
        pathTokenizer.Expect(ETokenType::Literal);

        if (patternToken == ETokenType::Literal &&
            patternTokenizer.GetLiteralValue() != pathTokenizer.GetLiteralValue())
        {
            break;
        }
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool IsAttributePath(TStringBuf path)
{
    NYson::TTokenizer ysonTokenizer(path);
    ysonTokenizer.ParseNext();
    int maxAttributeDepth = 0;
    int attributeDepth = 0;
    while (true) {
        switch (ysonTokenizer.CurrentToken().GetType()) {
            case NYson::ETokenType::LeftAngle:
                ++attributeDepth;
                maxAttributeDepth = std::max(maxAttributeDepth, attributeDepth);
                break;
            case NYson::ETokenType::RightAngle:
                --attributeDepth;
                break;
            default:
                break;
        }

        if (attributeDepth == 0) {
            break;
        }

        if (!ysonTokenizer.ParseNext()) {
            return false;
        }
    }

    NYPath::TTokenizer tokenizer(
        maxAttributeDepth == 0
        ? path
        : ysonTokenizer.GetCurrentSuffix());
    tokenizer.Advance();
    while (tokenizer.GetType() != NYPath::ETokenType::EndOfStream) {
        if (tokenizer.GetType() != NYPath::ETokenType::Slash) {
            return false;
        }
        tokenizer.Advance();
        if (tokenizer.GetType() != NYPath::ETokenType::Literal) {
            return false;
        }
        tokenizer.Advance();
    }
    return true;
}

void ValidateAttributePath(TYPathBuf path)
{
    TTokenizer tokenizer(path);
    while (tokenizer.Advance() != ETokenType::EndOfStream) {
        tokenizer.Expect(ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(ETokenType::Literal);
    }
}

bool AreAttributesRelated(TYPathBuf lhs, TYPathBuf rhs)
{
    TTokenizer lhsTokenizer(lhs);
    TTokenizer rhsTokenizer(rhs);
    SkipCommonTokens(lhsTokenizer, rhsTokenizer);
    return lhsTokenizer.GetType() == ETokenType::EndOfStream ||
        rhsTokenizer.GetType() == ETokenType::EndOfStream;
}

EAttributePathMatchResult MatchAttributePathToPattern(NYPath::TYPathBuf pattern, NYPath::TYPathBuf path)
{
    TTokenizer patternTokenizer(pattern);
    TTokenizer pathTokenizer(path);
    SkipCommonTokensWithPattern(patternTokenizer, pathTokenizer);
    if (patternTokenizer.GetType() == ETokenType::EndOfStream) {
        return pathTokenizer.GetType() == ETokenType::EndOfStream
            ? EAttributePathMatchResult::Full
            : EAttributePathMatchResult::PatternIsPrefix;
    }
    return pathTokenizer.GetType() == ETokenType::EndOfStream
        ? EAttributePathMatchResult::PathIsPrefix
        : EAttributePathMatchResult::None;
}

TSplitResult GetAttributePathRoot(const NYPath::TYPath& path, int length)
{
    int partsFound = 0;
    NYPath::TTokenizer tokenizer(path);
    while (tokenizer.GetType() != NYPath::ETokenType::EndOfStream) {
        auto prev = tokenizer.GetType();
        auto next = tokenizer.Advance();
        if (prev == NYPath::ETokenType::Slash && next == NYPath::ETokenType::Literal) {
            auto back = tokenizer.GetSuffix();
            tokenizer.Advance();
            partsFound += 1;
            if (partsFound == length) {
                return TSplitResult{tokenizer.GetPrefix(), back};
            }
        } else if (prev != NYPath::ETokenType::StartOfStream && next != NYPath::ETokenType::Literal) {
            break;
        }
    }
    return TSplitResult{"", path};
}

// Split pattern by asterisk.
TSplitResult SplitPatternByAsterisk(const NYPath::TYPath& path)
{
    NYPath::TTokenizer tokenizer(path);
    while (tokenizer.GetType() != NYPath::ETokenType::EndOfStream) {
        auto prev = tokenizer.GetType();
        auto prevText = tokenizer.GetPrefix();
        auto next = tokenizer.Advance();
        if (prev == NYPath::ETokenType::Slash && next == NYPath::ETokenType::Asterisk) {
            return TSplitResult{prevText, tokenizer.GetSuffix()};
        }
    }
    return TSplitResult(path, std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes

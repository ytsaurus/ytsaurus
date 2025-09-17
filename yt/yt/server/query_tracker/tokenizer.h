#pragma once

#include "private.h"

#include <yt/yt/client/api/public.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

struct TParsedToken
{
    std::string Token;
    i64 Occurrences{0};
};

DEFINE_ENUM(ETokenizationMode,
    ((Standard)  (0))
    ((ForSearch) (1))
);

////////////////////////////////////////////////////////////////////////////////

struct IQueryTokenizer
    : public TRefCounted
{
    virtual std::vector<std::string> Tokenize(const std::string& query, ETokenizationMode tokenizationMode) = 0;
};

DEFINE_REFCOUNTED_TYPE(IQueryTokenizer)

////////////////////////////////////////////////////////////////////////////////

IQueryTokenizerPtr CreateRegexTokenizer();

////////////////////////////////////////////////////////////////////////////////

std::vector<TParsedToken> Tokenize(
    const std::string& query,
    IQueryTokenizerPtr tokenizer,
    ETokenizationMode tokenizationMode = ETokenizationMode::Standard);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker

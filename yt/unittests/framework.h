#pragma once

// Include Google Test and Google Mock headers.
#define GTEST_DONT_DEFINE_FAIL 1

#include <contrib/testing/gtest.h>
#include <contrib/testing/gmock.h>

// Custom includes to glue Google and YT stuff together.
#include <util/generic/stroka.h>
#include <util/generic/strbuf.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A tiny helper function to generate random file names.
Stroka GenerateRandomFileName(const char* prefix);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

namespace testing {

////////////////////////////////////////////////////////////////////////////////

//! Argument matcher for (const TStringBuf&).
//! Semantics: Match by equality.
template <>
class Matcher<const TStringBuf&>
    : public internal::MatcherBase<const TStringBuf&> {
public:
    Matcher() {}

    explicit Matcher(const MatcherInterface<const TStringBuf&>* impl)
        : internal::MatcherBase<const TStringBuf&>(impl)
    {}

    Matcher(const Stroka& s); // NOLINT
    Matcher(const char* s); // NOLINT
    Matcher(const TStringBuf& s); // NOLINT
};

//! Argument matcher for (const Stroka&).
//! Semantics: Match by equality.
template <>
class Matcher<const Stroka&>
    : public internal::MatcherBase<const Stroka&> {
public:
    Matcher() {}

    explicit Matcher(const MatcherInterface<const Stroka&>* impl)
        : internal::MatcherBase<const Stroka&>(impl)
    {}

    Matcher(const Stroka& s); // NOLINT
    Matcher(const char* s); // NOLINT
};

//! Argument matcher for (Stroka)
//! Semantics: Match by equality.
template <>
class Matcher<Stroka>
    : public internal::MatcherBase<Stroka> {
public:
    Matcher() {}

    explicit Matcher(const MatcherInterface<Stroka>* impl)
        : internal::MatcherBase<Stroka>(impl)
    {}

    Matcher(const Stroka& s); // NOLINT
    Matcher(const char* s);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace testing

void PrintTo(const Stroka& string, ::std::ostream* os);
void PrintTo(const TStringBuf& string, ::std::ostream* os);

////////////////////////////////////////////////////////////////////////////////

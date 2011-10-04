#pragma once

////////////////////////////////////////////////////////////////////////////////

// STLport does not have std::tr1::tuple, so we have to fallback to custom
// implementation.
#define GTEST_HAS_TR1_TUPLE 1
#define GTEST_USE_OWN_TR1_TUPLE 1

// Preconfigure all the namespaces; i. e. bind ::std to ::NStl
#include <util/private/stl/config.h>
#include <util/private/stl/stlport-5.1.4/stlport/stl/config/features.h>

namespace NStl {
    namespace tr1 {
    }
}

// Custom functions and classes for unit-testing.
// NB: Implementation of all custom functions and classes reside in utmain.cpp

#include <util/generic/stroka.h>

namespace NYT {

//! A tiny helper function to generate random file names.
Stroka GenerateRandomFileName(const char* prefix);
 
} // namespace NYT

#include "framework/gtest.h"
#include "framework/gmock.h"

namespace testing {

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

} // namespace testing


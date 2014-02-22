#pragma once

// Include Google Test and Google Mock headers.
#define GTEST_DONT_DEFINE_FAIL 1

#include <contrib/testing/gtest.h>
#include <contrib/testing/gmock.h>

// Custom includes to glue Google and YT stuff together.
#include <util/generic/stroka.h>
#include <util/generic/strbuf.h>

#include <yt/core/misc/preprocessor.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A tiny helper function to generate random file names.
Stroka GenerateRandomFileName(const char* prefix);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

#define MOCK_RPC_SERVICE_METHOD(ns, method) \
    DEFINE_RPC_SERVICE_METHOD_THUNK(ns, method) \
    MOCK_METHOD3(method, void (TReq##method*, TRsp##method*, TCtx##method##Ptr))

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TPredicateMatcher
    : public ::testing::MatcherInterface<T>
{
public:
    TPredicateMatcher(
        std::function<bool(T)> predicate,
        const char* description)
        : Predicate_(predicate)
        , Description_(description)
    { }

    virtual bool MatchAndExplain(T value, ::testing::MatchResultListener* listener) const override
    {
        return Predicate_(value);
    }

    virtual void DescribeTo(std::ostream* os) const override
    {
        *os << Description_;
    }

private:
    std::function<bool(T)> Predicate_;
    const char* Description_;

};

template <class T>
::testing::Matcher<T> MakePredicateMatcher(
    std::function<bool(T)> predicate,
    const char* description)
{
    return ::testing::MakeMatcher(new TPredicateMatcher<T>(predicate, description));
}

#define MAKE_PREDICATE_MATCHER(type, arg, capture, predicate) \
    MakePredicateMatcher<type>( \
        capture (type arg) { return (predicate); }, \
        #predicate \
    )

////////////////////////////////////////////////////////////////////////////////

#define RPC_MOCK_CALL(mock, method) \
    method( \
        ::testing::_, \
        ::testing::_, \
        ::testing::_)

#define RPC_MOCK_CALL_WITH_PREDICATE(mock, method, capture, predicate) \
    method( \
        MAKE_PREDICATE_MATCHER(mock::TReq##method*, request, capture, predicate), \
        ::testing::_, \
        ::testing::_)

#define EXPECT_CALL_WITH_MESSAGE_IMPL(obj, call, message) \
    ((obj).gmock_##call).InternalExpectedAt(__FILE__, __LINE__, #obj, message)

#define EXPECT_CALL_WITH_MESSAGE(obj, call, message) \
    EXPECT_CALL_WITH_MESSAGE_IMPL(obj, call, message)

#define EXPECT_RPC_CALL(mock, method) \
    EXPECT_CALL_WITH_MESSAGE( \
        mock, \
        RPC_MOCK_CALL(mock, method), \
        #method \
    )

#define EXPECT_RPC_CALL_WITH_PREDICATE(mock, method, capture, predicate) \
    EXPECT_CALL_WITH_MESSAGE( \
        mock, \
        RPC_MOCK_CALL_WITH_PREDICATE(mock, method, capture, predicate), \
        #method "(" #predicate ")" \
    )

////////////////////////////////////////////////////////////////////////////////

#define ON_CALL_WITH_MESSAGE_IMPL(obj, call, message) \
    ((obj).gmock_##call).InternalDefaultActionSetAt(__FILE__, __LINE__, #obj, message)

#define ON_CALL_WITH_MESSAGE(obj, call, message) \
    ON_CALL_WITH_MESSAGE_IMPL(obj, call, message)

#define ON_RPC_CALL(mock, method) \
    ON_CALL_WITH_MESSAGE( \
        mock, \
        RPC_MOCK_CALL(mock, method), \
        #method \
    )

#define ON_RPC_WITH_PREDICATE(mock, method, capture, predicate) \
    ON_CALL_WITH_MESSAGE( \
        mock, \
        RPC_MOCK_CALL3(mock, method, capture, predicate), \
        #method "(" #predicate ")" \
    )

////////////////////////////////////////////////////////////////////////////////

#define HANLDE_RPC_CALL(mockType, method, capture, body) \
    ::testing::Invoke(capture ( \
        mockType::TReq##method* request, \
        mockType::TRsp##method* response, \
        mockType::TCtx##method##Ptr context) \
    body)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
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

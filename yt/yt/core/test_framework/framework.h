#pragma once

#include <yt/core/actions/public.h>

#include <library/cpp/ytalloc/core/misc/preprocessor.h>

// Include Google Test and Google Mock headers.
#define GTEST_DONT_DEFINE_FAIL 1

#include <contrib/libs/gtest/include/gtest/gtest.h>
#include <contrib/libs/gmock/include/gmock/gmock.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A tiny helper function to generate random file names.
TString GenerateRandomFileName(const char* prefix);

////////////////////////////////////////////////////////////////////////////////

// NB. EXPECT_THROW_* are macros not functions so when failure occurres
// gtest framework points to source code of test not the source code
// of EXPECT_THROW_* function.
#define EXPECT_THROW_THAT(expr, matcher) \
    do { \
        try { \
            expr; \
            ADD_FAILURE() << "Expected exception to be thrown"; \
        } catch (const std::exception& ex) { \
            EXPECT_THAT(ex.what(), matcher); \
        } \
    } while (0)

#define EXPECT_THROW_WITH_SUBSTRING(expr, exceptionSubstring) \
    EXPECT_THROW_THAT(expr, testing::HasSubstr(exceptionSubstring))

////////////////////////////////////////////////////////////////////////////////

void WaitForPredicate(
    std::function<bool()> predicate,
    int iterationCount = 100,
    TDuration period = TDuration::Seconds(1));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

namespace NYT::NRpc {

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

    virtual bool MatchAndExplain(T value, ::testing::MatchResultListener* /*listener*/) const override
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

} // namespace NYT::NRpc

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

    Matcher(const TString& s); // NOLINT
    Matcher(const char* s); // NOLINT
    Matcher(TStringBuf s); // NOLINT
};

//! Argument matcher for (TStringBuf).
//! Semantics: Match by equality.
template <>
class Matcher<TStringBuf>
    : public internal::MatcherBase<TStringBuf> {
public:
    Matcher() {}

    explicit Matcher(const MatcherInterface<TStringBuf>* impl)
        : internal::MatcherBase<TStringBuf>(impl)
    {}

    explicit Matcher(const MatcherInterface<const TStringBuf&>* impl)
        : internal::MatcherBase<TStringBuf>(impl)
    {}

    Matcher(const TString& s); // NOLINT
    Matcher(const char* s); // NOLINT
    Matcher(TStringBuf s); // NOLINT
};

//! Argument matcher for (const TString&).
//! Semantics: Match by equality.
template <>
class Matcher<const TString&>
    : public internal::MatcherBase<const TString&> {
public:
    Matcher() {}

    explicit Matcher(const MatcherInterface<const TString&>* impl)
        : internal::MatcherBase<const TString&>(impl)
    {}

    Matcher(const TString& s); // NOLINT
    Matcher(const char* s); // NOLINT
};

//! Argument matcher for (TString)
//! Semantics: Match by equality.
template <>
class Matcher<TString>
    : public internal::MatcherBase<TString> {
public:
    Matcher() {}

    explicit Matcher(const MatcherInterface<TString>* impl)
        : internal::MatcherBase<TString>(impl)
    {}

    explicit Matcher(const MatcherInterface<const TString&>* impl)
        : internal::MatcherBase<TString>(impl)
    {}

    Matcher(const TString& s); // NOLINT
    Matcher(const char* s);
};

////////////////////////////////////////////////////////////////////////////////

void RunAndTrackFiber(NYT::TClosure closure);

// Wraps tests in an extra fiber and awaits termination. Adapted from `gtest.h`.
#define TEST_W_(test_case_name, test_name, parent_class, parent_id)\
class GTEST_TEST_CLASS_NAME_(test_case_name, test_name) : public parent_class {\
 public:\
  GTEST_TEST_CLASS_NAME_(test_case_name, test_name)() {}\
 private:\
  virtual void TestBody();\
  void TestInnerBody();\
  static ::testing::TestInfo* const test_info_ GTEST_ATTRIBUTE_UNUSED_;\
  GTEST_DISALLOW_COPY_AND_ASSIGN_(\
    GTEST_TEST_CLASS_NAME_(test_case_name, test_name));\
};\
\
::testing::TestInfo* const GTEST_TEST_CLASS_NAME_(test_case_name, test_name)\
  ::test_info_ =\
    ::testing::internal::MakeAndRegisterTestInfo(\
        #test_case_name, #test_name, nullptr, nullptr, \
        ::testing::internal::CodeLocation(__FILE__, __LINE__), \
        (parent_id), \
        parent_class::SetUpTestCase, \
        parent_class::TearDownTestCase, \
        new ::testing::internal::TestFactoryImpl<\
            GTEST_TEST_CLASS_NAME_(test_case_name, test_name)>);\
void GTEST_TEST_CLASS_NAME_(test_case_name, test_name)::TestBody() {\
  ::testing::RunAndTrackFiber(BIND(\
    &GTEST_TEST_CLASS_NAME_(test_case_name, test_name)::TestInnerBody,\
    this));\
}\
void GTEST_TEST_CLASS_NAME_(test_case_name, test_name)::TestInnerBody()
#define TEST_W(test_fixture, test_name)\
  TEST_W_(test_fixture, test_name, test_fixture, \
    ::testing::internal::GetTypeId<test_fixture>())

////////////////////////////////////////////////////////////////////////////////

} // namespace testing

void PrintTo(const TString& string, ::std::ostream* os);
void PrintTo(TStringBuf string, ::std::ostream* os);

#define FRAMEWORK_INL_H_
#include "framework-inl.h"
#undef FRAMEWORK_INL_H_

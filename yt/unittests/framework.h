#pragma once

// Include Google Test and Google Mock headers.
#define GTEST_DONT_DEFINE_FAIL 1

#include <contrib/testing/gtest.h>
#include <contrib/testing/gmock.h>

// Custom includes to glue Google and YT stuff together.
#include <util/generic/stroka.h>
#include <util/generic/strbuf.h>

#include <core/misc/preprocessor.h>
#include <core/misc/enum.h>

#include <core/actions/public.h>

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
        #test_case_name, #test_name, NULL, NULL, \
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

void PrintTo(const Stroka& string, ::std::ostream* os);
void PrintTo(const TStringBuf& string, ::std::ostream* os);


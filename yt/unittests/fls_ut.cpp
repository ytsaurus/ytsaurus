#include "stdafx.h"
#include "framework.h"

#include <core/concurrency/fls.h>

#include <exception>

namespace NYT {
namespace NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TMyValue
{
    static int CtorCalls;
    static int DtorCalls;

    T Value;

    static void Reset()
    {
        CtorCalls = 0;
        DtorCalls = 0;
    }

    TMyValue()
    {
        ++CtorCalls;
    }

    ~TMyValue()
    {
        ++DtorCalls;
    }
};

template <> int TMyValue<int>::CtorCalls = 0;
template <> int TMyValue<int>::DtorCalls = 0;

template <> int TMyValue<Stroka>::CtorCalls = 0;
template <> int TMyValue<Stroka>::DtorCalls = 0;


class TFlsTest
    : public ::testing::Test
{
public:
    TFlsTest()
    {
        TMyValue<int>::Reset();
        TMyValue<Stroka>::Reset();
    }
};

TFlsValue<TMyValue<int>> IntValue;
TFlsValue<TMyValue<Stroka>> StringValue;

TEST_F(TFlsTest, OneFiber)
{
    {
        auto fiber = New<TFiber>(BIND([] () {
            ASSERT_EQ(TMyValue<int>::CtorCalls, 0);
            IntValue->Value = 1;
            ASSERT_EQ(TMyValue<int>::CtorCalls, 1);
        }));

        fiber->Run();
        ASSERT_EQ(fiber->GetState(), EFiberState::Terminated);
    }

    ASSERT_EQ(TMyValue<int>::CtorCalls, 1);
    ASSERT_EQ(TMyValue<int>::DtorCalls, 1);
}

TEST_F(TFlsTest, TwoFibers)
{
    {
        auto fiber1 = New<TFiber>(BIND([] () {
            ASSERT_EQ(TMyValue<Stroka>::CtorCalls, 0);
            StringValue->Value = "fiber1";
            ASSERT_EQ(TMyValue<Stroka>::CtorCalls, 1);

            Yield();

            ASSERT_EQ(StringValue->Value, "fiber1");
        }));

        auto fiber2 = New<TFiber>(BIND([] () {
            ASSERT_EQ(TMyValue<Stroka>::CtorCalls, 1);
            StringValue->Value = "fiber2";
            ASSERT_EQ(TMyValue<Stroka>::CtorCalls, 2);

            Yield();

            ASSERT_EQ(StringValue->Value, "fiber2");
        }));

        fiber1->Run();
        ASSERT_EQ(fiber1->GetState(), EFiberState::Suspended);

        ASSERT_EQ(TMyValue<Stroka>::CtorCalls, 1);
        ASSERT_EQ(TMyValue<int>::DtorCalls, 0);

        fiber2->Run();
        ASSERT_EQ(fiber2->GetState(), EFiberState::Suspended);

        ASSERT_EQ(TMyValue<Stroka>::CtorCalls, 2);
        ASSERT_EQ(TMyValue<Stroka>::DtorCalls, 0);

        fiber1->Run();
        ASSERT_EQ(fiber1->GetState(), EFiberState::Terminated);

        ASSERT_EQ(TMyValue<Stroka>::CtorCalls, 2);
        ASSERT_EQ(TMyValue<Stroka>::DtorCalls, 0);

        fiber2->Run();
        ASSERT_EQ(fiber2->GetState(), EFiberState::Terminated);
    }

    ASSERT_EQ(TMyValue<Stroka>::CtorCalls, 2);
    ASSERT_EQ(TMyValue<Stroka>::DtorCalls, 2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NConcurrency
} // namespace NYT


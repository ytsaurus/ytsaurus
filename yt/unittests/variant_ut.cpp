#include "stdafx.h"
#include "framework.h"

#include <core/misc/variant.h>

#include <util/generic/noncopyable.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TVariantTest, Pod1)
{
    TVariant<int> v(123);

    EXPECT_TRUE(v.Is<int>());

    EXPECT_EQ(0, v.Tag());
    EXPECT_EQ(0, v.TagOf<int>());

    EXPECT_EQ(123, v.As<int>());
    EXPECT_EQ(123, *v.TryAs<int>());
}

TEST(TVariantTest, Pod2)
{
    TVariant<int, double> v(3.14);

    EXPECT_TRUE(v.Is<double>());
    EXPECT_FALSE(v.Is<int>());

    EXPECT_EQ(1, v.Tag());
    EXPECT_EQ(0, v.TagOf<int>());
    EXPECT_EQ(1, v.TagOf<double>());

    EXPECT_EQ(3.14, v.As<double>());
    EXPECT_EQ(3.14, *v.TryAs<double>());
    EXPECT_EQ(nullptr, v.TryAs<int>());
}

TEST(TVariantTest, NonPod1)
{
    TVariant<Stroka> v(Stroka("hello"));
    EXPECT_EQ("hello", v.As<Stroka>());
}

struct S
{
    static int CtorCalls;
    static int DtorCalls;
    static int CopyCtorCalls;
    static int MoveCtorCalls;

    static void Reset()
    {
        CtorCalls = 0;
        DtorCalls = 0;
        CopyCtorCalls = 0;
        MoveCtorCalls = 0;
    }

    int Value;

    explicit S(int value)
        : Value(value)
    {
        ++CtorCalls;
    }

    S(const S& other)
        : Value(other.Value)
    {
        ++CopyCtorCalls;
    }

    S(S&& other)
        : Value(other.Value)
    {
        other.Value = -1;
        ++MoveCtorCalls;
    }

    ~S()
    {
        ++DtorCalls;
    }
};

int S::CtorCalls;
int S::DtorCalls;
int S::CopyCtorCalls;
int S::MoveCtorCalls;

TEST(TVariantTest, NonPod2)
{
    S::Reset();
    {
        TVariant<Stroka, S> v(Stroka("hello"));
        EXPECT_EQ("hello", v.As<Stroka>());
    }
    EXPECT_EQ(0, S::CtorCalls);
    EXPECT_EQ(0, S::DtorCalls);
    EXPECT_EQ(0, S::CopyCtorCalls);
    EXPECT_EQ(0, S::MoveCtorCalls);
}

TEST(TVariantTest, ConstructCopy1)
{
    S::Reset();
    {
        S s(123);
        TVariant<Stroka, S> v(s);
        EXPECT_EQ(123, v.As<S>().Value);
        EXPECT_EQ(123, s.Value);
    }
    EXPECT_EQ(1, S::CtorCalls);
    EXPECT_EQ(2, S::DtorCalls);
    EXPECT_EQ(1, S::CopyCtorCalls);
    EXPECT_EQ(0, S::MoveCtorCalls);
}

TEST(TVariantTest, ConstructCopy2)
{
    S::Reset();
    {
        S s(123);
        TVariant<Stroka, S> v1(s);
        TVariant<Stroka, S> v2(v1);
        EXPECT_EQ(123, s.Value);
        EXPECT_EQ(123, v1.As<S>().Value);
        EXPECT_EQ(123, v2.As<S>().Value);
        EXPECT_EQ(123, s.Value);
    }
    EXPECT_EQ(1, S::CtorCalls);
    EXPECT_EQ(3, S::DtorCalls);
    EXPECT_EQ(2, S::CopyCtorCalls);
    EXPECT_EQ(0, S::MoveCtorCalls);
}

TEST(TVariantTest, ConstructMove1)
{
    S::Reset();
    {
        S s(123);
        TVariant<Stroka, S> v(std::move(s));
        EXPECT_EQ(123, v.As<S>().Value);
        EXPECT_EQ(-1, s.Value);
    }
    EXPECT_EQ(1, S::CtorCalls);
    EXPECT_EQ(2, S::DtorCalls);
    EXPECT_EQ(0, S::CopyCtorCalls);
    EXPECT_EQ(1, S::MoveCtorCalls);
}

TEST(TVariantTest, ConstructMove2)
{
    S::Reset();
    {
        S s(123);
        TVariant<Stroka, S> v1(std::move(s));
        TVariant<Stroka, S> v2(std::move(v1));
        EXPECT_EQ(-1, v1.As<S>().Value);
        EXPECT_EQ(123, v2.As<S>().Value);
        EXPECT_EQ(-1, s.Value);
    }
    EXPECT_EQ(1, S::CtorCalls);
    EXPECT_EQ(3, S::DtorCalls);
    EXPECT_EQ(0, S::CopyCtorCalls);
    EXPECT_EQ(2, S::MoveCtorCalls);
}

TEST(TVariantTest, Move)
{
    S::Reset();
    {
        S s(123);
        TVariant<Stroka, S> v1(s);
        EXPECT_EQ(123, v1.As<S>().Value);

        TVariant<Stroka, S> v2(std::move(v1));
        EXPECT_EQ(-1, v1.As<S>().Value);
        EXPECT_EQ(123, v2.As<S>().Value);
        EXPECT_EQ(123, s.Value);
    }
    EXPECT_EQ(1, S::CtorCalls);
    EXPECT_EQ(3, S::DtorCalls);
    EXPECT_EQ(1, S::CopyCtorCalls);
    EXPECT_EQ(1, S::MoveCtorCalls);
}

TEST(TVariantTest, AssignCopy)
{
    S::Reset();
    {
        S s(123);
        TVariant<Stroka, S> v1(s);
        TVariant<Stroka, S> v2(Stroka("hello"));
        v2 = v1;

        EXPECT_EQ(123, v1.As<S>().Value);
        EXPECT_EQ(123, v2.As<S>().Value);
        EXPECT_EQ(123, s.Value);
    }
    EXPECT_EQ(1, S::CtorCalls);
    EXPECT_EQ(3, S::DtorCalls);
    EXPECT_EQ(2, S::CopyCtorCalls);
    EXPECT_EQ(0, S::MoveCtorCalls);
}

TEST(TVariantTest, MoveCopy)
{
    S::Reset();
    {
        S s(123);
        TVariant<Stroka, S> v1(s);
        TVariant<Stroka, S> v2(Stroka("hello"));
        v2 = std::move(v1);

        EXPECT_EQ(-1, v1.As<S>().Value);
        EXPECT_EQ(123, v2.As<S>().Value);
        EXPECT_EQ(123, s.Value);
    }
    EXPECT_EQ(1, S::CtorCalls);
    EXPECT_EQ(3, S::DtorCalls);
    EXPECT_EQ(1, S::CopyCtorCalls);
    EXPECT_EQ(1, S::MoveCtorCalls);
}

class TNonCopyable1
    : private TNonCopyable
{
public:
    TNonCopyable1()
    { }
};

class TNonCopyable2
    : private TNonCopyable
{
public:
    TNonCopyable2()
    { }
};

TEST(TVariantTest, Inplace)
{
    TVariant<TNonCopyable1, TNonCopyable2> v1{TVariantTypeTag<TNonCopyable1>()};
    EXPECT_TRUE(v1.Is<TNonCopyable1>());
    EXPECT_FALSE(v1.Is<TNonCopyable2>());

    TVariant<TNonCopyable1, TNonCopyable2> v2{TVariantTypeTag<TNonCopyable2>()};
    EXPECT_FALSE(v2.Is<TNonCopyable1>());
    EXPECT_TRUE(v2.Is<TNonCopyable2>());
}


////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

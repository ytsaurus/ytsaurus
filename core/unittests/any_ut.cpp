#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/any.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TAnyTest, Null)
{
    TAny a;
    EXPECT_FALSE(a);
}

TEST(TAnyTest, NotNull)
{
    TAny a(1);
    EXPECT_TRUE(a);
}

TEST(TAnyTest, Pod1)
{
    TAny v(123);

    EXPECT_TRUE(v.Is<int>());

    EXPECT_EQ(123, v.As<int>());
    EXPECT_EQ(123, *v.TryAs<int>());
}

TEST(TAnyTest, Pod2)
{
    TAny v(3.14);

    EXPECT_TRUE(v.Is<double>());
    EXPECT_FALSE(v.Is<int>());

    EXPECT_EQ(3.14, v.As<double>());
    EXPECT_EQ(3.14, *v.TryAs<double>());
    EXPECT_EQ(nullptr, v.TryAs<int>());
}

TEST(TAnyTest, NonPod1)
{
    TAny v(TString("hello"));
    EXPECT_EQ("hello", v.As<TString>());
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

TEST(TAnyTest, NonPod2)
{
    S::Reset();
    {
        TAny v(TString("hello"));
        EXPECT_EQ("hello", v.As<TString>());
    }
    EXPECT_EQ(0, S::CtorCalls);
    EXPECT_EQ(0, S::DtorCalls);
    EXPECT_EQ(0, S::CopyCtorCalls);
    EXPECT_EQ(0, S::MoveCtorCalls);
}

TEST(TAnyTest, ConstructCopy1)
{
    S::Reset();
    {
        S s(123);
        TAny v(s);
        EXPECT_EQ(123, v.As<S>().Value);
        EXPECT_EQ(123, s.Value);
    }
    EXPECT_EQ(1, S::CtorCalls);
    EXPECT_EQ(2, S::DtorCalls);
    EXPECT_EQ(1, S::CopyCtorCalls);
    EXPECT_EQ(0, S::MoveCtorCalls);
}

TEST(TAnyTest, ConstructCopy2)
{
    S::Reset();
    {
        S s(123);
        TAny v1(s);
        TAny v2(v1);
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

TEST(TAnyTest, ConstructMove1)
{
    S::Reset();
    {
        S s(123);
        TAny v(std::move(s));
        EXPECT_EQ(123, v.As<S>().Value);
        EXPECT_EQ(-1, s.Value);
    }
    EXPECT_EQ(1, S::CtorCalls);
    EXPECT_EQ(2, S::DtorCalls);
    EXPECT_EQ(0, S::CopyCtorCalls);
    EXPECT_EQ(1, S::MoveCtorCalls);
}

TEST(TAnyTest, ConstructMove2)
{
    S::Reset();
    {
        S s(123);
        TAny v1(std::move(s));
        TAny v2(std::move(v1));
        EXPECT_FALSE(v1);
        EXPECT_EQ(123, v2.As<S>().Value);
        EXPECT_EQ(-1, s.Value);
    }
    EXPECT_EQ(1, S::CtorCalls);
    EXPECT_EQ(2, S::DtorCalls);
    EXPECT_EQ(0, S::CopyCtorCalls);
    EXPECT_EQ(1, S::MoveCtorCalls);
}

TEST(TAnyTest, Move)
{
    S::Reset();
    {
        S s(123);
        TAny v1(s);
        EXPECT_EQ(123, v1.As<S>().Value);

        TAny v2(std::move(v1));
        EXPECT_FALSE(v1);
        EXPECT_EQ(123, v2.As<S>().Value);
        EXPECT_EQ(123, s.Value);
    }
    EXPECT_EQ(1, S::CtorCalls);
    EXPECT_EQ(2, S::DtorCalls);
    EXPECT_EQ(1, S::CopyCtorCalls);
    EXPECT_EQ(0, S::MoveCtorCalls);
}

TEST(TAnyTest, AssignCopy)
{
    S::Reset();
    {
        S s(123);
        TAny v1(s);
        TAny v2(TString("hello"));
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

TEST(TAnyTest, MoveCopy)
{
    S::Reset();
    {
        S s(123);
        TAny v1(s);
        TAny v2(TString("hello"));
        v2 = std::move(v1);

        EXPECT_FALSE(v1);
        EXPECT_EQ(123, v2.As<S>().Value);
        EXPECT_EQ(123, s.Value);
    }
    EXPECT_EQ(1, S::CtorCalls);
    EXPECT_EQ(2, S::DtorCalls);
    EXPECT_EQ(1, S::CopyCtorCalls);
    EXPECT_EQ(0, S::MoveCtorCalls);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

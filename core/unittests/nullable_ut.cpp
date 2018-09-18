#include <yt/core/test_framework/framework.h>
#include <yt/core/test_framework/probe.h>

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/property.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

#ifndef NDEBUG
TEST(TNullableDeathTest, DISABLED_Uninitialized)
{
    TNullable<int> nullable;
    EXPECT_FALSE(nullable.HasValue());
    ASSERT_DEATH(nullable.Get(), ".*");
}
#endif

template <class T>
inline void TestNullable(
    const TNullable<T>& nullable,
    bool hasValue,
    T expectedValue = T())
{
    if (hasValue) {
        EXPECT_TRUE(nullable.HasValue());
        EXPECT_EQ(expectedValue, nullable.Get());
    } else {
        EXPECT_FALSE(nullable.HasValue());
    }
}

TEST(TNullableTest, EmptyConstruct)
{
    TNullable<int> nullable1;
    EXPECT_FALSE(nullable1.HasValue());
    TNullable<int> nullable2(nullable1);
    EXPECT_FALSE(nullable1.HasValue());
    EXPECT_FALSE(nullable2.HasValue());
    TNullable<int> nullable3(std::move(nullable1));
    EXPECT_FALSE(nullable1.HasValue());
    EXPECT_FALSE(nullable2.HasValue());
    EXPECT_FALSE(nullable3.HasValue());
}

TEST(TNullableTest, ValueConstruct)
{
    {
        TProbeState state;
        TProbe probe(&state);
        TProbeScoper scope(&state);

        TNullable<TProbe> nullable(probe);
        EXPECT_TRUE(nullable.HasValue());
        EXPECT_EQ(0, state.Constructors);
        EXPECT_EQ(1, state.CopyConstructors);
        EXPECT_THAT(state, IsAlive());
        EXPECT_THAT(state, HasCopyMoveCounts(1, 0));
    }
    {
        TProbeState state;
        TProbe probe(&state);
        TProbeScoper scope(&state);

        TNullable<TProbe> nullable(std::move(probe));
        EXPECT_TRUE(nullable.HasValue());
        EXPECT_EQ(0, state.Constructors);
        EXPECT_EQ(1, state.MoveConstructors);
        EXPECT_EQ(0, state.ShadowDestructors);
        EXPECT_THAT(state, HasCopyMoveCounts(0, 1));
    }
}

TEST(TNullableTest, NullableConstruct)
{
    {
        TProbeState state;
        TNullable<TProbe> probe; probe.Emplace(&state);
        TProbeScoper scoper(&state);
        EXPECT_TRUE(probe.HasValue());

        TNullable<TProbe> nullable(probe);
        EXPECT_TRUE(nullable.HasValue());
        EXPECT_TRUE(probe.HasValue());
        EXPECT_EQ(0, state.Constructors);
        EXPECT_GE(1, state.CopyConstructors);
        EXPECT_THAT(state, NoMoves());
        EXPECT_THAT(state, NoAssignments());
    }
    {
        TProbeState state;
        TNullable<TProbe> probe; probe.Emplace(&state);
        TProbeScoper scoper(&state);
        EXPECT_TRUE(probe.HasValue());

        TNullable<TProbe> nullable(std::move(probe));
        EXPECT_TRUE(nullable.HasValue());
        EXPECT_FALSE(probe.HasValue());
        EXPECT_EQ(0, state.Constructors);
        EXPECT_GE(1, state.MoveConstructors);
        EXPECT_GE(1, state.ShadowDestructors);
        EXPECT_THAT(state, NoCopies());
        EXPECT_THAT(state, NoAssignments());
    }
}

TEST(TNullableTest, Construct)
{
    TestNullable(TNullable<int>(), false);
    TestNullable(TNullable<int>(1), true, 1);
    TestNullable(TNullable<int>(true, 1), true, 1);
    TestNullable(TNullable<int>(false, 1), false);
    TestNullable(TNullable<int>(TNullable<double>()), false);
    TestNullable(TNullable<int>(TNullable<double>(1.1)), true, 1);
}

TEST(TNullableTest, AssignReset)
{
    TNullable<int> nullable;

    TestNullable(nullable, false);
    TestNullable(nullable = 1, true, 1);
    TestNullable(nullable = TNullable<double>(), false);
    TestNullable(nullable = TNullable<double>(1.1), true, 1);

    nullable.Reset();

    TestNullable(nullable, false);
}

TEST(TNullableTest, Emplace)
{
    TNullable<TProbe> nullable;
    EXPECT_FALSE(nullable.HasValue());

    TProbeState state;

    nullable.Emplace(&state);
    EXPECT_TRUE(nullable.HasValue());
    EXPECT_EQ(1, state.Constructors);
    EXPECT_EQ(0, state.Destructors);
    EXPECT_THAT(state, NoCopies());
    EXPECT_THAT(state, NoMoves());

    nullable.Reset();
    EXPECT_FALSE(nullable.HasValue());
    EXPECT_EQ(1, state.Constructors);
    EXPECT_EQ(1, state.Destructors);
    EXPECT_THAT(state, NoCopies());
    EXPECT_THAT(state, NoMoves());
}

inline void TestSwap(TNullable<int> nullable1, TNullable<int> nullable2)
{
    bool initialized1 = nullable1.HasValue();
    int value1 = initialized1 ? *nullable1 : 0;
    bool initialized2 = nullable2.HasValue();
    int value2 = initialized2 ? *nullable2 : 0;

    nullable1.Swap(nullable2);

    TestNullable(nullable1, initialized2, value2);
    TestNullable(nullable2, initialized1, value1);
}

TEST(TNullableTest, Swap)
{
    TestSwap(TNullable<int>(1), TNullable<int>(2));
    TestSwap(TNullable<int>(1), TNullable<int>());
    TestSwap(TNullable<int>(), TNullable<int>(2));
    TestSwap(TNullable<int>(), TNullable<int>());
}

TEST(TNullableTest, Get)
{
    EXPECT_EQ(2, TNullable<int>(2).Get());
}

TEST(TNullableTest, GetWithDefault)
{
    EXPECT_EQ(2, TNullable<int>(2).Get(1));
    EXPECT_EQ(1, TNullable<int>().Get(1));
}

TEST(TNullableTest, MakeNullable)
{
    TestNullable(MakeNullable(1), true, 1);
    TestNullable(MakeNullable(true, 1), true, 1);
    TestNullable(MakeNullable(false, 1), false);
}

TEST(TNullableTest, Null)
{
    {
        TNullable<int> nullable = Null;
        TestNullable(nullable, false);
    }
    {
        TNullable<int> nullable(Null);
        TestNullable(nullable, false);
    }
}

TEST(TNullableTest, Operators)
{
    {
        TNullable<std::vector<int>> nullable(std::vector<int>(1));
        EXPECT_EQ(1, (*nullable).size());
        EXPECT_EQ(1, nullable->size());
        EXPECT_EQ(&nullable.Get(), nullable.GetPtr());

        EXPECT_TRUE(bool(nullable));
        nullable.Reset();
        EXPECT_FALSE(bool(nullable));
    }
    {
        EXPECT_EQ(TNullable<int>( ), TNullable<int>( ));
        EXPECT_EQ(TNullable<int>(1), TNullable<int>(1));
        EXPECT_EQ(TNullable<int>(1), 1);
        EXPECT_EQ(1, TNullable<int>(1));

        EXPECT_NE(TNullable<int>(1), TNullable<int>( ));
        EXPECT_NE(TNullable<int>( ), TNullable<int>(2));
        EXPECT_NE(TNullable<int>(1), TNullable<int>(2));
        EXPECT_NE(TNullable<int>(1), 2);
        EXPECT_NE(1, TNullable<int>(2));
    }
}

TEST(TNullableTest, ToString)
{
    EXPECT_EQ(ToString(MakeNullable(1)), "1");
    EXPECT_EQ(ToString(TNullable<int>()), "<Null>");
}

TEST(TNullableTest, Destructor)
{
    int counter = 0;

    class TestClass
    {
    public:
        TestClass(int& counter)
            : Counter_(counter)
        {
            Counter_++;
        }

        TestClass(const TestClass& other)
            : Counter_(other.Counter_)
        {
            Counter_++;
        }

        ~TestClass()
        {
            Counter_--;
        }

    private:
        int& Counter_;
    };

    EXPECT_EQ(0, counter);
    {
        TNullable<TestClass> obj(counter);
        EXPECT_EQ(1, counter);
    }
    EXPECT_EQ(0, counter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT


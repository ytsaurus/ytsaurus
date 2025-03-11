#include <yt/cpp/roren/library/bind/bind.h>

#include <yt/cpp/roren/library/unordered_invoker/unordered_invoker.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NRoren;
using NRoren::NPrivate::InvokeUnordered;

TEST(BindBack, Simple)
{
    // Lambda.
    ASSERT_EQ(BindBack([] (int x) { return x; }, 42)(), 42);
    // Function pointer.
    ASSERT_EQ(BindBack(+[] (int x) { return x; }, 42)(), 42);

    // Void return.
    {
        static int x = 0;
        BindBack([] (int y) -> void { x = y; }, 42)();
        ASSERT_EQ(x, 42);
    }

    // Multiple bindings.
    ASSERT_EQ(BindBack([] (int x, int y) { return x + y; }, 42)(1), 43);
    ASSERT_EQ(BindBack([] (int x, int y) { return x + y; }, 42, 1)(), 43);
    ASSERT_EQ(BindBack(BindBack([] (int x, int y) { return x + y; }, 42), 1)(), 43);
}

class TConstructorCounted
{
public:
    TConstructorCounted() = default;

    TConstructorCounted(const TConstructorCounted& other)
        : CopiesCount_(other.CopiesCount_ + 1)
        , MovesCount_(other.MovesCount_)
    { }

    TConstructorCounted(TConstructorCounted&& other)
        : CopiesCount_(other.CopiesCount_)
        , MovesCount_(other.MovesCount_ + 1)
    { }

    TConstructorCounted& operator=(const TConstructorCounted&) = delete;
    TConstructorCounted& operator=(TConstructorCounted&&) = delete;

    auto GetCounters() const
    {
        return std::pair(CopiesCount_, MovesCount_);
    }

    Y_SAVELOAD_DEFINE(CopiesCount_, MovesCount_);

private:
    int CopiesCount_ = 0;
    int MovesCount_ = 0;
};

auto CallCounted(TConstructorCounted& ref, TConstructorCounted&& rref, TConstructorCounted value, const TConstructorCounted& cref)
{
    return std::tuple(ref.GetCounters(), rref.GetCounters(), value.GetCounters(), cref.GetCounters());
}

TEST(BindBack, PerfectForwarding)
{
    using namespace std::placeholders;
    TConstructorCounted counted;

    ASSERT_EQ(
        BindBack(CallCounted, TConstructorCounted{})(counted, TConstructorCounted{}, TConstructorCounted{}),
        std::bind(CallCounted, _1, _2, _3, TConstructorCounted{})(counted, TConstructorCounted{}, TConstructorCounted{}));

    ASSERT_EQ(
        BindBack(CallCounted, TConstructorCounted{}, TConstructorCounted{})(counted, TConstructorCounted{}),
        std::bind(CallCounted, _1, _2, TConstructorCounted{}, TConstructorCounted{})(counted, TConstructorCounted{}));

    ASSERT_EQ(
        BindBack(BindBack(CallCounted, TConstructorCounted{}), TConstructorCounted{})(counted, TConstructorCounted{}),
        std::bind(std::bind(CallCounted, _1, _2, _3, TConstructorCounted{}), _1, _2, TConstructorCounted{})(counted, TConstructorCounted{}));
}

TEST(BindBack, InvokeUnordered)
{
    auto foo = [] (TConstructorCounted counted, int x, double y, char z) {
        return std::tuple(counted.GetCounters(), x, y, z);
    };
    const auto expectedResult = std::invoke(foo, TConstructorCounted{}, 42, 1000.0, 'a');

    ASSERT_EQ(InvokeUnordered(foo, 'a', TConstructorCounted{}, 42, 1000.0), expectedResult);

    ASSERT_EQ(InvokeUnordered(BindBack(foo, 'a'), TConstructorCounted{}, 42, 1000.0), expectedResult);
    ASSERT_EQ(InvokeUnordered(BindBack(foo, 'a'), TConstructorCounted{}, 1000.0, 42), expectedResult);
    ASSERT_EQ(InvokeUnordered(BindBack(foo, 'a'), 42, TConstructorCounted{}, 1000.0), expectedResult);
    ASSERT_EQ(InvokeUnordered(BindBack(foo, 'a'), 1000.0, TConstructorCounted{}, 42), expectedResult);
    ASSERT_EQ(InvokeUnordered(BindBack(foo, 'a'), 42, 1000.0, TConstructorCounted{}), expectedResult);
    ASSERT_EQ(InvokeUnordered(BindBack(foo, 'a'), 1000.0, 42, TConstructorCounted{}), expectedResult);

    ASSERT_EQ(InvokeUnordered(BindBack(foo, 1000.0, 'a'), TConstructorCounted{}, 42), expectedResult);
    ASSERT_EQ(InvokeUnordered(BindBack(foo, 1000.0, 'a'), 42, TConstructorCounted{}), expectedResult);

    {
        TConstructorCounted x;
        TConstructorCounted y(std::move(x));
        ASSERT_EQ(
            InvokeUnordered(BindBack(foo, TConstructorCounted{}, 42, 1000.0, 'a')),
            std::invoke(foo, y, 42, 1000.0, 'a'));
    }
}

#include <library/cpp/containers/bitset/bitset.h>

#include <util/generic/vector.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace {

template <typename T>
class TBitSetTest
    : public testing::Test
{
};

using TIntegerTypes = ::testing::Types<i32, ui32>;

template <typename T>
void TestOpOr(std::initializer_list<T> a, std::initializer_list<T> b)
{
    TBitSet<T> seta{a};
    TBitSet<T> setb{b};

    TBitSet<T> m = seta | setb;
    for (auto i : a) {
        EXPECT_TRUE(m.contains(i));
    }

    for (auto i : b) {
        EXPECT_TRUE(m.contains(i));
    }
}

template <typename T>
void TestOpAnd(std::initializer_list<T> a, std::initializer_list<T> b, std::initializer_list<T> expected)
{
    TBitSet<T> seta{a};
    TBitSet<T> setb{b};

    TBitSet<T> result = seta & setb;
    EXPECT_EQ(result.size(), expected.size());
    for (auto i : expected) {
        EXPECT_TRUE(result.contains(i));
    }
}

template <class T>
void TestOpXor(std::initializer_list<T> a, std::initializer_list<T> b, std::initializer_list<T> expected)
{
    TBitSet<T> seta{a};
    TBitSet<T> setb{b};

    TBitSet<T> result = seta ^ setb;
    EXPECT_EQ(expected.size(), result.size());
    for (auto i : expected) {
        EXPECT_TRUE(result.contains(i));
    }

    TBitSet<T> setx = seta ^ seta;
    EXPECT_TRUE(setx.empty());
}

} // namespace

TYPED_TEST_SUITE(TBitSetTest, TIntegerTypes);

TYPED_TEST(TBitSetTest, TestReserve)
{
    TBitSet<TypeParam> set(100);
    EXPECT_TRUE(set.empty());
    EXPECT_GE(set.capacity(), 100u);
    EXPECT_EQ(0u, set.size());

    size_t initialCapacity = set.capacity();
    for (TypeParam i = 0; i < 100; ++i) {
        set.insert(i);
    }
    EXPECT_EQ(initialCapacity, set.capacity());
}

TYPED_TEST(TBitSetTest, TestInit)
{
    TBitSet<TypeParam> set1({1, 2, 3, 4, 5, 4, 3, 2, 1, 0});
    TVector<TypeParam> vec({1, 2, 3, 4, 5, 4, 3, 2, 1, 0});

    TBitSet<TypeParam> set2(vec.begin(), vec.end());

    EXPECT_TRUE(set1);
    EXPECT_TRUE(set2);

    EXPECT_EQ(6u, set1.size());
    EXPECT_EQ(6u, set2.size());
    for (TypeParam i : {0, 1, 2, 3, 4, 5}) {
        EXPECT_TRUE(set1.contains(i));
        EXPECT_TRUE(set2.contains(i));
    }
}

TYPED_TEST(TBitSetTest, TestInsert)
{
    TBitSet<TypeParam> set;
    {
        auto [it, ok] = set.insert(1);
        EXPECT_EQ(1, *it);
        EXPECT_TRUE(ok);
    }
    {
        auto [it, ok] = set.insert(1);
        EXPECT_EQ(1, *it);
        EXPECT_TRUE(!ok);
    }

    set.insert({1, 2, 3, 4, 4, 4});
    EXPECT_EQ(4u, set.size());

    TVector<TypeParam> vec({1, 2, 3, 4, 5, 4, 3, 2, 1, 0});
    set.insert(vec.begin(), vec.end());
    EXPECT_EQ(6u, set.size());
    for (TypeParam i : {0, 1, 2, 3, 4, 5}) {
        EXPECT_TRUE(set.contains(i));
    }

    if constexpr (std::is_signed_v<TypeParam>) {
        set.insert({-100, -10, 1000});
        for (const i32 i : {-100, -10, 1000}) {
            EXPECT_TRUE(set.contains(i));
        }
    } else {
        set.emplace(2600468496u);
        set.emplace(540697973u);
        EXPECT_TRUE(set.contains(2600468496));
        EXPECT_TRUE(set.contains(540697973));
    }
}

TYPED_TEST(TBitSetTest, TestErase)
{
    TBitSet<TypeParam> set{1, 2, 3, 4, 5, 6};
    EXPECT_EQ(6u, set.size());

    set.erase(1);
    EXPECT_TRUE(!set.contains(1));
    EXPECT_EQ(5u, set.size());
    set.erase(1);
    EXPECT_EQ(5u, set.size());

    set.erase(set.find(6));
    EXPECT_TRUE(!set.contains(6));
    EXPECT_TRUE(set.end() == set.find(6));
    EXPECT_EQ(4u, set.size());

    set.erase(set.find(2), set.find(4));
    EXPECT_TRUE(!set.contains(2));
    EXPECT_TRUE(!set.contains(3));
    EXPECT_TRUE(set.contains(4));
    EXPECT_TRUE(set.contains(5));
    EXPECT_EQ(2u, set.size());

    set.erase(set.begin(), set.end());
    EXPECT_TRUE(!set.contains(4));
    EXPECT_TRUE(!set.contains(5));
    EXPECT_EQ(0u, set.size());

    if constexpr (std::is_signed_v<TypeParam>) {
        TBitSet<TypeParam> set2{-100, -10, 1000};
        for (TypeParam i : {-100, -10, 1000}) {
            set2.erase(i);
            EXPECT_FALSE(set.contains(i));
            set2.erase(i);
        }
    }
}

TYPED_TEST(TBitSetTest, TestFind)
{
    TBitSet<TypeParam> set{1, 2, 3, 4, 5, 6};
    auto it = set.find(4);
    EXPECT_TRUE(set.end() != it);
    EXPECT_EQ(TypeParam{4}, *it);
    EXPECT_TRUE(set.end() != ++it);
    EXPECT_EQ(TypeParam{5}, *it);

    EXPECT_TRUE(set.end() == set.find(7));
    EXPECT_TRUE(set.end() == set.find(0));
}

TYPED_TEST(TBitSetTest, TestBitwiseOr)
{
    if constexpr (std::is_signed_v<TypeParam>) {
        TestOpOr<TypeParam>({-1, -2, -3}, {-4, 5, 6});
    } else {
        TestOpOr<TypeParam>({1, 2, 3}, {4, 5, 6});
    }
}

TYPED_TEST(TBitSetTest, TestBitwiseAnd)
{
    if constexpr (std::is_signed_v<TypeParam>) {
        TestOpAnd<TypeParam>({-1, -2, -3}, {-1, 2, 3}, {-1});
    } else {
        TestOpAnd<TypeParam>({1, 2, 3}, {1, 4, 5}, {1});
    }
}

TYPED_TEST(TBitSetTest, TestBitwiseXor)
{
    if constexpr (std::is_signed_v<TypeParam>) {
        TestOpXor<TypeParam>({-1, -2, -3}, {-1, 2, 3}, {-2, -3, 2, 3});
    } else {
        TestOpXor<TypeParam>({1, 2, 3}, {1, 4, 5}, {2, 3, 4, 5});
    }
}

#include <gtest/gtest.h>

#include <yt/yt/library/di/object_id.h>

#include <yt/yt/core/misc/enum.h>

namespace NYT::NDI {
namespace {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETestEnum,
    (Foo)
    (Bar)
);

DEFINE_ENUM(ESecondTestEnum,
    (Foo)
);

namespace NInner {

DEFINE_ENUM(ETestEnum,
    (Foo)
);

} // namespace NInner

TEST(TEnumValueIndex, Get)
{
    auto fooIndex = TEnumValueIndex::Get(ETestEnum::Foo);
    ASSERT_EQ(fooIndex, fooIndex);

    auto barIndex = TEnumValueIndex::Get(ETestEnum::Bar);
    ASSERT_NE(fooIndex, barIndex);

    auto secondFooIndex = TEnumValueIndex::Get(ESecondTestEnum::Foo);
    ASSERT_NE(fooIndex, secondFooIndex);

    auto thirdFooIndex = TEnumValueIndex::Get(NInner::ETestEnum::Foo);
    ASSERT_NE(fooIndex, thirdFooIndex);
}

TEST(TEnumValueIndex, Format)
{
    auto fooIndex = TEnumValueIndex::Get(ETestEnum::Foo);
    ASSERT_EQ(ToString(fooIndex), "ETestEnum::Foo");

    auto barIndex = TEnumValueIndex::Get(ETestEnum::Bar);
    ASSERT_EQ(ToString(barIndex), "ETestEnum::Bar");
}

TEST(TEnumValueIndex, HashKey)
{
    auto fooIndex = TEnumValueIndex::Get(ETestEnum::Foo);
    auto barIndex = TEnumValueIndex::Get(ETestEnum::Bar);

    THashMap<TEnumValueIndex, int> map;

    map[fooIndex] = 1;
    map[barIndex] = 2;

    ASSERT_EQ(map[fooIndex], 1);
}

////////////////////////////////////////////////////////////////////////////////

struct TFoo {};

struct TBar {};

TEST(TObjectId, Get)
{
    auto fooId = TObjectId::Get<TFoo>();
    ASSERT_EQ(fooId, fooId);

    auto barId = TObjectId::Get<TBar>();
    ASSERT_NE(fooId, barId);

    auto fooKindId = TObjectId::Get<TBar>(ETestEnum::Foo);
    ASSERT_NE(fooId, fooKindId);
    ASSERT_EQ(fooKindId, fooKindId);

    auto fooNamedId = TObjectId::GetNamed<TBar>("Named");
    ASSERT_NE(fooId, fooNamedId);
    ASSERT_EQ(fooNamedId, fooNamedId);
}

TEST(TObjectId, Format)
{
    auto fooId = TObjectId::Get<TFoo>();
    ASSERT_EQ(ToString(fooId), "NYT::NDI::(anonymous namespace)::TFoo");

    auto fooKindId = TObjectId::Get<TFoo>(ETestEnum::Foo);
    ASSERT_EQ(ToString(fooKindId), "NYT::NDI::(anonymous namespace)::TFoo(ETestEnum::Foo)")
        << ToString(fooKindId);

    auto fooNamedId = TObjectId::GetNamed<TFoo>("Named");
    ASSERT_EQ(ToString(fooNamedId), "NYT::NDI::(anonymous namespace)::TFoo(\"Named\")");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDI

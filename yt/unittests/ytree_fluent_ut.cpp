#include "stdafx.h"

#include "../ytlib/ytree/yson_events.h"
#include "../ytlib/ytree/fluent.h"

#include <contrib/testing/framework.h>

using ::testing::Types;
using ::testing::InSequence;
using ::testing::StrictMock;

using NYT::NYTree::BuildYsonFluently;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TMockConsumer
    : public NYTree::IYsonConsumer
{
public:
    MOCK_METHOD2(OnStringScalar, void(const Stroka& value, bool hasAttributes));
    MOCK_METHOD2(OnInt64Scalar, void(i64 value, bool hasAttributes));
    MOCK_METHOD2(OnDoubleScalar, void(double value, bool hasAttributes));
    MOCK_METHOD1(OnEntity, void(bool hasAttributes));

    MOCK_METHOD0(OnBeginList, void());
    MOCK_METHOD0(OnListItem, void());
    MOCK_METHOD1(OnEndList, void(bool hasAttributes));

    MOCK_METHOD0(OnBeginMap, void());
    MOCK_METHOD1(OnMapItem, void(const Stroka& name));
    MOCK_METHOD1(OnEndMap, void(bool hasAttributes));

    MOCK_METHOD0(OnBeginAttributes, void());
    MOCK_METHOD1(OnAttributesItem, void(const Stroka& name));
    MOCK_METHOD0(OnEndAttributes, void());
};

// String-like Scalars {{{
////////////////////////////////////////////////////////////////////////////////

template<typename T>
class TYTreeFluentStringScalarTest
    : public ::testing::Test
{
};

TYPED_TEST_CASE_P(TYTreeFluentStringScalarTest);
TYPED_TEST_P(TYTreeFluentStringScalarTest, Ok)
{
    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, OnStringScalar("Hello World", false));

    TypeParam passedScalar = "Hello World";
    BuildYsonFluently(&mock)
        .Scalar(passedScalar);
}

typedef Types<const char*, Stroka>
    TYTreeFluentStringScalarTestTypes;
REGISTER_TYPED_TEST_CASE_P(TYTreeFluentStringScalarTest, Ok);
INSTANTIATE_TYPED_TEST_CASE_P(TypeParametrized, TYTreeFluentStringScalarTest,
    TYTreeFluentStringScalarTestTypes
);

////////////////////////////////////////////////////////////////////////////////
// }}}

// Integer-like Scalars {{{
////////////////////////////////////////////////////////////////////////////////

template<typename T>
class TYTreeFluentIntegerScalarTest
    : public ::testing::Test
{
};

TYPED_TEST_CASE_P(TYTreeFluentIntegerScalarTest);
TYPED_TEST_P(TYTreeFluentIntegerScalarTest, Ok)
{
    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, OnInt64Scalar(42, false));

    TypeParam passedScalar = 42;
    BuildYsonFluently(&mock)
        .Scalar(passedScalar);
}

typedef Types<i8, ui8, i16, ui16, i32, ui32, i64>
    TYTreeFluentIntegerScalarTestTypes;
REGISTER_TYPED_TEST_CASE_P(TYTreeFluentIntegerScalarTest, Ok);
INSTANTIATE_TYPED_TEST_CASE_P(TypeParametrized, TYTreeFluentIntegerScalarTest,
    TYTreeFluentIntegerScalarTestTypes
);

////////////////////////////////////////////////////////////////////////////////
// }}}

// Float-like Scalars {{{
////////////////////////////////////////////////////////////////////////////////

template<typename T>
class TYTreeFluentFloatScalarTest
    : public ::testing::Test
{
};

TYPED_TEST_CASE_P(TYTreeFluentFloatScalarTest);
TYPED_TEST_P(TYTreeFluentFloatScalarTest, Ok)
{
    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, OnDoubleScalar(::testing::DoubleEq(3.14f), false));

    TypeParam passedScalar = 3.14f;
    BuildYsonFluently(&mock)
        .Scalar(passedScalar);
}

typedef Types<float, double>
    TYTreeFluentFloatScalarTestTypes;
REGISTER_TYPED_TEST_CASE_P(TYTreeFluentFloatScalarTest, Ok);
INSTANTIATE_TYPED_TEST_CASE_P(TypeParametrized, TYTreeFluentFloatScalarTest,
    TYTreeFluentFloatScalarTestTypes
);

////////////////////////////////////////////////////////////////////////////////
// }}}

// Map {{{
////////////////////////////////////////////////////////////////////////////////

TEST(TYTreeFluentMapTest, Empty)
{
    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, OnBeginMap());
    EXPECT_CALL(mock, OnEndMap(false));
    
    BuildYsonFluently(&mock)
        .BeginMap()
        .EndMap();
}

TEST(TYTreeFluentMapTest, Simple)
{
    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, OnBeginMap());
    EXPECT_CALL(mock, OnMapItem("foo"));
    EXPECT_CALL(mock, OnInt64Scalar(10, false));
    EXPECT_CALL(mock, OnMapItem("bar"));
    EXPECT_CALL(mock, OnInt64Scalar(20, false));
    EXPECT_CALL(mock, OnEndMap(false));

    BuildYsonFluently(&mock)
        .BeginMap()
            .Item("foo")
            .Scalar(10)

            .Item("bar")
            .Scalar(20)
        .EndMap();
}

TEST(TYTreeFluentMapTest, Nested)
{
    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, OnBeginMap());
    EXPECT_CALL(mock, OnMapItem("foo"));
    EXPECT_CALL(mock, OnBeginMap());
    EXPECT_CALL(mock, OnMapItem("xxx"));
    EXPECT_CALL(mock, OnInt64Scalar(17, false));
    EXPECT_CALL(mock, OnEndMap(false));
    EXPECT_CALL(mock, OnMapItem("bar"));
    EXPECT_CALL(mock, OnInt64Scalar(42, false));
    EXPECT_CALL(mock, OnEndMap(false));

    BuildYsonFluently(&mock)
        .BeginMap()
            .Item("foo")
            .BeginMap()
                .Item("xxx")
                .Scalar(17)
            .EndMap()

            .Item("bar")
            .Scalar(42)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////
// }}}

// List {{{
////////////////////////////////////////////////////////////////////////////////

TEST(TYTreeFluentListTest, Empty)
{
    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, OnBeginList());
    EXPECT_CALL(mock, OnEndList(false));
    
    BuildYsonFluently(&mock)
        .BeginList()
        .EndList();
}

TEST(TYTreeFluentListTest, Simple)
{
    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, OnBeginList());
    EXPECT_CALL(mock, OnListItem());
    EXPECT_CALL(mock, OnStringScalar("foo", false));
    EXPECT_CALL(mock, OnListItem());
    EXPECT_CALL(mock, OnStringScalar("bar", false));
    EXPECT_CALL(mock, OnEndList(false));

    BuildYsonFluently(&mock)
        .BeginList()
            .Item()
            .Scalar("foo")

            .Item()
            .Scalar("bar")
        .EndList();
}

TEST(TYTreeFluentListTest, Nested)
{
    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, OnBeginList());
    EXPECT_CALL(mock, OnListItem());
    EXPECT_CALL(mock, OnBeginList());
    EXPECT_CALL(mock, OnListItem());
    EXPECT_CALL(mock, OnStringScalar("foo", false));
    EXPECT_CALL(mock, OnEndList(false));
    EXPECT_CALL(mock, OnListItem());
    EXPECT_CALL(mock, OnStringScalar("bar", false));
    EXPECT_CALL(mock, OnEndList(false));

    BuildYsonFluently(&mock)
        .BeginList()
            .Item()
            .BeginList()
                .Item()
                .Scalar("foo")
            .EndList()

            .Item()
            .Scalar("bar")
        .EndList();
}

////////////////////////////////////////////////////////////////////////////////
// }}}

TEST(TYTreeFluentTest, Complex)
{
    TMockConsumer mock;
    InSequence dummy;

    EXPECT_CALL(mock, OnBeginList());

    EXPECT_CALL(mock, OnListItem());
    EXPECT_CALL(mock, OnInt64Scalar(42, true));
    EXPECT_CALL(mock, OnBeginAttributes());
    EXPECT_CALL(mock, OnAttributesItem("attr1"));
    EXPECT_CALL(mock, OnInt64Scalar(-1, false));
    EXPECT_CALL(mock, OnAttributesItem("attr2"));
    EXPECT_CALL(mock, OnInt64Scalar(-2, false));
    EXPECT_CALL(mock, OnEndAttributes());

    EXPECT_CALL(mock, OnListItem());
    EXPECT_CALL(mock, OnInt64Scalar(17, false));

    EXPECT_CALL(mock, OnListItem());
    EXPECT_CALL(mock, OnBeginList());
    EXPECT_CALL(mock, OnEndList(false));

    EXPECT_CALL(mock, OnListItem());
    EXPECT_CALL(mock, OnBeginList());
    EXPECT_CALL(mock, OnListItem());
    EXPECT_CALL(mock, OnStringScalar("hello", false));
    EXPECT_CALL(mock, OnListItem());
    EXPECT_CALL(mock, OnStringScalar("world", false));
    EXPECT_CALL(mock, OnEndList(true));
    EXPECT_CALL(mock, OnBeginAttributes());
    EXPECT_CALL(mock, OnAttributesItem("hot"));
    EXPECT_CALL(mock, OnStringScalar("chocolate", false));
    EXPECT_CALL(mock, OnEndAttributes());

    EXPECT_CALL(mock, OnListItem());
    EXPECT_CALL(mock, OnBeginMap());
    EXPECT_CALL(mock, OnMapItem("aaa"));
    EXPECT_CALL(mock, OnInt64Scalar(1, false));
    EXPECT_CALL(mock, OnMapItem("bbb"));
    EXPECT_CALL(mock, OnInt64Scalar(2, false));
    EXPECT_CALL(mock, OnEndMap(false));

    EXPECT_CALL(mock, OnListItem());
    EXPECT_CALL(mock, OnEntity(true));
    EXPECT_CALL(mock, OnBeginAttributes());
    EXPECT_CALL(mock, OnAttributesItem("type"));
    EXPECT_CALL(mock, OnStringScalar("extra", false));
    EXPECT_CALL(mock, OnEndAttributes());

    EXPECT_CALL(mock, OnEndList(false));

    BuildYsonFluently(&mock)
        .BeginList()
            // 0
            .Item().WithAttributes()
            .Scalar(42)
            .BeginAttributes()
                .Item("attr1").Scalar(-1)
                .Item("attr2").Scalar(-2)
            .EndAttributes()

            // 1
            .Item()
            .Scalar(17)

            // 2
            .Item()
            .BeginList()
            .EndList()

            // 3
            .Item().WithAttributes()
            .BeginList()
                .Item().Scalar("hello")
                .Item().Scalar("world")
            .EndList()
            .BeginAttributes()
                .Item("hot").Scalar("chocolate")
            .EndAttributes()

            // 4
            .Item()
            .BeginMap()
                .Item("aaa").Scalar(1)
                .Item("bbb").Scalar(2)
            .EndMap()

            // 5
            .Item().WithAttributes()
            .Entity()
            .BeginAttributes()
                .Item("type").Scalar("extra")
            .EndAttributes()
        .EndList();
}

////////////////////////////////////////////////////////////////////////////////

} //

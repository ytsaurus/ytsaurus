#include "../ytlib/ytree/yson_events.h"
#include "../ytlib/ytree/fluent.h"

#include "framework/framework.h"

using ::testing::Types;
using ::testing::InSequence;
using ::testing::StrictMock;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TMockConsumer
    : public NYTree::IYsonConsumer
{
public:
    MOCK_METHOD0(BeginTree, void());
    MOCK_METHOD0(EndTree, void());

    MOCK_METHOD1(StringScalar, void(const Stroka& value));
    MOCK_METHOD1(Int64Scalar, void(i64 value));
    MOCK_METHOD1(DoubleScalar, void(double value));
    MOCK_METHOD0(EntityScalar, void());

    MOCK_METHOD0(BeginList, void());
    MOCK_METHOD1(ListItem, void(int index));
    MOCK_METHOD0(EndList, void());

    MOCK_METHOD0(BeginMap, void());
    MOCK_METHOD1(MapItem, void(const Stroka& name));
    MOCK_METHOD0(EndMap, void());

    MOCK_METHOD0(BeginAttributes, void());
    MOCK_METHOD1(AttributesItem, void(const Stroka& name));
    MOCK_METHOD0(EndAttributes, void());
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

    EXPECT_CALL(mock, BeginTree());
    EXPECT_CALL(mock, StringScalar("Hello World"));
    EXPECT_CALL(mock, EndTree());

    TypeParam passedScalar = "Hello World";
    NYTree::TFluentYsonBuilder::Create(&mock)
        .BeginTree()
            .Scalar(passedScalar)
        .EndTree();
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

    EXPECT_CALL(mock, BeginTree());
    EXPECT_CALL(mock, Int64Scalar(42));
    EXPECT_CALL(mock, EndTree());

    TypeParam passedScalar = 42;
    NYTree::TFluentYsonBuilder::Create(&mock)
        .BeginTree()
            .Scalar(passedScalar)
        .EndTree();
}

// TODO: Add <ui8, ui16, ui32, ui64> to the list of used types.
// Currently does not compile.
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

    EXPECT_CALL(mock, BeginTree());
    EXPECT_CALL(mock, DoubleScalar(::testing::DoubleEq(3.14f)));
    EXPECT_CALL(mock, EndTree());

    TypeParam passedScalar = 3.14f;
    NYTree::TFluentYsonBuilder::Create(&mock)
        .BeginTree()
            .Scalar(passedScalar)
        .EndTree();
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

    EXPECT_CALL(mock, BeginTree());
    EXPECT_CALL(mock, BeginMap());
    EXPECT_CALL(mock, EndMap());
    EXPECT_CALL(mock, EndTree());

    NYTree::TFluentYsonBuilder::Create(&mock)
        .BeginTree()
            .BeginMap()
            .EndMap()
        .EndTree();
}

TEST(TYTreeFluentMapTest, Simple)
{
    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, BeginTree());
    EXPECT_CALL(mock, BeginMap());
    EXPECT_CALL(mock, MapItem("foo"));
    EXPECT_CALL(mock, Int64Scalar(10));
    EXPECT_CALL(mock, MapItem("bar"));
    EXPECT_CALL(mock, Int64Scalar(20));
    EXPECT_CALL(mock, EndMap());
    EXPECT_CALL(mock, EndTree());

    NYTree::TFluentYsonBuilder::Create(&mock)
        .BeginTree()
            .BeginMap()
                .Item("foo")
                .Scalar(10)

                .Item("bar")
                .Scalar(20)
            .EndMap()
        .EndTree();
}

TEST(TYTreeFluentMapTest, Nested)
{
    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, BeginTree());
    EXPECT_CALL(mock, BeginMap());
    EXPECT_CALL(mock, MapItem("foo"));
    EXPECT_CALL(mock, BeginMap());
    EXPECT_CALL(mock, MapItem("xxx"));
    EXPECT_CALL(mock, Int64Scalar(17));
    EXPECT_CALL(mock, EndMap());
    EXPECT_CALL(mock, MapItem("bar"));
    EXPECT_CALL(mock, Int64Scalar(42));
    EXPECT_CALL(mock, EndMap());
    EXPECT_CALL(mock, EndTree());

    NYTree::TFluentYsonBuilder::Create(&mock)
        .BeginTree()
            .BeginMap()
                .Item("foo")
                .BeginMap()
                    .Item("xxx")
                    .Scalar(17)
                .EndMap()

                .Item("bar")
                .Scalar(42)
            .EndMap()
        .EndTree();
}

////////////////////////////////////////////////////////////////////////////////
// }}}

// List {{{
////////////////////////////////////////////////////////////////////////////////

TEST(TYTreeFluentListTest, Empty)
{
    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, BeginTree());
    EXPECT_CALL(mock, BeginList());
    EXPECT_CALL(mock, EndList());
    EXPECT_CALL(mock, EndTree());

    NYTree::TFluentYsonBuilder::Create(&mock)
        .BeginTree()
            .BeginList()
            .EndList()
        .EndTree();
}

TEST(TYTreeFluentListTest, Simple)
{
    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, BeginTree());
    EXPECT_CALL(mock, BeginList());
    EXPECT_CALL(mock, ListItem(0));
    EXPECT_CALL(mock, StringScalar("foo"));
    EXPECT_CALL(mock, ListItem(1));
    EXPECT_CALL(mock, StringScalar("bar"));
    EXPECT_CALL(mock, EndList());
    EXPECT_CALL(mock, EndTree());

    NYTree::TFluentYsonBuilder::Create(&mock)
        .BeginTree()
            .BeginList()
                .Item()
                .Scalar("foo")

                .Item()
                .Scalar("bar")
            .EndList()
        .EndTree();
}

TEST(TYTreeFluentListTest, Nested)
{
    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, BeginTree());
    EXPECT_CALL(mock, BeginList());
    EXPECT_CALL(mock, ListItem(0));
    EXPECT_CALL(mock, BeginList());
    EXPECT_CALL(mock, ListItem(0));
    EXPECT_CALL(mock, StringScalar("foo"));
    EXPECT_CALL(mock, EndList());
    EXPECT_CALL(mock, ListItem(1));
    EXPECT_CALL(mock, StringScalar("bar"));
    EXPECT_CALL(mock, EndList());
    EXPECT_CALL(mock, EndTree());

    NYTree::TFluentYsonBuilder::Create(&mock)
        .BeginTree()
            .BeginList()
                .Item()
                .BeginList()
                    .Item()
                    .Scalar("foo")
                .EndList()

                .Item()
                .Scalar("bar")
            .EndList()
        .EndTree();
}

////////////////////////////////////////////////////////////////////////////////
// }}}

TEST(TYTreeFluentTest, Complex)
{
    TMockConsumer mock;
    InSequence dummy;

    EXPECT_CALL(mock, BeginTree());
    EXPECT_CALL(mock, BeginList());

    EXPECT_CALL(mock, ListItem(0));
    EXPECT_CALL(mock, Int64Scalar(42));

    EXPECT_CALL(mock, BeginAttributes());
    EXPECT_CALL(mock, AttributesItem("attr1"));
    EXPECT_CALL(mock, Int64Scalar(-1));
    EXPECT_CALL(mock, AttributesItem("attr2"));
    EXPECT_CALL(mock, Int64Scalar(-2));
    EXPECT_CALL(mock, EndAttributes());

    EXPECT_CALL(mock, ListItem(1));
    EXPECT_CALL(mock, Int64Scalar(17));

    EXPECT_CALL(mock, ListItem(2));
    EXPECT_CALL(mock, BeginList());
    EXPECT_CALL(mock, EndList());

    EXPECT_CALL(mock, ListItem(3));
    EXPECT_CALL(mock, BeginList());
    EXPECT_CALL(mock, ListItem(0));
    EXPECT_CALL(mock, StringScalar("hello"));
    EXPECT_CALL(mock, ListItem(1));
    EXPECT_CALL(mock, StringScalar("world"));
    EXPECT_CALL(mock, EndList());
    EXPECT_CALL(mock, BeginAttributes());
    EXPECT_CALL(mock, AttributesItem("hot"));
    EXPECT_CALL(mock, StringScalar("chocolate"));
    EXPECT_CALL(mock, EndAttributes());

    EXPECT_CALL(mock, ListItem(4));
    EXPECT_CALL(mock, BeginMap());
    EXPECT_CALL(mock, MapItem("aaa"));
    EXPECT_CALL(mock, Int64Scalar(1));
    EXPECT_CALL(mock, MapItem("bbb"));
    EXPECT_CALL(mock, Int64Scalar(2));
    EXPECT_CALL(mock, EndMap());

    EXPECT_CALL(mock, ListItem(5));
    EXPECT_CALL(mock, EntityScalar());
    EXPECT_CALL(mock, BeginAttributes());
    EXPECT_CALL(mock, AttributesItem("type"));
    EXPECT_CALL(mock, StringScalar("extra"));
    EXPECT_CALL(mock, EndAttributes());

    EXPECT_CALL(mock, EndList());
    EXPECT_CALL(mock, EndTree());

    NYTree::TFluentYsonBuilder::Create(&mock)
        .BeginTree()
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
                .EntityScalar()
                .BeginAttributes()
                    .Item("type").Scalar("extra")
                .EndAttributes()
            .EndList()
        .EndTree();
}

////////////////////////////////////////////////////////////////////////////////

} //

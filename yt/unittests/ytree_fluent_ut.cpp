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

    MOCK_METHOD1(StringValue, void(const Stroka& value));
    MOCK_METHOD1(Int64Value, void(i64 value));
    MOCK_METHOD1(DoubleValue, void(double value));
    MOCK_METHOD0(EntityValue, void());

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

////////////////////////////////////////////////////////////////////////////////

TEST(TYTreeFluentTest, EmptyTree)
{
    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, BeginTree());
    EXPECT_CALL(mock, EndTree());

    FAIL() << "Should compile, at least.";
#if 0
    NYTree::TFluentYsonParser::Create(&mock)
        .BeginTree()
        .EndTree();
#endif
}

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
    EXPECT_CALL(mock, StringValue("Hello World"));
    EXPECT_CALL(mock, EndTree());

    TypeParam passedValue = "Hello World";
    NYTree::TFluentYsonParser::Create(&mock)
    .BeginTree().Value(passedValue).EndTree();
}

typedef Types<const char*, Stroka>
    TYTreeFluentStringScalarTestTypes;
REGISTER_TYPED_TEST_CASE_P(TYTreeFluentStringScalarTest, Ok);
INSTANTIATE_TYPED_TEST_CASE_P(My, TYTreeFluentStringScalarTest,
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
    EXPECT_CALL(mock, Int64Value(42));
    EXPECT_CALL(mock, EndTree());

    TypeParam passedValue = 42;
    NYTree::TFluentYsonParser::Create(&mock)
    .BeginTree().Value(passedValue).EndTree();
}

// TODO: Add <ui8, ui16, ui32, ui64> to the list of used types.
// Currently does not compile.
typedef Types<i8, i16, i32, i64>
    TYTreeFluentIntegerScalarTestTypes;
REGISTER_TYPED_TEST_CASE_P(TYTreeFluentIntegerScalarTest, Ok);
INSTANTIATE_TYPED_TEST_CASE_P(My, TYTreeFluentIntegerScalarTest,
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
    EXPECT_CALL(mock, DoubleValue(::testing::DoubleEq(3.1415926)));
    EXPECT_CALL(mock, EndTree());

    TypeParam passedValue = 3.1415926;
    NYTree::TFluentYsonParser::Create(&mock)
    .BeginTree().Value(passedValue).EndTree();
}

typedef Types<float, double>
    TYTreeFluentFloatScalarTestTypes;
REGISTER_TYPED_TEST_CASE_P(TYTreeFluentFloatScalarTest, Ok);
INSTANTIATE_TYPED_TEST_CASE_P(My, TYTreeFluentFloatScalarTest,
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

    NYTree::TFluentYsonParser::Create(&mock)
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
    EXPECT_CALL(mock, Int64Value(10));
    EXPECT_CALL(mock, MapItem("bar"));
    EXPECT_CALL(mock, Int64Value(20));
    EXPECT_CALL(mock, EndMap());
    EXPECT_CALL(mock, EndTree());

    NYTree::TFluentYsonParser::Create(&mock)
    .BeginTree()
        .BeginMap()
            .Item("foo")
            .Value(10)

            .Item("bar")
            .Value(20)
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
    EXPECT_CALL(mock, Int64Value(17));
    EXPECT_CALL(mock, EndMap());
    EXPECT_CALL(mock, MapItem("bar"));
    EXPECT_CALL(mock, Int64Value(42));
    EXPECT_CALL(mock, EndMap());
    EXPECT_CALL(mock, EndTree());

    NYTree::TFluentYsonParser::Create(&mock)
    .BeginTree()
        .BeginMap()
            .Item("foo")
            .BeginMap()
                .Item("xxx")
                .Value(17)
            .EndMap()

            .Item("bar")
            .Value(42)
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
}

TEST(TYTreeFluentListTest, Simple)
{
    StrictMock<TMockConsumer> mock;
    InSequence dummy;

    EXPECT_CALL(mock, BeginTree());
    EXPECT_CALL(mock, BeginList());
    EXPECT_CALL(mock, ListItem(0));
    EXPECT_CALL(mock, StringValue("foo"));
    EXPECT_CALL(mock, ListItem(1));
    EXPECT_CALL(mock, StringValue("bar"));
    EXPECT_CALL(mock, EndList());
    EXPECT_CALL(mock, EndTree());

    NYTree::TFluentYsonParser::Create(&mock)
    .BeginTree()
        .BeginList()
            .Item()
            .Value("foo")

            .Item()
            .Value("bar")
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
    EXPECT_CALL(mock, StringValue("foo"));
    EXPECT_CALL(mock, EndList());
    EXPECT_CALL(mock, ListItem(1));
    EXPECT_CALL(mock, StringValue("bar"));
    EXPECT_CALL(mock, EndList());
    EXPECT_CALL(mock, EndTree());

    NYTree::TFluentYsonParser::Create(&mock)
    .BeginTree()
        .BeginList()
            .Item()
            .BeginList()
                .Item()
                .Value("foo")
            .EndList()

            .Item()
            .Value("bar")
        .EndList()
    .EndTree();
}

////////////////////////////////////////////////////////////////////////////////
// }}}

TEST(TYTreeFluentTest, Complex)
{
    TMockConsumer mock;
    InSequence dummy;

    FAIL() << "Please, fix me!";

    EXPECT_CALL(mock, BeginTree());
    EXPECT_CALL(mock, BeginList());

    EXPECT_CALL(mock, ListItem(0));
    EXPECT_CALL(mock, Int64Value(42));

    EXPECT_CALL(mock, BeginAttributes());
    EXPECT_CALL(mock, AttributesItem("attr1"));
    EXPECT_CALL(mock, Int64Value(-1));
    EXPECT_CALL(mock, AttributesItem("attr2"));
    EXPECT_CALL(mock, Int64Value(-2));
    EXPECT_CALL(mock, EndAttributes());

    EXPECT_CALL(mock, ListItem(1));
    EXPECT_CALL(mock, Int64Value(17));

    EXPECT_CALL(mock, ListItem(2));
    EXPECT_CALL(mock, BeginList());
    EXPECT_CALL(mock, EndList());

    EXPECT_CALL(mock, ListItem(3));
    EXPECT_CALL(mock, BeginList());
    EXPECT_CALL(mock, ListItem(0));
    EXPECT_CALL(mock, StringValue("hello"));
    EXPECT_CALL(mock, ListItem(1));
    EXPECT_CALL(mock, StringValue("world"));
    EXPECT_CALL(mock, EndList());
    EXPECT_CALL(mock, BeginAttributes());
    EXPECT_CALL(mock, AttributesItem("hot"));
    EXPECT_CALL(mock, StringValue("chocolate"));
    EXPECT_CALL(mock, EndAttributes());

    EXPECT_CALL(mock, ListItem(4));
    EXPECT_CALL(mock, BeginMap());
    EXPECT_CALL(mock, MapItem("aaa"));
    EXPECT_CALL(mock, Int64Value(1));
    EXPECT_CALL(mock, MapItem("bbb"));
    EXPECT_CALL(mock, Int64Value(2));
    EXPECT_CALL(mock, EndMap());

    EXPECT_CALL(mock, ListItem(5));
    EXPECT_CALL(mock, EntityValue());
    EXPECT_CALL(mock, BeginAttributes());
    EXPECT_CALL(mock, AttributesItem("type"));
    EXPECT_CALL(mock, StringValue("extra"));
    EXPECT_CALL(mock, EndAttributes());

    EXPECT_CALL(mock, EndList());
    EXPECT_CALL(mock, EndTree());

    NYTree::TFluentYsonParser::Create(&mock)
        .BeginTree()
            .BeginList()
                // 0
                .Item().WithAttributes()
                .Value(42)
                .BeginAttributes()
                    .Item("attr1").Value(-1)
                    .Item("attr2").Value(-2)
                .EndAttributes()

                // 1
                .Item()
                .Value(17)

                // 2
                .Item()
                .BeginList()
                .EndList()

                // 3
                .Item().WithAttributes()
                .BeginList()
                    .Item().Value("hello")
                    .Item().Value("world")
                .EndList()
                .BeginAttributes()
                    .Item("hot").Value("chocolate")
                .EndAttributes()

                // 4
                .Item()
                .BeginMap()
                    .Item("aaa").Value(1)
                    .Item("bbb").Value(2)
                .EndMap()

                // 5
                .Item().WithAttributes()
                .EntityValue()
                .BeginAttributes()
                    .Item("type").Value("extra")
                .EndAttributes()
            .EndList()
        .EndTree();
}

////////////////////////////////////////////////////////////////////////////////

} //

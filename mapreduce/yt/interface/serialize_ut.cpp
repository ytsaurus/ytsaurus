#include <mapreduce/yt/interface/serialize.h>

#include <library/unittest/registar.h>

using namespace NYT;

Y_UNIT_TEST_SUITE(Serialization)
{
    Y_UNIT_TEST(TableSchema)
    {
        auto schema = TTableSchema()
            .AddColumn(TColumnSchema().Name("a").Type(EValueType::VT_STRING).SortOrder(SO_ASCENDING))
            .AddColumn(TColumnSchema().Name("b").Type(EValueType::VT_UINT64))
            .AddColumn(TColumnSchema().Name("c").Type(EValueType::VT_INT64, true));

        auto schemaNode = schema.ToNode();
        UNIT_ASSERT(schemaNode.IsList());
        UNIT_ASSERT_VALUES_EQUAL(schemaNode.Size(), 3);


        UNIT_ASSERT_VALUES_EQUAL(schemaNode[0]["name"], "a");
        UNIT_ASSERT_VALUES_EQUAL(schemaNode[0]["type"], "string");
        UNIT_ASSERT_VALUES_EQUAL(schemaNode[0]["required"], false);
        UNIT_ASSERT_VALUES_EQUAL(schemaNode[0]["sort_order"], "ascending");

        UNIT_ASSERT_VALUES_EQUAL(schemaNode[1]["name"], "b");
        UNIT_ASSERT_VALUES_EQUAL(schemaNode[1]["type"], "uint64");
        UNIT_ASSERT_VALUES_EQUAL(schemaNode[1]["required"], false);

        UNIT_ASSERT_VALUES_EQUAL(schemaNode[2]["name"], "c");
        UNIT_ASSERT_VALUES_EQUAL(schemaNode[2]["type"], "int64");
        UNIT_ASSERT_VALUES_EQUAL(schemaNode[2]["required"], true);
    }

    Y_UNIT_TEST(TableSchemaRawTypeV2)
    {
        auto schema = TTableSchema()
            .AddColumn(TColumnSchema()
                .Name("a")
                .RawTypeV2(TNode()
                    ("metatype", "list")
                    ("element", "string")))
            .AddColumn(TColumnSchema()
                .Name("b")
                .RawTypeV2(TNode()
                    ("metatype", "optional")
                    ("element", "uint64")))
            .AddColumn(TColumnSchema()
                .Name("c")
                .RawTypeV2("int64"));

        auto schemaNode = schema.ToNode();
        UNIT_ASSERT(schemaNode.IsList());
        UNIT_ASSERT_VALUES_EQUAL(schemaNode.Size(), 3);


        UNIT_ASSERT_VALUES_EQUAL(schemaNode[0]["name"], "a");
        UNIT_ASSERT_VALUES_EQUAL(
            schemaNode[0]["type_v2"],
            TNode()
                ("metatype", "list")
                ("element", "string"));

        UNIT_ASSERT_VALUES_EQUAL(schemaNode[1]["name"], "b");
        UNIT_ASSERT_VALUES_EQUAL(schemaNode[1]["type"], "uint64");
        UNIT_ASSERT_VALUES_EQUAL(schemaNode[1]["required"], false);
        UNIT_ASSERT_VALUES_EQUAL(
            schemaNode[1]["type_v2"],
            TNode()
                ("metatype", "optional")
                ("element", "uint64"));

        UNIT_ASSERT_VALUES_EQUAL(schemaNode[2]["name"], "c");
        UNIT_ASSERT_VALUES_EQUAL(schemaNode[2]["type"], "int64");
        UNIT_ASSERT_VALUES_EQUAL(schemaNode[2]["required"], true);
        UNIT_ASSERT_VALUES_EQUAL(schemaNode[2]["type_v2"], "int64");
    }
}

#include "common.h"

#include <mapreduce/yt/interface/protobuf_table_schema_ut.pb.h>

#include <library/unittest/registar.h>

#include <algorithm>

using namespace NYT;

TNode MakeLogicalType(EValueType type, bool required)
{
    TNode node = NYT::NDetail::ToString(type);
    if (!required) {
        node = TNode()
            ("metatype", "optional")
            ("element", std::move(node));
    }
    return node;
}

TTableSchema Canonize(TTableSchema schema)
{
    for (auto& columnSchema : schema.Columns_) {
        if (!columnSchema.RawTypeV2_) {
            columnSchema.RawTypeV2(MakeLogicalType(columnSchema.Type_, columnSchema.Required_));
        }
    }
    return schema;
}

#define TEST_FIELD(field, name, type) \
    UNIT_ASSERT_VALUES_EQUAL(name, field.Name_);\
    UNIT_ASSERT_VALUES_EQUAL(type, field.RawTypeV2_);\
    UNIT_ASSERT(!field.SortOrder_);

#define TEST_FIELD_SORTED(field, name, type) \
    UNIT_ASSERT_VALUES_EQUAL(name, field.Name_);\
    UNIT_ASSERT_VALUES_EQUAL(type, field.RawTypeV2_);\
    UNIT_ASSERT_VALUES_EQUAL(SO_ASCENDING, field.SortOrder_);

bool IsFieldPresent(const TTableSchema& schema, TStringBuf name)
{
    return cend(schema.Columns_) != std::find_if(
        cbegin(schema.Columns_),
        cend(schema.Columns_),
        [&] (const auto& v) {
            return v.Name_ == name;
        });
}

Y_UNIT_TEST_SUITE(ProtoSchemaTest_Simple)
{
    Y_UNIT_TEST(TIntegral)
    {
        const auto schema = Canonize(CreateTableSchema<NTesting::TIntegral>());

        UNIT_ASSERT_EQUAL(14, schema.Columns_.size());

        TEST_FIELD(schema.Columns_[0], "DoubleField", MakeLogicalType(EValueType::VT_DOUBLE, false));
        TEST_FIELD(schema.Columns_[1], "FloatField", MakeLogicalType(EValueType::VT_DOUBLE, false));
        TEST_FIELD(schema.Columns_[2], "Int32Field", MakeLogicalType(EValueType::VT_INT32, false));
        TEST_FIELD(schema.Columns_[3], "Int64Field", MakeLogicalType(EValueType::VT_INT64, false));
        TEST_FIELD(schema.Columns_[4], "Uint32Field", MakeLogicalType(EValueType::VT_UINT32, false));
        TEST_FIELD(schema.Columns_[5], "Uint64Field", MakeLogicalType(EValueType::VT_UINT64, false));
        TEST_FIELD(schema.Columns_[6], "Sint32Field", MakeLogicalType(EValueType::VT_INT32, false));
        TEST_FIELD(schema.Columns_[7], "Sint64Field", MakeLogicalType(EValueType::VT_INT64, false));
        TEST_FIELD(schema.Columns_[8], "Fixed32Field", MakeLogicalType(EValueType::VT_UINT32, false));
        TEST_FIELD(schema.Columns_[9], "Fixed64Field", MakeLogicalType(EValueType::VT_UINT64, false));
        TEST_FIELD(schema.Columns_[10], "Sfixed32Field", MakeLogicalType(EValueType::VT_INT32, false));
        TEST_FIELD(schema.Columns_[11], "Sfixed64Field", MakeLogicalType(EValueType::VT_INT64, false));
        TEST_FIELD(schema.Columns_[12], "BoolField", MakeLogicalType(EValueType::VT_BOOLEAN, false));
        TEST_FIELD(schema.Columns_[13], "EnumField", MakeLogicalType(EValueType::VT_STRING, false));
    }

    Y_UNIT_TEST(TOneOf)
    {
        const auto schema = Canonize(CreateTableSchema<NTesting::TOneOf>());

        UNIT_ASSERT_EQUAL(3, schema.Columns_.size());

        TEST_FIELD(schema.Columns_[0], "DoubleField", MakeLogicalType(EValueType::VT_DOUBLE, false));
        TEST_FIELD(schema.Columns_[1], "Int32Field", MakeLogicalType(EValueType::VT_INT32, false));
        TEST_FIELD(schema.Columns_[2], "BoolField", MakeLogicalType(EValueType::VT_BOOLEAN, false));
    }

    Y_UNIT_TEST(TWithRequired)
    {
        const auto schema = Canonize(CreateTableSchema<NTesting::TWithRequired>());

        UNIT_ASSERT_EQUAL(2, schema.Columns_.size());

        TEST_FIELD(schema.Columns_[0], "RequiredField", MakeLogicalType(EValueType::VT_STRING, true));
        TEST_FIELD(schema.Columns_[1], "NotRequiredField", MakeLogicalType(EValueType::VT_STRING, false));
    }

    Y_UNIT_TEST(TAggregated)
    {
        const auto schema = Canonize(CreateTableSchema<NTesting::TAggregated>());

        UNIT_ASSERT_EQUAL(6, schema.Columns_.size());

        TEST_FIELD(schema.Columns_[0], "StringField", MakeLogicalType(EValueType::VT_STRING, false));
        TEST_FIELD(schema.Columns_[1], "BytesField", MakeLogicalType(EValueType::VT_STRING, false));
        TEST_FIELD(schema.Columns_[2], "NestedField", MakeLogicalType(EValueType::VT_STRING, false));
        TEST_FIELD(schema.Columns_[3], "NestedRepeatedField", MakeLogicalType(EValueType::VT_STRING, false));
        TEST_FIELD(schema.Columns_[4], "NestedOneOfField", MakeLogicalType(EValueType::VT_STRING, false));
        TEST_FIELD(schema.Columns_[5], "NestedRecursiveField", MakeLogicalType(EValueType::VT_STRING, false));
    }

    Y_UNIT_TEST(TAliased)
    {
        const auto schema = Canonize(CreateTableSchema<NTesting::TAliased>());

        UNIT_ASSERT_EQUAL(3, schema.Columns_.size());

        TEST_FIELD(schema.Columns_[0], "key", MakeLogicalType(EValueType::VT_INT32, false));
        TEST_FIELD(schema.Columns_[1], "subkey", MakeLogicalType(EValueType::VT_DOUBLE, false));
        TEST_FIELD(schema.Columns_[2], "Data", MakeLogicalType(EValueType::VT_STRING, false));
    }

    Y_UNIT_TEST(KeyColumns)
    {
        const TKeyColumns keys = {"key", "subkey"};

        const auto schema = Canonize(CreateTableSchema<NTesting::TAliased>(keys));

        TEST_FIELD_SORTED(schema.Columns_[0], "key", MakeLogicalType(EValueType::VT_INT32, false));
        TEST_FIELD_SORTED(schema.Columns_[1], "subkey", MakeLogicalType(EValueType::VT_DOUBLE, false));
        TEST_FIELD(schema.Columns_[2], "Data", MakeLogicalType(EValueType::VT_STRING, false));
    }

    Y_UNIT_TEST(KeyColumnsReordered)
    {
        const TKeyColumns keys = {"subkey"};

        const auto schema = Canonize(CreateTableSchema<NTesting::TAliased>(keys));

        TEST_FIELD_SORTED(schema.Columns_[0], "subkey", MakeLogicalType(EValueType::VT_DOUBLE, false));
        TEST_FIELD(schema.Columns_[1], "key", MakeLogicalType(EValueType::VT_INT32, false));
        TEST_FIELD(schema.Columns_[2], "Data", MakeLogicalType(EValueType::VT_STRING, false));
        UNIT_ASSERT_EQUAL(schema.Columns_.size(), 3);
    }

    Y_UNIT_TEST(KeyColumnsInvalid)
    {
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NTesting::TAliased>({"subkey", "subkey"}), yexception);
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NTesting::TAliased>({"key", "junk"}), yexception);
    }

    Y_UNIT_TEST(KeepFieldsWithoutExtensionTrue)
    {
        const auto s = Canonize(CreateTableSchema<NTesting::TAliased>({}, true));
        UNIT_ASSERT(IsFieldPresent(s, "key"));
        UNIT_ASSERT(IsFieldPresent(s, "subkey"));
        UNIT_ASSERT(IsFieldPresent(s, "Data"));
    }

    Y_UNIT_TEST(KeepFieldsWithoutExtensionFalse)
    {
        const auto s = Canonize(CreateTableSchema<NTesting::TAliased>({}, false));
        UNIT_ASSERT(IsFieldPresent(s, "key"));
        UNIT_ASSERT(IsFieldPresent(s, "subkey"));
        UNIT_ASSERT(!IsFieldPresent(s, "Data"));
    }
}

Y_UNIT_TEST_SUITE(ProtoSchemaTest_Complex)
{
    Y_UNIT_TEST(TRepeated)
    {
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NTesting::TRepeated>(), yexception);

        const auto schema = Canonize(CreateTableSchema<NTesting::TRepeatedYtMode>());
        UNIT_ASSERT_EQUAL(1, schema.Columns_.size());
        TEST_FIELD(
            schema.Columns_[0],
            "Int32Field",
            TNode()("metatype", "list")("element", MakeLogicalType(EValueType::VT_INT32, true)));
    }

    const TNode& GetUrlRowType(bool required)
    {
        static auto node = TNode()
            ("metatype", "struct")
            ("fields", TNode()
                .Add(TNode()("name", "Host")("type", MakeLogicalType(EValueType::VT_STRING, false)))
                .Add(TNode()("name", "Path")("type", MakeLogicalType(EValueType::VT_STRING, false)))
                .Add(TNode()("name", "HttpCode")("type", MakeLogicalType(EValueType::VT_INT32, false))));
        static auto optionalNode = TNode()
            ("metatype", "optional")
            ("element", node);
        return required ? node : optionalNode;
    }

    Y_UNIT_TEST(TRowFieldSerializationOption)
    {
        const auto schema = Canonize(CreateTableSchema<NTesting::TRowFieldSerializationOption>());

        UNIT_ASSERT_EQUAL(2, schema.Columns_.size());

        TEST_FIELD(
            schema.Columns_[0],
            "UrlRow_1",
            GetUrlRowType(false));

        TEST_FIELD(
            schema.Columns_[1],
            "UrlRow_2",
            MakeLogicalType(EValueType::VT_STRING, false));
    }

    Y_UNIT_TEST(TRowMessageSerializationOption)
    {
        const auto schema = Canonize(CreateTableSchema<NTesting::TRowMessageSerializationOption>());

        UNIT_ASSERT_EQUAL(2, schema.Columns_.size());

        TEST_FIELD(
            schema.Columns_[0],
            "UrlRow_1",
            GetUrlRowType(false));

        TEST_FIELD(
            schema.Columns_[1],
            "UrlRow_2",
            GetUrlRowType(false));
    }

    Y_UNIT_TEST(TRowMixedSerializationOptions)
    {
        const auto schema = Canonize(CreateTableSchema<NTesting::TRowMixedSerializationOptions>());

        UNIT_ASSERT_EQUAL(2, schema.Columns_.size());

        TEST_FIELD(
            schema.Columns_[0],
            "UrlRow_1",
            GetUrlRowType(false));

        TEST_FIELD(
            schema.Columns_[1],
            "UrlRow_2",
            MakeLogicalType(EValueType::VT_STRING, false));
    }

    const TNode& GetUrlRowType_ColumnNames(bool required)
    {
        static auto node = TNode()
            ("metatype", "struct")
            ("fields", TNode()
                .Add(TNode()("name", "Host_ColumnName")("type", MakeLogicalType(EValueType::VT_STRING, false)))
                .Add(TNode()("name", "Path_KeyColumnName")("type", MakeLogicalType(EValueType::VT_STRING, false)))
                .Add(TNode()("name", "HttpCode")("type", MakeLogicalType(EValueType::VT_INT32, false))));
        static auto optionalNode = TNode()
            ("metatype", "optional")
            ("element", node);
        return required ? node : optionalNode;
    }

    Y_UNIT_TEST(TRowMixedSerializationOptions_ColumnNames)
    {
        const auto schema = Canonize(CreateTableSchema<NTesting::TRowMixedSerializationOptions_ColumnNames>());

        UNIT_ASSERT_EQUAL(2, schema.Columns_.size());

        TEST_FIELD(
            schema.Columns_[0],
            "UrlRow_1",
            GetUrlRowType_ColumnNames(false));

        TEST_FIELD(
            schema.Columns_[1],
            "UrlRow_2",
            MakeLogicalType(EValueType::VT_STRING, false));
    }
}

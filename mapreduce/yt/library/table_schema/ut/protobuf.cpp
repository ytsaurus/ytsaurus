#include <mapreduce/yt/library/table_schema/protobuf.h>

#include <mapreduce/yt/library/table_schema/ut/test.pb.h>

#include <library/unittest/registar.h>

#include <algorithm>

using namespace NYT;

#define TEST_FIELD(field, name, type) \
    UNIT_ASSERT_EQUAL(name, field.Name_);\
    UNIT_ASSERT_EQUAL(type, field.Type_);\
    UNIT_ASSERT(!field.SortOrder_);

#define TEST_FIELD_SORTED(field, name, type) \
    UNIT_ASSERT_EQUAL(name, field.Name_);\
    UNIT_ASSERT_EQUAL(type, field.Type_);\
    UNIT_ASSERT_EQUAL(SO_ASCENDING, field.SortOrder_);

#define TEST_FIELD_PRESENT(schema, name) \
    UNIT_ASSERT(std::find_if(cbegin(schema.Columns_), cend(schema.Columns_), [&](const auto& v){ return v.Name_ == name; }) != cend(schema.Columns_));

#define TEST_FIELD_NOT_PRESENT(schema, name) \
    UNIT_ASSERT(std::find_if(cbegin(schema.Columns_), cend(schema.Columns_), [&](const auto& v){ return v.Name_ == name; }) == cend(schema.Columns_));

Y_UNIT_TEST_SUITE(ProtoSchemaTest) {
    Y_UNIT_TEST(TIntegral) {
        const auto schema = CreateTableSchema<NTesting::TIntegral>();

        UNIT_ASSERT_EQUAL(14, schema.Columns_.size());

        TEST_FIELD(schema.Columns_[0], "DoubleField", EValueType::VT_DOUBLE);
        TEST_FIELD(schema.Columns_[1], "FloatField", EValueType::VT_DOUBLE);
        TEST_FIELD(schema.Columns_[2], "Int32Field", EValueType::VT_INT32);
        TEST_FIELD(schema.Columns_[3], "Int64Field", EValueType::VT_INT64);
        TEST_FIELD(schema.Columns_[4], "Uint32Field", EValueType::VT_UINT32);
        TEST_FIELD(schema.Columns_[5], "Uint64Field", EValueType::VT_UINT64);
        TEST_FIELD(schema.Columns_[6], "Sint32Field", EValueType::VT_INT32);
        TEST_FIELD(schema.Columns_[7], "Sint64Field", EValueType::VT_INT64);
        TEST_FIELD(schema.Columns_[8], "Fixed32Field", EValueType::VT_UINT32);
        TEST_FIELD(schema.Columns_[9], "Fixed64Field", EValueType::VT_UINT64);
        TEST_FIELD(schema.Columns_[10], "Sfixed32Field", EValueType::VT_INT32);
        TEST_FIELD(schema.Columns_[11], "Sfixed64Field", EValueType::VT_INT64);
        TEST_FIELD(schema.Columns_[12], "BoolField", EValueType::VT_BOOLEAN);
        TEST_FIELD(schema.Columns_[13], "EnumField", EValueType::VT_STRING);
    }

    Y_UNIT_TEST(TRepeated) {
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NTesting::TRepeated>(), yexception);
    }

    Y_UNIT_TEST(TOneOf) {
        const auto schema = CreateTableSchema<NTesting::TOneOf>();

        UNIT_ASSERT_EQUAL(3, schema.Columns_.size());

        TEST_FIELD(schema.Columns_[0], "DoubleField", EValueType::VT_DOUBLE);
        TEST_FIELD(schema.Columns_[1], "Int32Field", EValueType::VT_INT32);
        TEST_FIELD(schema.Columns_[2], "BoolField", EValueType::VT_BOOLEAN);
    }

    Y_UNIT_TEST(TWithRequired) {
        const auto schema = CreateTableSchema<NTesting::TWithRequired>();

        UNIT_ASSERT_EQUAL(2, schema.Columns_.size());

        TEST_FIELD(schema.Columns_[0], "RequiredField", EValueType::VT_STRING);
        UNIT_ASSERT(schema.Columns_[0].Required_);
        TEST_FIELD(schema.Columns_[1], "NotRequiredField", EValueType::VT_STRING);
        UNIT_ASSERT(!schema.Columns_[1].Required_);
    }

    Y_UNIT_TEST(TAggregated) {
        const auto schema = CreateTableSchema<NTesting::TAggregated>();

        UNIT_ASSERT_EQUAL(6, schema.Columns_.size());

        TEST_FIELD(schema.Columns_[0], "StringField", EValueType::VT_STRING);
        TEST_FIELD(schema.Columns_[1], "BytesField", EValueType::VT_STRING);
        TEST_FIELD(schema.Columns_[2], "NestedField", EValueType::VT_STRING);
        TEST_FIELD(schema.Columns_[3], "NestedRepeatedField", EValueType::VT_STRING);
        TEST_FIELD(schema.Columns_[4], "NestedOneOfField", EValueType::VT_STRING);
        TEST_FIELD(schema.Columns_[5], "NestedRecursiveField", EValueType::VT_STRING);
    }

    Y_UNIT_TEST(TAliased) {
        const auto schema = CreateTableSchema<NTesting::TAliased>();

        UNIT_ASSERT_EQUAL(3, schema.Columns_.size());

        TEST_FIELD(schema.Columns_[0], "key", EValueType::VT_INT32);
        TEST_FIELD(schema.Columns_[1], "subkey", EValueType::VT_DOUBLE);
        TEST_FIELD(schema.Columns_[2], "Data", EValueType::VT_STRING);
    }

    Y_UNIT_TEST(KeyColumns) {
        const TKeyColumns keys = {"key", "subkey"};

        const auto schema = CreateTableSchema<NTesting::TAliased>(keys);

        TEST_FIELD_SORTED(schema.Columns_[0], "key", EValueType::VT_INT32);
        TEST_FIELD_SORTED(schema.Columns_[1], "subkey", EValueType::VT_DOUBLE);
        TEST_FIELD(schema.Columns_[2], "Data", EValueType::VT_STRING);
    }

    Y_UNIT_TEST(KeyColumnsReordered) {
        const TKeyColumns keys = {"subkey"};

        const auto schema = CreateTableSchema<NTesting::TAliased>(keys);

        TEST_FIELD_SORTED(schema.Columns_[0], "subkey", EValueType::VT_DOUBLE);
        TEST_FIELD(schema.Columns_[1], "key", EValueType::VT_INT32);
        TEST_FIELD(schema.Columns_[2], "Data", EValueType::VT_STRING);
        UNIT_ASSERT_EQUAL(schema.Columns_.size(), 3);
    }

    Y_UNIT_TEST(KeyColumnsInvalid) {
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NTesting::TAliased>({"subkey", "subkey"}), yexception);
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NTesting::TAliased>({"key", "junk"}), yexception);
    }

    Y_UNIT_TEST(KeepFieldsWithoutExtensionTrue) {
        const auto s = CreateTableSchema<NTesting::TAliased>({}, true);
        TEST_FIELD_PRESENT(s, "key");
        TEST_FIELD_PRESENT(s, "subkey");
        TEST_FIELD_PRESENT(s, "Data");
    }

    Y_UNIT_TEST(KeepFieldsWithoutExtensionFalse) {
        const auto s = CreateTableSchema<NTesting::TAliased>({}, false);
        TEST_FIELD_PRESENT(s, "key");
        TEST_FIELD_PRESENT(s, "subkey");
        TEST_FIELD_NOT_PRESENT(s, "Data");
    }
}

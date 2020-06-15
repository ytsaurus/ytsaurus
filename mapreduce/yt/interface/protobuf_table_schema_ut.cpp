#include "common.h"
#include "errors.h"
#include "common_ut.h"

#include <mapreduce/yt/interface/protobuf_table_schema_ut.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

using namespace NYT;

bool IsFieldPresent(const TTableSchema& schema, TStringBuf name)
{
    for (const auto& field : schema.Columns()) {
        if (field.Name() == name) {
            return true;
        }
    }
    return false;
}

Y_UNIT_TEST_SUITE(ProtoSchemaTest_Simple)
{
    Y_UNIT_TEST(TIntegral)
    {
        const auto schema = CreateTableSchema<NTesting::TIntegral>();

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("DoubleField").Type(ToTypeV3(EValueType::VT_DOUBLE, false)))
            .AddColumn(TColumnSchema().Name("FloatField").Type(ToTypeV3(EValueType::VT_DOUBLE, false)))
            .AddColumn(TColumnSchema().Name("Int32Field").Type(ToTypeV3(EValueType::VT_INT32, false)))
            .AddColumn(TColumnSchema().Name("Int64Field").Type(ToTypeV3(EValueType::VT_INT64, false)))
            .AddColumn(TColumnSchema().Name("Uint32Field").Type(ToTypeV3(EValueType::VT_UINT32, false)))
            .AddColumn(TColumnSchema().Name("Uint64Field").Type(ToTypeV3(EValueType::VT_UINT64, false)))
            .AddColumn(TColumnSchema().Name("Sint32Field").Type(ToTypeV3(EValueType::VT_INT32, false)))
            .AddColumn(TColumnSchema().Name("Sint64Field").Type(ToTypeV3(EValueType::VT_INT64, false)))
            .AddColumn(TColumnSchema().Name("Fixed32Field").Type(ToTypeV3(EValueType::VT_UINT32, false)))
            .AddColumn(TColumnSchema().Name("Fixed64Field").Type(ToTypeV3(EValueType::VT_UINT64, false)))
            .AddColumn(TColumnSchema().Name("Sfixed32Field").Type(ToTypeV3(EValueType::VT_INT32, false)))
            .AddColumn(TColumnSchema().Name("Sfixed64Field").Type(ToTypeV3(EValueType::VT_INT64, false)))
            .AddColumn(TColumnSchema().Name("BoolField").Type(ToTypeV3(EValueType::VT_BOOLEAN, false)))
            .AddColumn(TColumnSchema().Name("EnumField").Type(ToTypeV3(EValueType::VT_STRING, false))));
    }

    Y_UNIT_TEST(TOneOf)
    {
        const auto schema = CreateTableSchema<NTesting::TOneOf>();

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("DoubleField").Type(ToTypeV3(EValueType::VT_DOUBLE, false)))
            .AddColumn(TColumnSchema().Name("Int32Field").Type(ToTypeV3(EValueType::VT_INT32, false)))
            .AddColumn(TColumnSchema().Name("BoolField").Type(ToTypeV3(EValueType::VT_BOOLEAN, false))));
    }

    Y_UNIT_TEST(TWithRequired)
    {
        const auto schema = CreateTableSchema<NTesting::TWithRequired>();

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("RequiredField").Type(ToTypeV3(EValueType::VT_STRING, true)))
            .AddColumn(TColumnSchema().Name("NotRequiredField").Type(ToTypeV3(EValueType::VT_STRING, false))));
    }

    Y_UNIT_TEST(TAggregated)
    {
        const auto schema = CreateTableSchema<NTesting::TAggregated>();

        UNIT_ASSERT_VALUES_EQUAL(6, schema.Columns().size());
        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("StringField").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema().Name("BytesField").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema().Name("NestedField").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema().Name("NestedRepeatedField").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema().Name("NestedOneOfField").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema().Name("NestedRecursiveField").Type(ToTypeV3(EValueType::VT_STRING, false))));
    }

    Y_UNIT_TEST(TAliased)
    {
        const auto schema = CreateTableSchema<NTesting::TAliased>();

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("key").Type(ToTypeV3(EValueType::VT_INT32, false)))
            .AddColumn(TColumnSchema().Name("subkey").Type(ToTypeV3(EValueType::VT_DOUBLE, false)))
            .AddColumn(TColumnSchema().Name("Data").Type(ToTypeV3(EValueType::VT_STRING, false))));
    }

    Y_UNIT_TEST(KeyColumns)
    {
        const TKeyColumns keys = {"key", "subkey"};

        const auto schema = CreateTableSchema<NTesting::TAliased>(keys);

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema()
                .Name("key")
                .Type(ToTypeV3(EValueType::VT_INT32, false))
                .SortOrder(ESortOrder::SO_ASCENDING))
            .AddColumn(TColumnSchema()
                .Name("subkey")
                .Type(ToTypeV3(EValueType::VT_DOUBLE, false))
                .SortOrder(ESortOrder::SO_ASCENDING))
            .AddColumn(TColumnSchema().Name("Data").Type(ToTypeV3(EValueType::VT_STRING, false))));
    }

    Y_UNIT_TEST(KeyColumnsReordered)
    {
        const TKeyColumns keys = {"subkey"};

        const auto schema = CreateTableSchema<NTesting::TAliased>(keys);

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema()
                .Name("subkey")
                .Type(ToTypeV3(EValueType::VT_DOUBLE, false))
                .SortOrder(ESortOrder::SO_ASCENDING))
            .AddColumn(TColumnSchema().Name("key").Type(ToTypeV3(EValueType::VT_INT32, false)))
            .AddColumn(TColumnSchema().Name("Data").Type(ToTypeV3(EValueType::VT_STRING, false))));
    }

    Y_UNIT_TEST(KeyColumnsInvalid)
    {
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NTesting::TAliased>({"subkey", "subkey"}), yexception);
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NTesting::TAliased>({"key", "junk"}), yexception);
    }

    Y_UNIT_TEST(KeepFieldsWithoutExtensionTrue)
    {
        const auto schema = CreateTableSchema<NTesting::TAliased>({}, true);
        UNIT_ASSERT(IsFieldPresent(schema, "key"));
        UNIT_ASSERT(IsFieldPresent(schema, "subkey"));
        UNIT_ASSERT(IsFieldPresent(schema, "Data"));
        UNIT_ASSERT(schema.Strict());
    }

    Y_UNIT_TEST(KeepFieldsWithoutExtensionFalse)
    {
        const auto schema = CreateTableSchema<NTesting::TAliased>({}, false);
        UNIT_ASSERT(IsFieldPresent(schema, "key"));
        UNIT_ASSERT(IsFieldPresent(schema, "subkey"));
        UNIT_ASSERT(!IsFieldPresent(schema, "Data"));
        UNIT_ASSERT(schema.Strict());
    }

    Y_UNIT_TEST(ProtobufTypeOption)
    {
        const auto schema = CreateTableSchema<NTesting::TWithTypeOptions>({});

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .Strict(false)
            .AddColumn(TColumnSchema().Name("ColorIntField").Type(ToTypeV3(EValueType::VT_INT64, false)))
            .AddColumn(TColumnSchema().Name("ColorStringField").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema().Name("AnyField").Type(ToTypeV3(EValueType::VT_ANY, false)))
            .AddColumn(TColumnSchema().Name("EmbeddedField").Type(
                NTi::Optional(NTi::Struct({
                    {"ColorIntField", ToTypeV3(EValueType::VT_INT64, false)},
                    {"ColorStringField", ToTypeV3(EValueType::VT_STRING, false)},
                    {"AnyField", ToTypeV3(EValueType::VT_ANY, false)}}))))
            .AddColumn(TColumnSchema().Name("RepeatedEnumIntField").Type(NTi::List(NTi::Int64()))));
    }

    Y_UNIT_TEST(ProtobufTypeOption_TypeMismatch)
    {
        UNIT_ASSERT_EXCEPTION(
            CreateTableSchema<NTesting::TWithTypeOptions_TypeMismatch_EnumInt>({}),
            yexception);
        UNIT_ASSERT_EXCEPTION(
            CreateTableSchema<NTesting::TWithTypeOptions_TypeMismatch_EnumString>({}),
            yexception);
        UNIT_ASSERT_EXCEPTION(
            CreateTableSchema<NTesting::TWithTypeOptions_TypeMismatch_Any>({}),
            yexception);
        UNIT_ASSERT_EXCEPTION(
            CreateTableSchema<NTesting::TWithTypeOptions_TypeMismatch_OtherColumns>({}),
            yexception);
    }
}

Y_UNIT_TEST_SUITE(ProtoSchemaTest_Complex)
{
    Y_UNIT_TEST(TRepeated)
    {
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NTesting::TRepeated>(), yexception);

        const auto schema = CreateTableSchema<NTesting::TRepeatedYtMode>();
        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("Int32Field").Type(NTi::List(ToTypeV3(EValueType::VT_INT32, true)))));
    }

    Y_UNIT_TEST(TRepeatedOptionalList)
    {
        const auto schema = CreateTableSchema<NTesting::TOptionalList>();
        auto type = NTi::Optional(NTi::List(NTi::Int64()));
        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("OptionalListInt64").TypeV3(type)));
    }

    NTi::TTypePtr GetUrlRowType(bool required)
    {
        static const NTi::TTypePtr structType = NTi::Struct({
            {"Host", ToTypeV3(EValueType::VT_STRING, false)},
            {"Path", ToTypeV3(EValueType::VT_STRING, false)},
            {"HttpCode", ToTypeV3(EValueType::VT_INT32, false)}});
        return required ? structType : NTi::TTypePtr(NTi::Optional(structType));
    }

    Y_UNIT_TEST(TRowFieldSerializationOption)
    {
        const auto schema = CreateTableSchema<NTesting::TRowFieldSerializationOption>();

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("UrlRow_1").Type(GetUrlRowType(false)))
            .AddColumn(TColumnSchema().Name("UrlRow_2").Type(ToTypeV3(EValueType::VT_STRING, false))));
    }

    Y_UNIT_TEST(TRowMessageSerializationOption)
    {
        const auto schema = CreateTableSchema<NTesting::TRowMessageSerializationOption>();

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("UrlRow_1").Type(GetUrlRowType(false)))
            .AddColumn(TColumnSchema().Name("UrlRow_2").Type(GetUrlRowType(false))));
    }

    Y_UNIT_TEST(TRowMixedSerializationOptions)
    {
        const auto schema = CreateTableSchema<NTesting::TRowMixedSerializationOptions>();

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("UrlRow_1").Type(GetUrlRowType(false)))
            .AddColumn(TColumnSchema().Name("UrlRow_2").Type(ToTypeV3(EValueType::VT_STRING, false))));
    }

    NTi::TTypePtr GetUrlRowType_ColumnNames(bool required)
    {
        static const NTi::TTypePtr type = NTi::Struct({
            {"Host_ColumnName", ToTypeV3(EValueType::VT_STRING, false)},
            {"Path_KeyColumnName", ToTypeV3(EValueType::VT_STRING, false)},
            {"HttpCode", ToTypeV3(EValueType::VT_INT32, false)},
        });
        return required ? type : NTi::TTypePtr(NTi::Optional(type));
    }

    Y_UNIT_TEST(TRowMixedSerializationOptions_ColumnNames)
    {
        const auto schema = CreateTableSchema<NTesting::TRowMixedSerializationOptions_ColumnNames>();

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema().Name("UrlRow_1").Type(GetUrlRowType_ColumnNames(false)))
            .AddColumn(TColumnSchema().Name("UrlRow_2").Type(ToTypeV3(EValueType::VT_STRING, false))));
    }

    Y_UNIT_TEST(NoOptionInheritance)
    {
        auto deepestEmbedded = NTi::Optional(NTi::Struct({{"x", ToTypeV3(EValueType::VT_INT64, false)}}));

        const auto schema = CreateTableSchema<NTesting::TNoOptionInheritance>();

        ASSERT_SERIALIZABLES_EQUAL(schema, TTableSchema()
            .AddColumn(TColumnSchema()
                .Name("EmbeddedYt_YtOption")
                .Type(NTi::Optional(NTi::Struct({{"embedded", deepestEmbedded}}))))
            .AddColumn(TColumnSchema().Name("EmbeddedYt_ProtobufOption").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema().Name("EmbeddedYt_NoOption").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema()
                .Name("EmbeddedProtobuf_YtOption")
                .Type(NTi::Optional(NTi::Struct({{"embedded",  ToTypeV3(EValueType::VT_STRING, false)}}))))
            .AddColumn(TColumnSchema().Name("EmbeddedProtobuf_ProtobufOption").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema().Name("EmbeddedProtobuf_NoOption").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema()
                .Name("Embedded_YtOption")
                .Type(NTi::Optional(NTi::Struct({{"embedded",  ToTypeV3(EValueType::VT_STRING, false)}}))))
            .AddColumn(TColumnSchema().Name("Embedded_ProtobufOption").Type(ToTypeV3(EValueType::VT_STRING, false)))
            .AddColumn(TColumnSchema().Name("Embedded_NoOption").Type(ToTypeV3(EValueType::VT_STRING, false))));
    }

    Y_UNIT_TEST(Cyclic)
    {
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NTesting::TCyclic>(), TApiUsageError);
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NTesting::TCyclic::TA>(), TApiUsageError);
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NTesting::TCyclic::TB>(), TApiUsageError);
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NTesting::TCyclic::TC>(), TApiUsageError);
        UNIT_ASSERT_EXCEPTION(CreateTableSchema<NTesting::TCyclic::TD>(), TApiUsageError);

        ASSERT_SERIALIZABLES_EQUAL(
            TTableSchema().AddColumn(
                TColumnSchema().Name("d").TypeV3(NTi::Optional(NTi::String()))),
            CreateTableSchema<NTesting::TCyclic::TE>());
    }
}

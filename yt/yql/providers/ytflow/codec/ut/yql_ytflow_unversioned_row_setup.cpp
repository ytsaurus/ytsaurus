#include "yql_ytflow_unversioned_row_setup.h"

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/yt/memory/new.h>
#include <library/cpp/yt/misc/guid.h>

#include <yt/yt/client/complex_types/uuid_text.h>
#include <yt/yt/client/table_client/helpers.h>
#include <library/cpp/yt/mpl/concepts.h>
#include <library/cpp/yt/mpl/type_traits.h>
#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/writer.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/library/decimal/decimal.h>

#include <util/generic/buffer.h>
#include <util/generic/hash.h>
#include <util/stream/mem.h>
#include <util/stream/str.h>


namespace NYql::NYtflow::NCodec::NTest {

TUnversionedRowSetup::TUnversionedRowSetup()
    : RowBuffer(NYT::New<NYT::NTableClient::TRowBuffer>())
{
}

template <typename TValue>
void TUnversionedRowSetup::SetSimpleValue(
    NYT::NTableClient::TMutableUnversionedRow& mutableUnversionedRow,
    TStringBuf name,
    TValue&& value
) {
    auto columnIndex = YtSchema->GetColumnIndex(name);
    mutableUnversionedRow[columnIndex] = NYT::NTableClient::ToUnversionedValue(
        std::forward<decltype(value)>(value),
        RowBuffer,
        columnIndex,
        NYT::NTableClient::EValueFlags::None);
}

void TUnversionedRowSetup::SetCompositeValue(
    NYT::NTableClient::TMutableUnversionedRow& mutableUnversionedRow,
    TStringBuf name,
    TProduceCallback&& callback
) {
    auto stream = TStringStream();
    auto writer = NYT::NYson::TYsonWriter(&stream);

    callback(NYT::NYTree::BuildYsonFluently(&writer));

    auto columnIndex = YtSchema->GetColumnIndex(name);
    mutableUnversionedRow[columnIndex] = RowBuffer->CaptureValue(
        NYT::NTableClient::MakeUnversionedCompositeValue(
            stream.Str(),
            columnIndex,
            NYT::NTableClient::EValueFlags::None));

}

void TUnversionedRowSetup::SetUnversionedValue(
    NYT::NTableClient::TMutableUnversionedRow& mutableUnversionedRow,
    TStringBuf name,
    NYT::NTableClient::TUnversionedValue&& value
) {
    auto columnIndex = YtSchema->GetColumnIndex(name);
    value.Id = columnIndex;
    mutableUnversionedRow[columnIndex] = RowBuffer->CaptureValue(value);
}

NYT::NTableClient::TUnversionedValue TUnversionedRowSetup::GetUnversionedValue(
    const NYT::NTableClient::TUnversionedRow& unversionedRow,
    TStringBuf name
) const {
    auto columnIndex = YtSchema->GetColumnIndexOrThrow(name);
    return unversionedRow[columnIndex];
}

template <typename TValue>
void TUnversionedRowSetup::AssertSimpleValue(
    const NYT::NTableClient::TUnversionedRow& unversionedRow,
    TStringBuf name,
    TValue&& value
) const {
    auto unversionedValue = GetUnversionedValue(unversionedRow, name);

    if constexpr (NYT::NMpl::COneOf<TValue, i8, i16, i32, i64>) {
        UNIT_ASSERT_EQUAL(
            unversionedValue.Type, NYT::NTableClient::EValueType::Int64);

        UNIT_ASSERT_VALUES_EQUAL(unversionedValue.Data.Int64, value);
    } else if constexpr (NYT::NMpl::COneOf<TValue, ui8, ui16, ui32, ui64>) {
        UNIT_ASSERT_EQUAL(
            unversionedValue.Type, NYT::NTableClient::EValueType::Uint64);

        UNIT_ASSERT_VALUES_EQUAL(unversionedValue.Data.Uint64, value);
    } else if constexpr (NYT::NMpl::COneOf<TValue, float, double>) {
        UNIT_ASSERT_EQUAL(
            unversionedValue.Type, NYT::NTableClient::EValueType::Double);

        UNIT_ASSERT_VALUES_EQUAL(unversionedValue.Data.Double, value);
    } else if constexpr (std::is_same_v<TValue, bool>) {
        UNIT_ASSERT_EQUAL(
            unversionedValue.Type, NYT::NTableClient::EValueType::Boolean);

        UNIT_ASSERT_VALUES_EQUAL(unversionedValue.Data.Boolean, value);
    } else {
        UNIT_ASSERT_EQUAL(
            unversionedValue.Type, NYT::NTableClient::EValueType::String);

        UNIT_ASSERT_VALUES_EQUAL(unversionedValue.AsString(), value);
    }
}

void TUnversionedRowSetup::AssertCompositeValue(
    const NYT::NTableClient::TUnversionedRow& unversionedRow,
    TStringBuf name,
    TConsumeCallback&& consumeCallback
) const {
    auto unversionedValue = GetUnversionedValue(unversionedRow, name);
    UNIT_ASSERT_EQUAL(unversionedValue.Type, NYT::NTableClient::EValueType::Composite);

    auto stream = TMemoryInput(unversionedValue.AsStringBuf());
    auto parser = NYT::NYson::TYsonPullParser(&stream, NYT::NYson::EYsonType::ListFragment);

    consumeCallback(parser);

    auto ysonItem = parser.Next();
    UNIT_ASSERT(ysonItem.IsEndOfStream());
}

TUnversionedRowSetupFull::TUnversionedRowSetupFull()
{
    YtSchema = BuildYtSchema();
}

NYT::NTableClient::TTableSchemaPtr TUnversionedRowSetupFull::BuildYtSchema() {
    return NYT::NYTree::ConvertTo<
        NYT::NTableClient::TTableSchemaPtr
    >(NYT::NYson::TYsonString(TString(R"""(
        [
            {name="string_field";type="string";required=%true};
            {name="uuid_field";type="uuid";required=%true};
            {name="json_field";type="json";required=%true};
            {name="utf8_field";type="utf8";required=%true};
            {name="int64_field";type="int64";required=%true};
            {name="int32_field";type="int32";required=%true};
            {name="int16_field";type="int16";required=%true};
            {name="int8_field";type="int8";required=%true};
            {name="uint64_field";type="uint64";required=%true};
            {name="uint32_field";type="uint32";required=%true};
            {name="uint16_field";type="uint16";required=%true};
            {name="uint8_field";type="uint8";required=%true};
            {name="double_field";type="double";required=%true};
            {name="float_field";type="float";required=%true};
            {name="bool_field";type="boolean";required=%true};
            {name="yson_field";type="any"};
            {name="date_field";type="date";required=%true};
            {name="datetime_field";type="datetime";required=%true};
            {name="timestamp_field";type="timestamp";required=%true};
            {name="interval_field";type="interval";required=%true};
            {name="date32_field";type="date32";required=%true};
            {name="datetime64_field";type="datetime64";required=%true};
            {name="timestamp64_field";type="timestamp64";required=%true};
            {name="interval64_field";type="interval64";required=%true};
            {
                name="decimal_field";type_v3={
                    type_name="decimal";
                    precision=5;
                    scale=2;
                };
                required=%true
            };
            {
                name="tuple_field";type_v3={
                    type_name="tuple";
                    elements=[{type="string"};{type="int32"};];
                };
                required=%true
            };
            {
                name="struct_field";type_v3={
                    type_name="struct";
                    members=[
                        {name="nested_string_field";type="string"};
                        {name="nested_int32_field";type="int32"};
                        {name="nested_yson_field";type="yson"};
                        {
                            name="nested_optional_field";
                            type={
                                type_name="optional";
                                item={type_name="string"};
                            };
                        };
                        {
                            name="nested_empty_optional_field";
                            type={
                                type_name="optional";
                                item={type_name="string"};
                            };
                        };
                        {
                            name="nested_doubly_optional_field";
                            type={
                                type_name="optional";
                                item={
                                    type_name="optional";
                                    item={type_name="string"};
                                };
                            };
                        };
                        {
                            name="nested_empty_doubly_optional_field";
                            type={
                                type_name="optional";
                                item={
                                    type_name="optional";
                                    item={type_name="string"};
                                };
                            };
                        };
                    ];
                };
                required=%true
            };
            {
                name="list_field";type_v3={
                    type_name="list";
                    item={type_name="string"};
                };
                required=%true
            };
            {
                name="optional_field";type_v3={
                    type_name="optional";
                    item={type_name="string"};
                };
            };
            {
                name="empty_optional_field";type_v3={
                    type_name="optional";
                    item={type_name="string"};
                };
            };
            {
                name="dict_field";type_v3={
                    type_name="dict";
                    key={type_name="int64"};
                    value={type_name="string"};
                };
                required=%true
            };
            {
                name="void_field";type_v3={
                    type_name="void";
                };
            };
            {
                name="null_field";type_v3={
                    type_name="null";
                };
            };
            {
                name="tagged_field";type_v3={
                    type_name="tagged";
                    tag="custom_tag";
                    item={type_name="string"};
                };
                required=%true
            };
            {
                name="variant_tuple_field";type_v3={
                    type_name="variant";
                    elements=[{type="string"};{type="int32"};];
                };
                required=%true
            };
            {
                name="variant_struct_field";type_v3={
                    type_name="variant";
                    members=[
                        {name="variant_string_field";type="string"};
                        {name="variant_int32_field";type="int32"};
                        {name="variant_yson_field";type="yson"};
                    ];
                };
                required=%true
            };
        ]
    )""")));
}

NYT::NTableClient::TUnversionedRow TUnversionedRowSetupFull::BuildUnversionedRow() {
    auto mutableUnversionedRow = RowBuffer->AllocateUnversioned(
        YtSchema->GetColumnCount());

    auto setSimpleValue = [&](TStringBuf name, auto&& value) {
        return SetSimpleValue(
            mutableUnversionedRow,
            name,
            std::forward<decltype(value)>(value));
    };

    auto setCompositeValue = [&](TStringBuf name, auto produceCallback) {
        return SetCompositeValue(mutableUnversionedRow, name, produceCallback);
    };

    auto setUnversionedValue = [&](TStringBuf name, NYT::NTableClient::TUnversionedValue&& value) {
        return SetUnversionedValue(mutableUnversionedRow, name, std::move(value));
    };

    setSimpleValue("string_field", "foobar");

    {
        // order of 0 and 1 is reversed due to truncate of leading zeroes
        auto guid = NYT::TGuid::FromString("10234567-89abcdef-10234567-89abcdef");

        TBuffer buffer(16);
        NYT::NComplexTypes::GuidToBytes(guid, buffer.data());
        buffer.Advance(16);

        TString binaryGuid;
        buffer.AsString(binaryGuid);

        setSimpleValue("uuid_field", binaryGuid);
    }

    setSimpleValue("json_field", "{}");
    setSimpleValue("utf8_field", "поиск");

    setSimpleValue("int64_field", static_cast<i64>(1));
    setSimpleValue("int32_field", static_cast<i32>(2));
    setSimpleValue("int16_field", static_cast<i16>(3));
    setSimpleValue("int8_field", static_cast<i8>(4));
    setSimpleValue("uint64_field", static_cast<ui64>(5));
    setSimpleValue("uint32_field", static_cast<ui32>(6));
    setSimpleValue("uint16_field", static_cast<ui16>(7));
    setSimpleValue("uint8_field", static_cast<ui8>(8));

    setSimpleValue("double_field", 1.1);
    setSimpleValue("float_field", 2.2f);

    setSimpleValue("bool_field", true);

    setUnversionedValue("yson_field", NYT::NTableClient::MakeUnversionedAnyValue("#"));

    setSimpleValue("date_field", static_cast<ui64>(9));
    setSimpleValue("datetime_field", static_cast<ui64>(10));
    setSimpleValue("timestamp_field", static_cast<ui64>(11));
    setSimpleValue("interval_field", static_cast<i64>(12));

    setSimpleValue("date32_field", static_cast<i64>(13));
    setSimpleValue("datetime64_field", static_cast<i64>(14));
    setSimpleValue("timestamp64_field", static_cast<i64>(15));
    setSimpleValue("interval64_field", static_cast<i64>(16));

    setSimpleValue(
        "decimal_field", NYT::NDecimal::TDecimal::TextToBinary("123.45", 5, 2));

    setCompositeValue("tuple_field", [](auto fluentBuilder) {
        fluentBuilder
            .BeginList()
                .Item()
                    .Value("foobar")
                .Item()
                    .Value(42)
            .EndList();
    });

    setCompositeValue("struct_field", [](auto fluentBuilder) {
        fluentBuilder
            .BeginList()
                .Item()
                    .Value("foo")
                .Item()
                    .Value(42)
                .Item()
                    .Value("[24]")
                .Item()
                    .Value("baz")
                .Item()
                    .Entity()
                .Item()
                    .BeginList()
                        .Item()
                            .Value("opbaz")
                    .EndList()
                .Item()
                    .BeginList()
                        .Item()
                            .Entity()
                    .EndList()
            .EndList();
    });

    setCompositeValue("list_field", [](auto fluentBuilder) {
        fluentBuilder
            .BeginList()
                .Item()
                    .Value("foo")
                .Item()
                    .Value("bar")
            .EndList();
    });

    setSimpleValue("optional_field", "foobar");
    setUnversionedValue(
        "empty_optional_field",
        NYT::NTableClient::MakeUnversionedNullValue());

    setCompositeValue("dict_field", [](auto fluentBuilder) {
        fluentBuilder
            .BeginList()
                .Item()
                    .BeginList()
                        .Item()
                            .Value(24)
                        .Item()
                            .Value("foo")
                    .EndList()
                .Item()
                    .BeginList()
                        .Item()
                            .Value(42)
                        .Item()
                            .Value("bar")
                    .EndList()
            .EndList();
    });

    setUnversionedValue(
        "void_field",
        NYT::NTableClient::MakeUnversionedNullValue());

    setUnversionedValue(
        "null_field",
        NYT::NTableClient::MakeUnversionedNullValue());

    setSimpleValue("tagged_field", "foo");

    setCompositeValue("variant_tuple_field", [](auto fluentBuilder) {
        fluentBuilder
            .BeginList()
                .Item()
                    .Value(1)
                .Item()
                    .Value(42)
            .EndList();
    });

    setCompositeValue("variant_struct_field", [](auto fluentBuilder) {
        fluentBuilder
            .BeginList()
                .Item()
                    .Value(0)
                .Item()
                    .Value("foobar")
            .EndList();
    });

    return mutableUnversionedRow;
}

void TUnversionedRowSetupFull::AssertExpectedUnversionedRow(
    NYT::NTableClient::TUnversionedRow unversionedRow
) const {
    UNIT_ASSERT_VALUES_EQUAL(unversionedRow.GetCount(), YtSchema->GetColumnCount());

    auto getUnversionedValue = [&](TStringBuf name) {
        return GetUnversionedValue(unversionedRow, name);
    };

    auto assertSimpleValue = [&](TStringBuf name, auto&& value) {
        return AssertSimpleValue(
            unversionedRow,
            name,
            std::forward<decltype(value)>(value));
    };

    auto assertCompositeValue = [&](TStringBuf name, auto consumeCallback) {
        return AssertCompositeValue(unversionedRow, name, consumeCallback);
    };

    assertSimpleValue("string_field", "foobar");

    {
        auto unversionedValue = getUnversionedValue("uuid_field");
        UNIT_ASSERT_EQUAL(unversionedValue.Type, NYT::NTableClient::EValueType::String);

        auto guid = NYT::NComplexTypes::GuidFromBytes(unversionedValue.AsStringBuf());
        auto formattableGuid = NYT::TFormattableGuid(guid);

        UNIT_ASSERT_VALUES_EQUAL(
            formattableGuid.ToStringBuf(), "10234567-89abcdef-10234567-89abcdef");
    }

    assertSimpleValue("json_field", "{}");
    assertSimpleValue("utf8_field", "поиск");

    assertSimpleValue("int64_field", static_cast<i64>(1));
    assertSimpleValue("int32_field", static_cast<i32>(2));
    assertSimpleValue("int16_field", static_cast<i16>(3));
    assertSimpleValue("int8_field", static_cast<i8>(4));
    assertSimpleValue("uint64_field", static_cast<ui64>(5));
    assertSimpleValue("uint32_field", static_cast<ui32>(6));
    assertSimpleValue("uint16_field", static_cast<ui16>(7));
    assertSimpleValue("uint8_field", static_cast<ui8>(8));

    assertSimpleValue("double_field", 1.1);
    assertSimpleValue("float_field", 2.2f);

    assertSimpleValue("bool_field", true);

    {
        auto unversionedValue = getUnversionedValue("yson_field");
        UNIT_ASSERT_EQUAL(unversionedValue.Type, NYT::NTableClient::EValueType::Any);

        UNIT_ASSERT_VALUES_EQUAL(unversionedValue.AsStringBuf(), "#");
    }

    assertSimpleValue("date_field", static_cast<ui64>(9));
    assertSimpleValue("datetime_field", static_cast<ui64>(10));
    assertSimpleValue("timestamp_field", static_cast<ui64>(11));
    assertSimpleValue("interval_field", static_cast<i64>(12));

    assertSimpleValue("date32_field", static_cast<i64>(13));
    assertSimpleValue("datetime64_field", static_cast<i64>(14));
    assertSimpleValue("timestamp64_field", static_cast<i64>(15));
    assertSimpleValue("interval64_field", static_cast<i64>(16));

    {
        auto unversionedValue = getUnversionedValue("decimal_field");
        UNIT_ASSERT_EQUAL(unversionedValue.Type, NYT::NTableClient::EValueType::String);

        auto decimalValue = NYT::NDecimal::TDecimal::BinaryToText(
            unversionedValue.AsStringBuf(), 5, 2);

        UNIT_ASSERT_VALUES_EQUAL(decimalValue, "123.45");
    }

    assertCompositeValue("tuple_field", [](auto& parser) {
        parser.ParseBeginList();

        UNIT_ASSERT_VALUES_EQUAL(parser.ParseString(), "foobar");
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt64(), 42);

        parser.ParseEndList();
    });

    assertCompositeValue("struct_field", [](auto& parser) {
        parser.ParseBeginList();

        UNIT_ASSERT_VALUES_EQUAL(parser.ParseString(), "foo");
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt64(), 42);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseString(), "[24]");

        UNIT_ASSERT_VALUES_EQUAL(parser.ParseString(), "baz");
        parser.ParseEntity();

        parser.ParseBeginList();
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseString(), "opbaz");
        parser.ParseEndList();

        parser.ParseBeginList();
        parser.ParseEntity();
        parser.ParseEndList();

        parser.ParseEndList();
    });

    assertCompositeValue("list_field", [](auto& parser) {
        parser.ParseBeginList();

        UNIT_ASSERT_VALUES_EQUAL(parser.ParseString(), "foo");
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseString(), "bar");

        parser.ParseEndList();
    });

    assertSimpleValue("optional_field", "foobar");

    {
        auto unversionedValue = getUnversionedValue("empty_optional_field");
        UNIT_ASSERT_EQUAL(unversionedValue.Type, NYT::NTableClient::EValueType::Null);
    }

    assertCompositeValue("dict_field", [](auto& parser) {
        parser.ParseBeginList();

        THashMap<i64, TString> dict;

        while (!parser.IsEndList()) {
            parser.ParseBeginList();

            i64 key = parser.ParseInt64();
            auto value = TString(parser.ParseString());
            dict[key] = std::move(value);

            parser.ParseEndList();
        }

        parser.ParseEndList();

        UNIT_ASSERT_VALUES_EQUAL(dict.size(), 2);

        {
            i64 key = 24;
            UNIT_ASSERT(dict.contains(key));
            UNIT_ASSERT_VALUES_EQUAL(dict[key], "foo");
        }

        {
            i64 key = 42;
            UNIT_ASSERT(dict.contains(key));
            UNIT_ASSERT_VALUES_EQUAL(dict[key], "bar");
        }
    });

    {
        auto unversionedValue = getUnversionedValue("void_field");
        UNIT_ASSERT_EQUAL(unversionedValue.Type, NYT::NTableClient::EValueType::Null);
    }

    {
        auto unversionedValue = getUnversionedValue("null_field");
        UNIT_ASSERT_EQUAL(unversionedValue.Type, NYT::NTableClient::EValueType::Null);
    }

    assertSimpleValue("tagged_field", "foo");

    assertCompositeValue("variant_tuple_field", [](auto& parser) {
        parser.ParseBeginList();

        UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt64(), 1);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt64(), 42);

        parser.ParseEndList();
    });

    assertCompositeValue("variant_struct_field", [](auto& parser) {
        parser.ParseBeginList();

        UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt64(), 0);
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseString(), "foobar");

        parser.ParseEndList();
    });
}

TUnversionedRowSetupLarge::TUnversionedRowSetupLarge()
{
    YtSchema = BuildYtSchema();
}

NYT::NTableClient::TTableSchemaPtr TUnversionedRowSetupLarge::BuildYtSchema() {
    return NYT::NYTree::ConvertTo<
        NYT::NTableClient::TTableSchemaPtr
    >(NYT::NYson::TYsonString(TString(R"""(
        [
            {name="string_field";type="string";required=%false};
            {name="int64_field";type="int64";required=%false};
            {name="int32_field";type="int32";required=%false};
            {name="int16_field";type="int16";required=%false};
            {
                name="struct_field";type_v3={
                    type_name="struct";
                    members=[
                        {
                            name="nested_string_field";
                            type={
                                type_name="optional";
                                item={type_name="string"};
                            };
                        };
                        {
                            name="nested_int32_field";
                            type={
                                type_name="optional";
                                item={type_name="int32"};
                            };
                        };
                    ];
                };
            };
        ]
    )""")));
}

NYT::NTableClient::TUnversionedRow TUnversionedRowSetupLarge::BuildUnversionedRow() {
    auto mutableUnversionedRow = RowBuffer->AllocateUnversioned(
        YtSchema->GetColumnCount());

    auto setSimpleValue = [&](TStringBuf name, auto&& value) {
        return SetSimpleValue(
            mutableUnversionedRow,
            name,
            std::forward<decltype(value)>(value));
    };

    auto setCompositeValue = [&](TStringBuf name, auto produceCallback) {
        return SetCompositeValue(mutableUnversionedRow, name, produceCallback);
    };

    setSimpleValue("string_field", "foobar");
    setSimpleValue("int64_field", static_cast<i64>(1));
    setSimpleValue("int32_field", static_cast<i32>(2));
    setSimpleValue("int16_field", static_cast<i16>(3));

    setCompositeValue("struct_field", [](auto fluentBuilder) {
        fluentBuilder
            .BeginList()
                .Item()
                    .Value("foo")
                .Item()
                    .Value(42)
            .EndList();
    });

    return mutableUnversionedRow;
}

void TUnversionedRowSetupLarge::AssertExpectedUnversionedRow(
    NYT::NTableClient::TUnversionedRow unversionedRow
) const {
    UNIT_ASSERT_VALUES_EQUAL(unversionedRow.GetCount(), YtSchema->GetColumnCount());

    auto assertSimpleValue = [&](TStringBuf name, auto&& value) {
        return AssertSimpleValue(
            unversionedRow,
            name,
            std::forward<decltype(value)>(value));
    };

    auto assertCompositeValue = [&](TStringBuf name, auto consumeCallback) {
        return AssertCompositeValue(unversionedRow, name, consumeCallback);
    };

    assertSimpleValue("string_field", "foobar");
    assertSimpleValue("int64_field", static_cast<i64>(1));
    assertSimpleValue("int32_field", static_cast<i32>(2));
    assertSimpleValue("int16_field", static_cast<i16>(3));

    assertCompositeValue("struct_field", [](auto& parser) {
        parser.ParseBeginList();

        UNIT_ASSERT_VALUES_EQUAL(parser.ParseString(), "foo");
        UNIT_ASSERT_VALUES_EQUAL(parser.ParseInt64(), 42);

        parser.ParseEndList();
    });
}

TUnversionedRowSetupLargeOptional::TUnversionedRowSetupLargeOptional()
{
    YtSchema = BuildYtSchema();
}

NYT::NTableClient::TTableSchemaPtr TUnversionedRowSetupLargeOptional::BuildYtSchema() {
    return NYT::NYTree::ConvertTo<
        NYT::NTableClient::TTableSchemaPtr
    >(NYT::NYson::TYsonString(TString(R"""(
        [
            {name="string_field";type="string";required=%false};
            {name="int64_field";type="int64";required=%false};
            {name="int32_field";type="int32";required=%false};
            {name="int16_field";type="int16";required=%false};
            {
                name="struct_field";type_v3={
                    type_name="struct";
                    members=[
                        {
                            name="nested_string_field";
                            type={
                                type_name="optional";
                                item={type_name="string"};
                            };
                        };
                        {
                            name="nested_int32_field";
                            type={
                                type_name="optional";
                                item={type_name="int32"};
                            };
                        };
                    ];
                };
            };
        ]
    )""")));
}

NYT::NTableClient::TUnversionedRow TUnversionedRowSetupLargeOptional::BuildUnversionedRow() {
    auto mutableUnversionedRow = RowBuffer->AllocateUnversioned(
        YtSchema->GetColumnCount());

    auto setSimpleValue = [&](TStringBuf name, auto&& value) {
        return SetSimpleValue(
            mutableUnversionedRow,
            name,
            std::forward<decltype(value)>(value));
    };

    auto setCompositeValue = [&](TStringBuf name, auto produceCallback) {
        return SetCompositeValue(mutableUnversionedRow, name, produceCallback);
    };

    auto setEntity = [&](TStringBuf name) {
        auto columnIndex = YtSchema->GetColumnIndex(name);
        mutableUnversionedRow[columnIndex] = NYT::NTableClient::MakeUnversionedNullValue(
            columnIndex,
            NYT::NTableClient::EValueFlags::None);
    };

    setSimpleValue("string_field", "foobar");
    setSimpleValue("int64_field", static_cast<i64>(1));
    setEntity("int32_field");
    setEntity("int16_field");

    setCompositeValue("struct_field", [](auto fluentBuilder) {
        fluentBuilder
            .BeginList()
                .Item()
                    .Value("foo")
                .Item()
                    .Entity()
            .EndList();
    });

    return mutableUnversionedRow;
}

void TUnversionedRowSetupLargeOptional::AssertExpectedUnversionedRow(
    NYT::NTableClient::TUnversionedRow unversionedRow
) const {
    UNIT_ASSERT_VALUES_EQUAL(unversionedRow.GetCount(), YtSchema->GetColumnCount());

    auto assertSimpleValue = [&](TStringBuf name, auto&& value) {
        return AssertSimpleValue(
            unversionedRow,
            name,
            std::forward<decltype(value)>(value));
    };

    auto assertCompositeValue = [&](TStringBuf name, auto consumeCallback) {
        return AssertCompositeValue(unversionedRow, name, consumeCallback);
    };

    auto assertEntity = [&](TStringBuf name) {
        auto columnIndex = YtSchema->GetColumnIndexOrThrow(name);
        UNIT_ASSERT(
            unversionedRow[columnIndex].Type == NYT::NTableClient::EValueType::Null);
    };

    assertSimpleValue("string_field", "foobar");
    assertSimpleValue("int64_field", static_cast<i64>(1));
    assertEntity("int32_field");
    assertEntity("int16_field");

    assertCompositeValue("struct_field", [](auto& parser) {
        parser.ParseBeginList();

        UNIT_ASSERT_VALUES_EQUAL(parser.ParseString(), "foo");
        parser.ParseEntity();

        parser.ParseEndList();
    });
}

TUnversionedRowSetupSmall::TUnversionedRowSetupSmall()
{
    YtSchema = BuildYtSchema();
}

NYT::NTableClient::TTableSchemaPtr TUnversionedRowSetupSmall::BuildYtSchema() {
    return NYT::NYTree::ConvertTo<
        NYT::NTableClient::TTableSchemaPtr
    >(NYT::NYson::TYsonString(TString(R"""(
        [
            {name="string_field";type="string";required=%false};
            {name="int64_field";type="int64";required=%false};
            {
                name="struct_field";type_v3={
                    type_name="struct";
                    members=[
                        {
                            name="nested_string_field";
                            type={
                                type_name="optional";
                                item={type_name="string"};
                            };
                        };
                    ];
                };
            };
        ]
    )""")));
}

NYT::NTableClient::TUnversionedRow TUnversionedRowSetupSmall::BuildUnversionedRow() {
    auto mutableUnversionedRow = RowBuffer->AllocateUnversioned(
        YtSchema->GetColumnCount());

    auto setSimpleValue = [&](TStringBuf name, auto&& value) {
        return SetSimpleValue(
            mutableUnversionedRow,
            name,
            std::forward<decltype(value)>(value));
    };

    auto setCompositeValue = [&](TStringBuf name, auto produceCallback) {
        return SetCompositeValue(mutableUnversionedRow, name, produceCallback);
    };

    setSimpleValue("string_field", "foobar");
    setSimpleValue("int64_field", static_cast<i64>(1));

    setCompositeValue("struct_field", [](auto fluentBuilder) {
        fluentBuilder
            .BeginList()
                .Item()
                    .Value("foo")
            .EndList();
    });

    return mutableUnversionedRow;
}

void TUnversionedRowSetupSmall::AssertExpectedUnversionedRow(
    NYT::NTableClient::TUnversionedRow unversionedRow
) const {
    UNIT_ASSERT_VALUES_EQUAL(unversionedRow.GetCount(), YtSchema->GetColumnCount());

    auto assertSimpleValue = [&](TStringBuf name, auto&& value) {
        return AssertSimpleValue(
            unversionedRow,
            name,
            std::forward<decltype(value)>(value));
    };

    auto assertCompositeValue = [&](TStringBuf name, auto consumeCallback) {
        return AssertCompositeValue(unversionedRow, name, consumeCallback);
    };

    assertSimpleValue("string_field", "foobar");
    assertSimpleValue("int64_field", static_cast<i64>(1));

    assertCompositeValue("struct_field", [](auto& parser) {
        parser.ParseBeginList();

        UNIT_ASSERT_VALUES_EQUAL(parser.ParseString(), "foo");

        parser.ParseEndList();
    });
}

} // namespace NYql::NYtflow::NCodec::NTest

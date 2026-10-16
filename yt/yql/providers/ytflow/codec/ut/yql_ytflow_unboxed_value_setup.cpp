#include "yql_ytflow_unboxed_value_setup.h"

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/types/uuid/uuid.h>

#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/public/decimal/yql_decimal_serialize.h>
#include <yql/essentials/utils/yql_panic.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>

#include <type_traits>


namespace NYql::NYtflow::NCodec::NTest {

TUnboxedValueSetup::TUnboxedValueSetup()
    : Alloc(__LOCATION__)
    , TypeEnv(Alloc)
    , TypeBuilder(TypeEnv)
    , MemUsage("ytflow_codec_test")
    , HolderFactory(Alloc.Ref(), MemUsage)
    , ValueBuilder(MakeHolder<NKikimr::NMiniKQL::TDefaultValueBuilder>(HolderFactory))
    , TypeInfoHelper(new NKikimr::NMiniKQL::TTypeInfoHelper{})
    , RuntimeSettings(NYql::MakeRuntimeSettings())
    , FunctionTypeInfoBuilder(
        new NKikimr::NMiniKQL::TFunctionTypeInfoBuilder(
            UnknownLangVersion, *RuntimeSettings, TypeEnv, TypeInfoHelper, "ytflow_codec_test_module", nullptr,
            NYql::NUdf::TSourcePosition()))
{
}

void TUnboxedValueSetup::SetUnboxedValue(
    NYql::NUdf::TUnboxedValue* items,
    TStringBuf name,
    NYql::NUdf::TUnboxedValue&& unboxedValue
) {
    auto memberIndex = Type->GetMemberIndex(name);
    auto memberType = Type->GetMemberType(memberIndex);

    if (memberType->IsData()) {
        auto* dataType = static_cast<const NKikimr::NMiniKQL::TDataType*>(memberType);

        if (auto dataSlot = dataType->GetDataSlot()) {
            YQL_ENSURE(NKikimr::NMiniKQL::IsValidValue(*dataSlot, unboxedValue));
        }
    }

    items[memberIndex] = std::move(unboxedValue);
}

template <typename TValue>
void TUnboxedValueSetup::SetSimpleValue(
    NYql::NUdf::TUnboxedValue* items,
    TStringBuf name,
    TValue&& value
) {
    SetUnboxedValue(
        items,
        name,
        NYql::NUdf::TUnboxedValuePod(std::forward<decltype(value)>(value)));
}

template <typename TValue>
void TUnboxedValueSetup::SetStringValue(
    NYql::NUdf::TUnboxedValue* items,
    TStringBuf name,
    TValue&& value
) {
    NYql::NUdf::TUnboxedValue unboxedValue;

    if constexpr (std::is_same_v<std::decay_t<TValue>, NYql::NUdf::TStringRef>) {
        unboxedValue = ValueBuilder->NewString(std::forward<TValue>(value));
    } else {
        unboxedValue = ValueBuilder->NewString(
            NYql::NUdf::TStringRef::Of(std::forward<TValue>(value)));
    }

    SetUnboxedValue(items, name, std::move(unboxedValue));
}

NYql::NUdf::TUnboxedValue TUnboxedValueSetup::GetMember(
    const NYql::NUdf::TUnboxedValue& unboxedValue,
    const NKikimr::NMiniKQL::TStructType* structType,
    TStringBuf name
) const {
    auto memberIndex = structType->GetMemberIndex(name);
    return unboxedValue.GetElement(memberIndex);
}

void TUnboxedValueSetup::AssertStringUnboxedValue(
    const NYql::NUdf::TUnboxedValue& unboxedValue,
    TStringBuf value
) const {
    auto stringValue = TStringBuf(unboxedValue.AsStringRef());
    UNIT_ASSERT_STRINGS_EQUAL(stringValue, value);
}

void TUnboxedValueSetup::AssertStringValue(
    const NYql::NUdf::TUnboxedValue& unboxedValue,
    TStringBuf name,
    TStringBuf value
) const {
    AssertStringUnboxedValue(GetMember(unboxedValue, Type, name), value);
}

template <typename TValue>
void TUnboxedValueSetup::AssertSimpleUnboxedValue(
    const NYql::NUdf::TUnboxedValue& unboxedValue,
    TValue&& value
) const {
    auto nativeValue = unboxedValue.Get<TValue>();

    if constexpr (std::is_same_v<TValue, NYql::NDecimal::TInt128>) {
        UNIT_ASSERT_EQUAL(nativeValue, value);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(nativeValue, value);
    }
}

template <typename TValue>
void TUnboxedValueSetup::AssertSimpleValue(
    const NYql::NUdf::TUnboxedValue& unboxedValue,
    TStringBuf name,
    TValue&& value
) const {
    AssertSimpleUnboxedValue(
        GetMember(unboxedValue, Type, name),
        std::forward<decltype(value)>(value));
}

TUnboxedValueSetupFull::TUnboxedValueSetupFull()
{
    Type = static_cast<const NKikimr::NMiniKQL::TStructType*>(BuildType());
}

const NKikimr::NMiniKQL::TType* TUnboxedValueSetupFull::BuildType() {
    return TypeBuilder.NewStructType({
        {"string_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::String)},
        {"uuid_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Uuid)},
        {"json_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Json)},
        {"utf8_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Utf8)},
        {"int64_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Int64)},
        {"int32_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Int32)},
        {"int16_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Int16)},
        {"int8_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Int8)},
        {"uint64_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Uint64)},
        {"uint32_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Uint32)},
        {"uint16_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Uint16)},
        {"uint8_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Uint8)},
        {"double_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Double)},
        {"float_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Float)},
        {"bool_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Bool)},
        {"yson_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Yson)},
        {"date_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Date)},
        {"datetime_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Datetime)},
        {"timestamp_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Timestamp)},
        {"interval_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Interval)},
        {"date32_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Date32)},
        {"datetime64_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Datetime64)},
        {"timestamp64_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Timestamp64)},
        {"interval64_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Interval64)},
        {"decimal_field", TypeBuilder.NewDecimalType(5, 2)},
        {"tuple_field", TypeBuilder.NewTupleType({
            TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::String),
            TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Int32)
        })},
        {"struct_field", TypeBuilder.NewStructType({
            {"nested_string_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::String)},
            {"nested_int32_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Int32)},
            {"nested_yson_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Yson)},
            {"nested_optional_field", TypeBuilder.NewOptionalType(
                TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::String))},
            {"nested_empty_optional_field", TypeBuilder.NewOptionalType(
                TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::String))},
            {"nested_doubly_optional_field", TypeBuilder.NewOptionalType(
                TypeBuilder.NewOptionalType(
                    TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::String)))},
            {"nested_empty_doubly_optional_field", TypeBuilder.NewOptionalType(
                TypeBuilder.NewOptionalType(
                    TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::String)))}
        })},
        {"list_field", TypeBuilder.NewListType(
            TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::String)
        )},
        {"optional_field", TypeBuilder.NewOptionalType(
            TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::String)
        )},
        {"empty_optional_field", TypeBuilder.NewOptionalType(
            TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::String)
        )},
        {"dict_field", TypeBuilder.NewDictType(
            TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Int64),
            TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::String),
            /*multi*/ false
        )},
        {"void_field", TypeBuilder.NewVoidType()},
        {"null_field", TypeBuilder.NewNullType()},
        {"tagged_field", TypeBuilder.NewTaggedType(
            TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::String),
            "custom_tag"
        )},
        {"variant_tuple_field", TypeBuilder.NewVariantType(
            TypeBuilder.NewTupleType({
                TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::String),
                TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Int32)
            })
        )},
        {"variant_struct_field", TypeBuilder.NewVariantType(
            TypeBuilder.NewStructType({
                {"variant_string_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::String)},
                {"variant_int32_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Int32)},
                {"variant_yson_field", TypeBuilder.NewDataType(NYql::NUdf::EDataSlot::Yson)}
            })
        )}
    });
}

NYql::NUdf::TUnboxedValue TUnboxedValueSetupFull::BuildUnboxedValue() {
    NYql::NUdf::TUnboxedValue* items;
    auto unboxedValue = ValueBuilder->NewArray(Type->GetMembersCount(), items);

    auto setUnboxedValue = [&](
        TStringBuf name, NYql::NUdf::TUnboxedValue&& unboxedValue
    ) {
        return SetUnboxedValue(items, name, std::move(unboxedValue));
    };

    auto setSimpleValue = [&](TStringBuf name, auto&& value) {
        return SetSimpleValue(items, name, std::forward<decltype(value)>(value));
    };

    auto setStringValue = [&](TStringBuf name, auto&& value) {
        return SetStringValue(items, name, std::forward<decltype(value)>(value));
    };

    setStringValue("string_field", "foobar");

    {
        // order of 0 and 1 is reversed due to truncate of leading zeroes
        auto uuidUnboxedValue = NKikimr::NMiniKQL::ParseUuid(
            NYql::NUdf::TStringRef::Of("10234567-89ab-cdef-1023-456789abcdef"));

        setUnboxedValue("uuid_field", std::move(uuidUnboxedValue));
    }

    setStringValue("json_field", "{}");
    setStringValue("utf8_field", "поиск");

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
    setStringValue("yson_field", "#");

    setSimpleValue("date_field", static_cast<ui16>(9));
    setSimpleValue("datetime_field", static_cast<ui32>(10));
    setSimpleValue("timestamp_field", static_cast<ui64>(11));
    setSimpleValue("interval_field", static_cast<i64>(12));

    setSimpleValue("date32_field", static_cast<i32>(13));
    setSimpleValue("datetime64_field", static_cast<i64>(14));
    setSimpleValue("timestamp64_field", static_cast<i64>(15));
    setSimpleValue("interval64_field", static_cast<i64>(16));
    setSimpleValue("decimal_field", static_cast<NYql::NDecimal::TInt128>(12345));

    {
        auto memberIndex = Type->GetMemberIndex("tuple_field");

        NYql::NUdf::TUnboxedValue memberItems[] = {
            ValueBuilder->NewString(NYql::NUdf::TStringRef::Of("foobar")),
            NYql::NUdf::TUnboxedValuePod(static_cast<i32>(42)),
        };

        items[memberIndex] = ValueBuilder->NewList(memberItems, 2);
    }

    {
        auto memberIndex = Type->GetMemberIndex("struct_field");
        auto* memberStructType = static_cast<const NKikimr::NMiniKQL::TStructType*>(
            Type->GetMemberType(memberIndex));

        NYql::NUdf::TUnboxedValue* memberItems;
        auto memberUnboxedValue = ValueBuilder->NewArray(
            memberStructType->GetMembersCount(), memberItems);

        memberItems[memberStructType->GetMemberIndex("nested_string_field")] =
            ValueBuilder->NewString(NYql::NUdf::TStringRef::Of("foo"));

        memberItems[memberStructType->GetMemberIndex("nested_int32_field")] =
            NYql::NUdf::TUnboxedValuePod(static_cast<i32>(42));

        memberItems[memberStructType->GetMemberIndex("nested_yson_field")] =
            ValueBuilder->NewString(NYql::NUdf::TStringRef::Of("[24]"));

        memberItems[memberStructType->GetMemberIndex("nested_optional_field")] =
            ValueBuilder->NewString(NYql::NUdf::TStringRef::Of("baz"))
                .MakeOptional();

        memberItems[memberStructType->GetMemberIndex("nested_empty_optional_field")] =
            NYql::NUdf::TUnboxedValuePod();

        memberItems[memberStructType->GetMemberIndex("nested_doubly_optional_field")] =
            ValueBuilder->NewString(NYql::NUdf::TStringRef::Of("opbaz"))
                .MakeOptional()
                .MakeOptional();

        memberItems[memberStructType->GetMemberIndex("nested_empty_doubly_optional_field")] =
            NYql::NUdf::TUnboxedValuePod()
                .MakeOptional();

        items[memberIndex] = std::move(memberUnboxedValue);
    }

    {
        auto memberIndex = Type->GetMemberIndex("list_field");

        NYql::NUdf::TUnboxedValue memberItems[] = {
            ValueBuilder->NewString(NYql::NUdf::TStringRef::Of("foo")),
            ValueBuilder->NewString(NYql::NUdf::TStringRef::Of("bar")),
        };

        items[memberIndex] = ValueBuilder->NewList(memberItems, 2);
    }

    {
        auto memberIndex = Type->GetMemberIndex("optional_field");
        auto memberUnboxedValue = ValueBuilder->NewString(
            NYql::NUdf::TStringRef::Of("foobar"));

        items[memberIndex] = memberUnboxedValue.MakeOptional();
    }

    {
        auto memberIndex = Type->GetMemberIndex("empty_optional_field");
        items[memberIndex] = NYql::NUdf::TUnboxedValue();
    }

    {
        auto memberIndex = Type->GetMemberIndex("dict_field");

        auto* udfDictType = FunctionTypeInfoBuilder
            ->Dict()
                ->Key(FunctionTypeInfoBuilder->SimpleType<i64>())
                .Value(FunctionTypeInfoBuilder->SimpleType<char*>())
                .Build();

        auto dictValueBuilder = ValueBuilder->NewDict(
            udfDictType, NYql::NUdf::TDictFlags::EDictKind::Sorted);

        dictValueBuilder
            ->Add(
                NYql::NUdf::TUnboxedValuePod(static_cast<i64>(24)),
                ValueBuilder->NewString(NYql::NUdf::TStringRef::Of("foo")))
            .Add(
                NYql::NUdf::TUnboxedValuePod(static_cast<i64>(42)),
                ValueBuilder->NewString(NYql::NUdf::TStringRef::Of("bar")));

        items[memberIndex] = dictValueBuilder->Build();
    }

    setSimpleValue("void_field", NYql::NUdf::TUnboxedValuePod::Void());
    setSimpleValue("null_field", NYql::NUdf::TUnboxedValuePod::Zero());

    setStringValue("tagged_field", "foo");

    {
        auto memberIndex = Type->GetMemberIndex("variant_tuple_field");
        items[memberIndex] = ValueBuilder->NewVariant(
            1, NYql::NUdf::TUnboxedValuePod(static_cast<i32>(42)));
    }

    {
        auto memberIndex = Type->GetMemberIndex("variant_struct_field");
        auto* memberStructType = static_cast<const NKikimr::NMiniKQL::TVariantType*>(
            Type->GetMemberType(memberIndex));

        auto* underlyingStructType = static_cast<const NKikimr::NMiniKQL::TStructType*>(
            memberStructType->GetUnderlyingType());

        items[memberIndex] = ValueBuilder->NewVariant(
            underlyingStructType->GetMemberIndex("variant_string_field"),
            ValueBuilder->NewString(NYql::NUdf::TStringRef::Of("foobar")));
    }

    return unboxedValue;
}

void TUnboxedValueSetupFull::AssertExpectedUnboxedValue(
    const NYql::NUdf::TUnboxedValue& unboxedValue
) const {
    UNIT_ASSERT(unboxedValue.IsBoxed());
    UNIT_ASSERT_VALUES_EQUAL(unboxedValue.GetListLength(), Type->GetMembersCount());

    auto getNestedMember = [&](
        const auto& unboxedValue, auto* structType, TStringBuf name
    ) {
        return GetMember(unboxedValue, structType, name);
    };

    auto getMember = [&](TStringBuf name) {
        return GetMember(unboxedValue, Type, name);
    };

    auto assertStringUnboxedValue = [&](
        const NYql::NUdf::TUnboxedValue& unboxedValue, TStringBuf value
    ) {
        return AssertStringUnboxedValue(unboxedValue, value);
    };

    auto assertStringValue = [&](TStringBuf name, TStringBuf value) {
        return AssertStringValue(unboxedValue, name, value);
    };

    auto assertSimpleUnboxedValue = [&](
        const NYql::NUdf::TUnboxedValue& unboxedValue, auto&& value
    ) {
        return AssertSimpleUnboxedValue(
            unboxedValue,
            std::forward<decltype(value)>(value));
    };

    auto assertSimpleValue = [&](TStringBuf name, auto&& value) {
        return AssertSimpleValue(
            unboxedValue,
            name,
            std::forward<decltype(value)>(value));
    };

    assertStringValue("string_field", "foobar");

    {
        auto stringUnboxedValue = getMember("uuid_field");
        auto value = TString(TStringBuf(stringUnboxedValue.AsStringRef()));

        UNIT_ASSERT_VALUES_EQUAL(
            NKikimr::NUuid::UuidBytesToString(value),
            "10234567-89ab-cdef-1023-456789abcdef");
    }

    assertStringValue("json_field", "{}");
    assertStringValue("utf8_field", "поиск");

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

    assertSimpleValue("date_field", static_cast<ui16>(9));
    assertSimpleValue("datetime_field", static_cast<ui32>(10));
    assertSimpleValue("timestamp_field", static_cast<ui64>(11));
    assertSimpleValue("interval_field", static_cast<i64>(12));

    assertSimpleValue("date32_field", static_cast<i32>(13));
    assertSimpleValue("datetime64_field", static_cast<i64>(14));
    assertSimpleValue("timestamp64_field", static_cast<i64>(15));
    assertSimpleValue("interval64_field", static_cast<i64>(16));
    assertSimpleValue("decimal_field", static_cast<NYql::NDecimal::TInt128>(12345));

    {
        auto memberUnboxedValue = getMember("tuple_field");
        UNIT_ASSERT_VALUES_EQUAL(memberUnboxedValue.GetListLength(), 2);

        assertStringUnboxedValue(memberUnboxedValue.GetElement(0), "foobar");
        assertSimpleUnboxedValue(memberUnboxedValue.GetElement(1), static_cast<i32>(42));
    }

    {
        auto memberIndex = Type->GetMemberIndex("struct_field");
        auto* memberStructType = static_cast<const NKikimr::NMiniKQL::TStructType*>(
            Type->GetMemberType(memberIndex));

        auto memberUnboxedValue = getMember("struct_field");
        UNIT_ASSERT_VALUES_EQUAL(
            memberUnboxedValue.GetListLength(),
            memberStructType->GetMembersCount());

        assertStringUnboxedValue(
            getNestedMember(memberUnboxedValue, memberStructType, "nested_string_field"),
            "foo");

        assertSimpleUnboxedValue(
            getNestedMember(memberUnboxedValue, memberStructType, "nested_int32_field"),
            static_cast<i32>(42));

        assertStringUnboxedValue(
            getNestedMember(memberUnboxedValue, memberStructType, "nested_yson_field"),
            "[24]");

        {
            auto nestedMemberUnboxedValue = getNestedMember(
                memberUnboxedValue,
                memberStructType,
                "nested_optional_field");

            assertStringUnboxedValue(nestedMemberUnboxedValue.GetOptionalValue(), "baz");
        }

        {
            auto nestedMemberUnboxedValue = getNestedMember(
                memberUnboxedValue,
                memberStructType,
                "nested_empty_optional_field");

            UNIT_ASSERT(!nestedMemberUnboxedValue);
        }

        {
            auto nestedMemberUnboxedValue = getNestedMember(
                memberUnboxedValue,
                memberStructType,
                "nested_doubly_optional_field");

            assertStringUnboxedValue(
                nestedMemberUnboxedValue.GetOptionalValue().GetOptionalValue(),
                "opbaz");
        }

        {
            auto nestedMemberUnboxedValue = getNestedMember(
                memberUnboxedValue,
                memberStructType,
                "nested_empty_doubly_optional_field");

            UNIT_ASSERT(!nestedMemberUnboxedValue.GetOptionalValue());
        }
    }

    {
        auto memberUnboxedValue = getMember("list_field");
        UNIT_ASSERT_VALUES_EQUAL(memberUnboxedValue.GetListLength(), 2);

        auto listIterator = memberUnboxedValue.GetListIterator();

        {
            NYql::NUdf::TUnboxedValue item;
            listIterator.Next(item);
            assertStringUnboxedValue(item, "foo");
        }

        {
            NYql::NUdf::TUnboxedValue item;
            listIterator.Next(item);

            assertStringUnboxedValue(item, "bar");
        }
    }

    {
        auto memberUnboxedValue = getMember("optional_field");
        assertStringUnboxedValue(memberUnboxedValue.GetOptionalValue(), "foobar");
    }

    {
        auto memberUnboxedValue = getMember("dict_field");
        UNIT_ASSERT_VALUES_EQUAL(memberUnboxedValue.GetDictLength(), 2);

        auto dictIterator = memberUnboxedValue.GetDictIterator();

        THashMap<i64, TString> dict;
        NYql::NUdf::TUnboxedValue key, payload;

        while (dictIterator.NextPair(key, payload)) {
            dict[key.Get<i64>()] = TString(TStringBuf(payload.AsStringRef()));
        }

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
    }

    {
        auto memberUnboxedValue = getMember("void_field");
        UNIT_ASSERT(memberUnboxedValue.IsEmbedded());
    }

    {
        auto memberUnboxedValue = getMember("null_field");
        UNIT_ASSERT(memberUnboxedValue.IsEmbedded());
    }

    {
        auto memberUnboxedValue = getMember("tagged_field");
        assertStringUnboxedValue(memberUnboxedValue, "foo");
    }

    {
        auto memberUnboxedValue = getMember("variant_tuple_field");
        UNIT_ASSERT_VALUES_EQUAL(memberUnboxedValue.GetVariantIndex(), 1);

        auto variantItem = memberUnboxedValue.GetVariantItem();
        assertSimpleUnboxedValue(variantItem, static_cast<i32>(42));
    }

    {
        auto memberIndex = Type->GetMemberIndex("variant_struct_field");
        auto* memberStructType = static_cast<const NKikimr::NMiniKQL::TVariantType*>(
            Type->GetMemberType(memberIndex));

        auto* underlyingStructType = static_cast<const NKikimr::NMiniKQL::TStructType*>(
            memberStructType->GetUnderlyingType());

        auto memberUnboxedValue = getMember("variant_struct_field");
        UNIT_ASSERT_VALUES_EQUAL(
            memberUnboxedValue.GetVariantIndex(),
            underlyingStructType->GetMemberIndex("variant_string_field"));

        auto variantItem = memberUnboxedValue.GetVariantItem();
        assertStringUnboxedValue(variantItem, "foobar");
    }
}

TUnboxedValueSetupLarge::TUnboxedValueSetupLarge()
{
    Type = static_cast<const NKikimr::NMiniKQL::TStructType*>(BuildType());
}

const NKikimr::NMiniKQL::TType* TUnboxedValueSetupLarge::BuildType() {
    auto optionalDataType = [&](auto slot) {
        return TypeBuilder.NewOptionalType(TypeBuilder.NewDataType(slot));
    };

    return TypeBuilder.NewStructType({
        {"string_field", optionalDataType(NYql::NUdf::EDataSlot::String)},
        {"int64_field", optionalDataType(NYql::NUdf::EDataSlot::Int64)},
        {"int32_field", optionalDataType(NYql::NUdf::EDataSlot::Int32)},
        {"int16_field", optionalDataType(NYql::NUdf::EDataSlot::Int16)},
        {"struct_field", TypeBuilder.NewStructType({
            {"nested_string_field", optionalDataType(NYql::NUdf::EDataSlot::String)},
            {"nested_int32_field", optionalDataType(NYql::NUdf::EDataSlot::Int32)},
        })}
    });
}

NYql::NUdf::TUnboxedValue TUnboxedValueSetupLarge::BuildUnboxedValue() {
    NYql::NUdf::TUnboxedValue* items;
    auto unboxedValue = ValueBuilder->NewArray(Type->GetMembersCount(), items);

    auto setSimpleValue = [&](TStringBuf name, auto&& value) {
        return SetSimpleValue(items, name, std::forward<decltype(value)>(value));
    };

    auto setStringValue = [&](TStringBuf name, auto&& value) {
        return SetStringValue(items, name, std::forward<decltype(value)>(value));
    };

    setStringValue("string_field", "foobar");
    setSimpleValue("int64_field", static_cast<i64>(1));
    setSimpleValue("int32_field", static_cast<i32>(2));
    setSimpleValue("int16_field", static_cast<i16>(3));

    {
        auto memberIndex = Type->GetMemberIndex("struct_field");
        auto* memberStructType = static_cast<const NKikimr::NMiniKQL::TStructType*>(
            Type->GetMemberType(memberIndex));

        NYql::NUdf::TUnboxedValue* memberItems;
        auto memberUnboxedValue = ValueBuilder->NewArray(
            memberStructType->GetMembersCount(), memberItems);

        memberItems[memberStructType->GetMemberIndex("nested_string_field")] =
            ValueBuilder->NewString(NYql::NUdf::TStringRef::Of("foo"))
                .MakeOptional();

        memberItems[memberStructType->GetMemberIndex("nested_int32_field")] =
            NYql::NUdf::TUnboxedValuePod(static_cast<i32>(42))
                .MakeOptional();

        items[memberIndex] = std::move(memberUnboxedValue);
    }

    return unboxedValue;
}

void TUnboxedValueSetupLarge::AssertExpectedUnboxedValue(
    const NYql::NUdf::TUnboxedValue& unboxedValue
) const {
    UNIT_ASSERT(unboxedValue.IsBoxed());
    UNIT_ASSERT_VALUES_EQUAL(unboxedValue.GetListLength(), Type->GetMembersCount());

    auto getNestedMember = [&](
        const auto& unboxedValue, auto* structType, TStringBuf name
    ) {
        return GetMember(unboxedValue, structType, name);
    };

    auto getMember = [&](TStringBuf name) {
        return GetMember(unboxedValue, Type, name);
    };

    auto assertStringUnboxedValue = [&](
        const NYql::NUdf::TUnboxedValue& unboxedValue, TStringBuf value
    ) {
        return AssertStringUnboxedValue(unboxedValue, value);
    };

    auto assertStringValue = [&](TStringBuf name, TStringBuf value) {
        return AssertStringValue(unboxedValue, name, value);
    };

    auto assertSimpleUnboxedValue = [&](
        const NYql::NUdf::TUnboxedValue& unboxedValue, auto&& value
    ) {
        return AssertSimpleUnboxedValue(
            unboxedValue,
            std::forward<decltype(value)>(value));
    };

    auto assertSimpleValue = [&](TStringBuf name, auto&& value) {
        return AssertSimpleValue(
            unboxedValue,
            name,
            std::forward<decltype(value)>(value));
    };

    assertStringValue("string_field", "foobar");
    assertSimpleValue("int64_field", static_cast<i64>(1));
    assertSimpleValue("int32_field", static_cast<i32>(2));
    assertSimpleValue("int16_field", static_cast<i16>(3));

    {
        auto memberIndex = Type->GetMemberIndex("struct_field");
        auto* memberStructType = static_cast<const NKikimr::NMiniKQL::TStructType*>(
            Type->GetMemberType(memberIndex));

        auto memberUnboxedValue = getMember("struct_field");
        UNIT_ASSERT_VALUES_EQUAL(memberUnboxedValue.GetListLength(), 2);

        {
            auto nestedMemberUnboxedValue = getNestedMember(
                memberUnboxedValue,
                memberStructType,
                "nested_string_field");

            assertStringUnboxedValue(nestedMemberUnboxedValue.GetOptionalValue(), "foo");
        }

        {
            auto nestedMemberUnboxedValue = getNestedMember(
                memberUnboxedValue,
                memberStructType,
                "nested_int32_field");

            assertSimpleUnboxedValue(
                nestedMemberUnboxedValue.GetOptionalValue(),
                static_cast<i32>(42));
        }
    }
}

TUnboxedValueSetupLargeOptional::TUnboxedValueSetupLargeOptional()
{
    Type = static_cast<const NKikimr::NMiniKQL::TStructType*>(BuildType());
}

const NKikimr::NMiniKQL::TType* TUnboxedValueSetupLargeOptional::BuildType() {
    auto optionalDataType = [&](auto slot) {
        return TypeBuilder.NewOptionalType(TypeBuilder.NewDataType(slot));
    };

    return TypeBuilder.NewStructType({
        {"string_field", optionalDataType(NYql::NUdf::EDataSlot::String)},
        {"int64_field", optionalDataType(NYql::NUdf::EDataSlot::Int64)},
        {"int32_field", optionalDataType(NYql::NUdf::EDataSlot::Int32)},
        {"int16_field", optionalDataType(NYql::NUdf::EDataSlot::Int16)},
        {"struct_field", TypeBuilder.NewStructType({
            {"nested_string_field", optionalDataType(NYql::NUdf::EDataSlot::String)},
            {"nested_int32_field", optionalDataType(NYql::NUdf::EDataSlot::Int32)},
        })}
    });
}

NYql::NUdf::TUnboxedValue TUnboxedValueSetupLargeOptional::BuildUnboxedValue() {
    NYql::NUdf::TUnboxedValue* items;
    auto unboxedValue = ValueBuilder->NewArray(Type->GetMembersCount(), items);

    auto setSimpleValue = [&](TStringBuf name, auto&& value) {
        return SetSimpleValue(items, name, std::forward<decltype(value)>(value));
    };

    auto setStringValue = [&](TStringBuf name, auto&& value) {
        return SetStringValue(items, name, std::forward<decltype(value)>(value));
    };

    auto setEntity = [&](TStringBuf name) {
        setSimpleValue(name, NYql::NUdf::TUnboxedValuePod{});
    };

    setStringValue("string_field", "foobar");
    setSimpleValue("int64_field", static_cast<i64>(1));
    setEntity("int32_field");
    setEntity("int16_field");

    {
        auto memberIndex = Type->GetMemberIndex("struct_field");
        auto* memberStructType = static_cast<const NKikimr::NMiniKQL::TStructType*>(
            Type->GetMemberType(memberIndex));

        NYql::NUdf::TUnboxedValue* memberItems;
        auto memberUnboxedValue = ValueBuilder->NewArray(
            memberStructType->GetMembersCount(), memberItems);

        memberItems[memberStructType->GetMemberIndex("nested_string_field")] =
            ValueBuilder->NewString(NYql::NUdf::TStringRef::Of("foo"))
                .MakeOptional();

        memberItems[memberStructType->GetMemberIndex("nested_int32_field")] =
            NYql::NUdf::TUnboxedValuePod{};

        items[memberIndex] = std::move(memberUnboxedValue);
    }

    return unboxedValue;
}

void TUnboxedValueSetupLargeOptional::AssertExpectedUnboxedValue(
    const NYql::NUdf::TUnboxedValue& unboxedValue
) const {
    UNIT_ASSERT(unboxedValue.IsBoxed());
    UNIT_ASSERT_VALUES_EQUAL(unboxedValue.GetListLength(), Type->GetMembersCount());

    auto getNestedMember = [&](
        const auto& unboxedValue, auto* structType, TStringBuf name
    ) {
        return GetMember(unboxedValue, structType, name);
    };

    auto getMember = [&](TStringBuf name) {
        return GetMember(unboxedValue, Type, name);
    };

    auto assertStringUnboxedValue = [&](
        const NYql::NUdf::TUnboxedValue& unboxedValue, TStringBuf value
    ) {
        return AssertStringUnboxedValue(unboxedValue, value);
    };

    auto assertStringValue = [&](TStringBuf name, TStringBuf value) {
        return AssertStringValue(unboxedValue, name, value);
    };

    auto assertSimpleValue = [&](TStringBuf name, auto&& value) {
        return AssertSimpleValue(
            unboxedValue,
            name,
            std::forward<decltype(value)>(value));
    };

    auto assertEntity = [&](TStringBuf name) {
        auto memberUnboxedValue = getMember(name);
        UNIT_ASSERT(!memberUnboxedValue);
    };

    assertStringValue("string_field", "foobar");
    assertSimpleValue("int64_field", static_cast<i64>(1));
    assertEntity("int32_field");
    assertEntity("int16_field");

    {
        auto memberIndex = Type->GetMemberIndex("struct_field");
        auto* memberStructType = static_cast<const NKikimr::NMiniKQL::TStructType*>(
            Type->GetMemberType(memberIndex));

        auto memberUnboxedValue = getMember("struct_field");
        UNIT_ASSERT_VALUES_EQUAL(memberUnboxedValue.GetListLength(), 2);

        {
            auto nestedMemberUnboxedValue = getNestedMember(
                memberUnboxedValue,
                memberStructType,
                "nested_string_field");

            assertStringUnboxedValue(nestedMemberUnboxedValue.GetOptionalValue(), "foo");
        }

        {
            auto nestedMemberUnboxedValue = getNestedMember(
                memberUnboxedValue,
                memberStructType,
                "nested_int32_field");

            UNIT_ASSERT(!nestedMemberUnboxedValue);
        }
    }
}

TUnboxedValueSetupSmall::TUnboxedValueSetupSmall()
{
    Type = static_cast<const NKikimr::NMiniKQL::TStructType*>(BuildType());
}

const NKikimr::NMiniKQL::TType* TUnboxedValueSetupSmall::BuildType() {
    auto optionalDataType = [&](auto slot) {
        return TypeBuilder.NewOptionalType(TypeBuilder.NewDataType(slot));
    };

    return TypeBuilder.NewStructType({
        {"string_field", optionalDataType(NYql::NUdf::EDataSlot::String)},
        {"int64_field", optionalDataType(NYql::NUdf::EDataSlot::Int64)},
        {"struct_field", TypeBuilder.NewStructType({
            {"nested_string_field", optionalDataType(NYql::NUdf::EDataSlot::String)},
        })}
    });
}

NYql::NUdf::TUnboxedValue TUnboxedValueSetupSmall::BuildUnboxedValue() {
    NYql::NUdf::TUnboxedValue* items;
    auto unboxedValue = ValueBuilder->NewArray(Type->GetMembersCount(), items);

    auto setSimpleValue = [&](TStringBuf name, auto&& value) {
        return SetSimpleValue(items, name, std::forward<decltype(value)>(value));
    };

    auto setStringValue = [&](TStringBuf name, auto&& value) {
        return SetStringValue(items, name, std::forward<decltype(value)>(value));
    };

    setStringValue("string_field", "foobar");
    setSimpleValue("int64_field", static_cast<i64>(1));

    {
        auto memberIndex = Type->GetMemberIndex("struct_field");
        auto* memberStructType = static_cast<const NKikimr::NMiniKQL::TStructType*>(
            Type->GetMemberType(memberIndex));

        NYql::NUdf::TUnboxedValue* memberItems;
        auto memberUnboxedValue = ValueBuilder->NewArray(
            memberStructType->GetMembersCount(), memberItems);

        memberItems[memberStructType->GetMemberIndex("nested_string_field")] =
            ValueBuilder->NewString(NYql::NUdf::TStringRef::Of("foo"))
                .MakeOptional();

        items[memberIndex] = std::move(memberUnboxedValue);
    }

    return unboxedValue;
}

void TUnboxedValueSetupSmall::AssertExpectedUnboxedValue(
    const NYql::NUdf::TUnboxedValue& unboxedValue
) const {
    UNIT_ASSERT(unboxedValue.IsBoxed());
    UNIT_ASSERT_VALUES_EQUAL(unboxedValue.GetListLength(), Type->GetMembersCount());

    auto getNestedMember = [&](
        const auto& unboxedValue, auto* structType, TStringBuf name
    ) {
        return GetMember(unboxedValue, structType, name);
    };

    auto getMember = [&](TStringBuf name) {
        return GetMember(unboxedValue, Type, name);
    };

    auto assertStringUnboxedValue = [&](
        const NYql::NUdf::TUnboxedValue& unboxedValue, TStringBuf value
    ) {
        return AssertStringUnboxedValue(unboxedValue, value);
    };

    auto assertStringValue = [&](TStringBuf name, TStringBuf value) {
        return AssertStringValue(unboxedValue, name, value);
    };

    auto assertSimpleValue = [&](TStringBuf name, auto&& value) {
        return AssertSimpleValue(
            unboxedValue,
            name,
            std::forward<decltype(value)>(value));
    };

    assertStringValue("string_field", "foobar");
    assertSimpleValue("int64_field", static_cast<i64>(1));

    {
        auto memberIndex = Type->GetMemberIndex("struct_field");
        auto* memberStructType = static_cast<const NKikimr::NMiniKQL::TStructType*>(
            Type->GetMemberType(memberIndex));

        auto memberUnboxedValue = getMember("struct_field");
        UNIT_ASSERT_VALUES_EQUAL(memberUnboxedValue.GetListLength(), 1);

        {
            auto nestedMemberUnboxedValue = getNestedMember(
                memberUnboxedValue,
                memberStructType,
                "nested_string_field");

            assertStringUnboxedValue(nestedMemberUnboxedValue.GetOptionalValue(), "foo");
        }
    }
}

} // namespace NYql::NYtflow::NCodec::NTest

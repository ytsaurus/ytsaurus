#include "yql_ytflow_input_codec.h"
#include "yql_ytflow_struct_precomputes.h"
#include "yql_ytflow_type_helpers.h"
#include "yql_ytflow_value_skipper.h"

#include <library/cpp/yt/memory/new.h>
#include <library/cpp/yt/misc/guid.h>

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/public/udf/udf_string.h>
#include <yql/essentials/public/udf/udf_string_ref.h>
#include <yql/essentials/types/uuid/uuid.h>
#include <yql/essentials/utils/yql_panic.h>

#include <yt/yt/client/complex_types/uuid_text.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/library/decimal/decimal.h>

#include <util/generic/hash.h>
#include <util/stream/mem.h>

#include <vector>


namespace NYql::NYtflow::NCodec::NPrivate {

class TInputCodecBase {
public:
    TInputCodecBase(
        const NKikimr::NMiniKQL::TType* type,
        NYT::NTableClient::TLogicalTypePtr ytType,
        NYql::NUdf::IValueBuilder& valueBuilder,
        NYql::NUdf::IFunctionTypeInfoBuilder& functionTypeInfoBuilder,
        TConvertOptions convertOptions
    )
        : ValueBuilder(valueBuilder)
        , ConvertOptions(convertOptions.WithConvertDirection(EConvertDirection::YtToYql))
        , ValueSkipper(CreateValueSkipper())
    {
        Type = type;
        YtType = std::move(ytType);

        ValidateTypesCorrespondence(Type, YtType.Get(), ConvertOptions);

        StructPrecomputes = BuildStructPrecomputes(Type, YtType.Get(), ConvertOptions);

        DictUdfTypes = BuildDictUdfTypes(Type, functionTypeInfoBuilder);
    }

    virtual ~TInputCodecBase() = default;

    static NYT::NTableClient::TLogicalTypePtr BuildStructYtType(
        const NYT::NTableClient::TTableSchemaPtr& ytSchema
    ) {
        std::vector<NYT::NTableClient::TStructField> fields;
        fields.reserve(ytSchema->Columns().size());

        for (const auto& columnSchema: ytSchema->Columns()) {
            fields.push_back(NYT::NTableClient::TStructField{
                .Name = columnSchema.Name(),
                .StableName = columnSchema.Name(),
                .Type = columnSchema.LogicalType(),
            });
        }

        return NYT::New<NYT::NTableClient::TStructLogicalType>(
            std::move(fields),
            /*removedFieldStableNames*/ std::vector<std::string>{});
    }

protected:
    NYql::NUdf::TUnboxedValue ConvertRow(
        NYT::NTableClient::TUnversionedRow unversionedRow
    ) {
        YQL_ENSURE(
            Type->IsStruct(),
            "Method is supported only for struct input types");

        auto* structType = static_cast<const NKikimr::NMiniKQL::TStructType*>(Type);

        TVector<NYql::NUdf::TUnboxedValue> items;
        items.resize(structType->GetMembersCount());

        for (
            ui32 memberIndex = 0;
            memberIndex < structType->GetMembersCount();
            ++memberIndex
        ) {
            auto memberDescriptorKey = std::pair(structType, memberIndex);
            auto memberDescriptor = StructPrecomputes.MemberDescriptors[memberDescriptorKey];
            auto ytMemberIndex = memberDescriptor.YtIndex;

            if (!ytMemberIndex) {
                items[memberIndex] = NYql::NUdf::TUnboxedValue();
                continue;
            }

            items[memberIndex] = Convert(
                unversionedRow[*ytMemberIndex],
                memberDescriptor.Type,
                memberDescriptor.YtType);
        }

        return ValueBuilder.NewList(items.data(), items.size());
    }

    NYql::NUdf::TUnboxedValue ConvertValue(
        NYT::NTableClient::TUnversionedValue unversionedValue
    ) {
        return Convert(unversionedValue, Type, YtType.Get());
    }

    NYql::NUdf::TUnboxedValue ConvertValueRange(
        NYT::NTableClient::TUnversionedValueRange unversionedValues
    ) {
        YQL_ENSURE(
            Type->IsTuple(),
            "Method is only supported for tuple input types");

        auto* tupleType = static_cast<const NKikimr::NMiniKQL::TTupleType*>(Type);
        auto* ytTupleType = static_cast<
            const NYT::NTableClient::TTupleLogicalType*>(YtType.Get());

        const auto& ytElementTypes = ytTupleType->GetElements();

        YQL_ENSURE(
            unversionedValues.Size() == tupleType->GetElementsCount(),
            "Unexpected value count: "
                << unversionedValues.Size() << " (got) != "
                << tupleType->GetElementsCount() << " (expected)");

        TVector<NYql::NUdf::TUnboxedValue> items;
        items.reserve(tupleType->GetElementsCount());

        for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
            items.push_back(Convert(
                unversionedValues[index],
                tupleType->GetElementType(index),
                ytElementTypes[index].Get()));
        }

        return ValueBuilder.NewList(items.data(), items.size());
    }

private:
    NYql::NUdf::TUnboxedValue Convert(
        NYT::NTableClient::TUnversionedValue unversionedValue,
        const NKikimr::NMiniKQL::TType* type,
        const NYT::NTableClient::TLogicalType* ytType
    ) {
        if (type->IsData()) {
            auto* dataType = static_cast<const NKikimr::NMiniKQL::TDataType*>(type);

            if (*dataType->GetDataSlot() == NYql::NUdf::EDataSlot::Decimal) {
                auto* decimalType = static_cast<
                    const NKikimr::NMiniKQL::TDataDecimalType*>(dataType);

                auto* ytDecimalType = static_cast<
                    const NYT::NTableClient::TDecimalLogicalType*>(ytType);

                return Convert(unversionedValue, decimalType, ytDecimalType);
            } else if (*dataType->GetDataSlot() == NYql::NUdf::EDataSlot::Yson) {
                // YQL & YT type systems disagree here:
                // it's possible to have non-optional Yson in YQL, but Any in YT
                // can only be optional
                auto* ytOptionalType = static_cast<
                    const NYT::NTableClient::TOptionalLogicalType*>(ytType);

                auto* ytUnderlyingType = ytOptionalType->GetElement().Get();
                auto* ytDataType = static_cast<
                    const NYT::NTableClient::TSimpleLogicalType*>(ytUnderlyingType);

                return Convert(unversionedValue, dataType, ytDataType);
            } else {
                auto* ytDataType = static_cast<
                    const NYT::NTableClient::TSimpleLogicalType*>(ytType);

                return Convert(unversionedValue, dataType, ytDataType);
            }
        } else if (type->IsVoid()) {
            auto* voidType = static_cast<const NKikimr::NMiniKQL::TVoidType*>(type);
            auto* ytVoidType = static_cast<
                const NYT::NTableClient::TSimpleLogicalType*>(ytType);

            return Convert(unversionedValue, voidType, ytVoidType);
        } else if (type->IsNull()) {
            auto* nullType = static_cast<const NKikimr::NMiniKQL::TNullType*>(type);
            auto* ytNullType = static_cast<
                const NYT::NTableClient::TSimpleLogicalType*>(ytType);

            return Convert(unversionedValue, nullType, ytNullType);
        } else if (type->IsOptional()) {
            auto* ytOptionalType = static_cast<
                const NYT::NTableClient::TOptionalLogicalType*>(ytType);

            if (!ytOptionalType->IsElementNullable()) {
                auto* optionalType = static_cast<
                    const NKikimr::NMiniKQL::TOptionalType*>(type);

                return Convert(unversionedValue, optionalType, ytOptionalType);
            }
        } else if (type->IsTagged()) {
            auto* taggedType = static_cast<const NKikimr::NMiniKQL::TTaggedType*>(type);
            auto* ytTaggedType = static_cast<
                const NYT::NTableClient::TTaggedLogicalType*>(ytType);

            return Convert(unversionedValue, taggedType, ytTaggedType);
        }

        YQL_ENSURE(unversionedValue.Type == NYT::NTableClient::EValueType::Composite);

        auto stream = TMemoryInput(unversionedValue.AsStringBuf());
        auto ysonParser = NYT::NYson::TYsonPullParser(
            &stream, NYT::NYson::EYsonType::ListFragment);

        auto unboxedValue = Convert(ysonParser, type, ytType);

        YQL_ENSURE(ysonParser.Next().IsEndOfStream());

        return unboxedValue;
    }

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NTableClient::TUnversionedValue unversionedValue,
        const NKikimr::NMiniKQL::TDataType* type,
        const NYT::NTableClient::TSimpleLogicalType* /*ytType*/
    ) {
        NYql::NUdf::TUnboxedValue unboxedValue;

        switch (*type->GetDataSlot()) {
        case NYql::NUdf::EDataSlot::String:
        case NYql::NUdf::EDataSlot::Json:
        case NYql::NUdf::EDataSlot::Utf8:
            YQL_ENSURE(unversionedValue.Type == NYT::NTableClient::EValueType::String);
            unboxedValue = ValueBuilder.NewString(
                NYql::NUdf::TStringRef(unversionedValue.AsStringBuf()));

            break;

        case NYql::NUdf::EDataSlot::Uuid:
            unboxedValue = ConvertUuid(unversionedValue.AsStringBuf());
            break;

        case NYql::NUdf::EDataSlot::Int8:
            YQL_ENSURE(unversionedValue.Type == NYT::NTableClient::EValueType::Int64);
            unboxedValue = NYql::NUdf::TUnboxedValuePod(
                static_cast<i8>(unversionedValue.Data.Int64));

            break;

        case NYql::NUdf::EDataSlot::Int16:
            YQL_ENSURE(unversionedValue.Type == NYT::NTableClient::EValueType::Int64);
            unboxedValue = NYql::NUdf::TUnboxedValuePod(
                static_cast<i16>(unversionedValue.Data.Int64));

            break;

        case NYql::NUdf::EDataSlot::Int32:
            YQL_ENSURE(unversionedValue.Type == NYT::NTableClient::EValueType::Int64);
            unboxedValue = NYql::NUdf::TUnboxedValuePod(
                static_cast<i32>(unversionedValue.Data.Int64));

            break;

        case NYql::NUdf::EDataSlot::Int64:
        case NYql::NUdf::EDataSlot::Interval:
        case NYql::NUdf::EDataSlot::Date32:
        case NYql::NUdf::EDataSlot::Datetime64:
        case NYql::NUdf::EDataSlot::Timestamp64:
        case NYql::NUdf::EDataSlot::Interval64:
            YQL_ENSURE(unversionedValue.Type == NYT::NTableClient::EValueType::Int64);
            unboxedValue = NYql::NUdf::TUnboxedValuePod(unversionedValue.Data.Int64);
            break;

        case NYql::NUdf::EDataSlot::Uint8:
            YQL_ENSURE(unversionedValue.Type == NYT::NTableClient::EValueType::Uint64);
            unboxedValue = NYql::NUdf::TUnboxedValuePod(
                static_cast<ui8>(unversionedValue.Data.Uint64));

            break;

        case NYql::NUdf::EDataSlot::Uint16:
            YQL_ENSURE(unversionedValue.Type == NYT::NTableClient::EValueType::Uint64);
            unboxedValue = NYql::NUdf::TUnboxedValuePod(
                static_cast<ui16>(unversionedValue.Data.Uint64));

            break;

        case NYql::NUdf::EDataSlot::Uint32:
            YQL_ENSURE(unversionedValue.Type == NYT::NTableClient::EValueType::Uint64);
            unboxedValue = NYql::NUdf::TUnboxedValuePod(
                static_cast<ui32>(unversionedValue.Data.Uint64));

            break;

        case NYql::NUdf::EDataSlot::Uint64:
        case NYql::NUdf::EDataSlot::Date:
        case NYql::NUdf::EDataSlot::Datetime:
        case NYql::NUdf::EDataSlot::Timestamp:
            YQL_ENSURE(unversionedValue.Type == NYT::NTableClient::EValueType::Uint64);
            unboxedValue = NYql::NUdf::TUnboxedValuePod(unversionedValue.Data.Uint64);
            break;

        case NYql::NUdf::EDataSlot::Float:
            YQL_ENSURE(unversionedValue.Type == NYT::NTableClient::EValueType::Double);
            unboxedValue = NYql::NUdf::TUnboxedValuePod(
                static_cast<float>(unversionedValue.Data.Double));

            break;

        case NYql::NUdf::EDataSlot::Double:
            YQL_ENSURE(unversionedValue.Type == NYT::NTableClient::EValueType::Double);
            unboxedValue = NYql::NUdf::TUnboxedValuePod(unversionedValue.Data.Double);
            break;

        case NYql::NUdf::EDataSlot::Bool:
            YQL_ENSURE(unversionedValue.Type == NYT::NTableClient::EValueType::Boolean);
            unboxedValue = NYql::NUdf::TUnboxedValuePod(unversionedValue.Data.Boolean);
            break;

        case NYql::NUdf::EDataSlot::Yson:
            YQL_ENSURE(unversionedValue.Type == NYT::NTableClient::EValueType::Any);
            unboxedValue = ValueBuilder.NewString(
                NYql::NUdf::TStringRef(unversionedValue.AsStringBuf()));

            break;

        default:
            YQL_ENSURE(false, "Unsupported type: " << *type->GetDataSlot());
        }

        return unboxedValue;
    }

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NTableClient::TUnversionedValue unversionedValue,
        const NKikimr::NMiniKQL::TDataDecimalType* type,
        const NYT::NTableClient::TDecimalLogicalType* /*ytType*/
    ) {
        YQL_ENSURE(unversionedValue.Type == NYT::NTableClient::EValueType::String);
        auto [precision, scale] = type->GetParams();
        return ConvertDecimal(unversionedValue.AsStringBuf(), precision);
    }

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NTableClient::TUnversionedValue unversionedValue,
        const NKikimr::NMiniKQL::TOptionalType* type,
        const NYT::NTableClient::TOptionalLogicalType* ytType
    ) {
        // immediate top-level version only, composite & nested are handled via ysonParser
        if (unversionedValue.Type == NYT::NTableClient::EValueType::Null) {
            return NYql::NUdf::TUnboxedValue();
        }

        auto* itemType = type->GetItemType();
        auto* ytItemType = ytType->GetElement().Get();

        auto item = Convert(unversionedValue, itemType, ytItemType);
        return item.MakeOptional();
    }

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NTableClient::TUnversionedValue unversionedValue,
        const NKikimr::NMiniKQL::TVoidType* /*type*/,
        const NYT::NTableClient::TSimpleLogicalType* /*ytType*/
    ) {
        YQL_ENSURE(unversionedValue.Type == NYT::NTableClient::EValueType::Null);
        return NYql::NUdf::TUnboxedValuePod::Void();
    }

    NYql::NUdf::TUnboxedValue Convert(
       NYT::NTableClient::TUnversionedValue unversionedValue,
        const NKikimr::NMiniKQL::TNullType* /*type*/,
        const NYT::NTableClient::TSimpleLogicalType* /*ytType*/
    ) {
        YQL_ENSURE(unversionedValue.Type == NYT::NTableClient::EValueType::Null);
        return NYql::NUdf::TUnboxedValuePod::Zero();
    }

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NTableClient::TUnversionedValue unversionedValue,
        const NKikimr::NMiniKQL::TTaggedType* type,
        const NYT::NTableClient::TTaggedLogicalType* ytType
    ) {
        auto* baseType = type->GetBaseType();
        auto* ytBaseType = ytType->GetElement().Get();

        return Convert(unversionedValue, baseType, ytBaseType);
    }

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NKikimr::NMiniKQL::TType* type,
        const NYT::NTableClient::TLogicalType* ytType
    ) {
        NYql::NUdf::TUnboxedValue unboxedValue;

        if (type->IsData()) {
            auto* dataType = static_cast<const NKikimr::NMiniKQL::TDataType*>(type);

            if (*dataType->GetDataSlot() == NYql::NUdf::EDataSlot::Decimal) {
                auto* decimalType = static_cast<
                    const NKikimr::NMiniKQL::TDataDecimalType*>(dataType);

                auto* ytDecimalType = static_cast<
                    const NYT::NTableClient::TDecimalLogicalType*>(ytType);

                unboxedValue = Convert(ysonParser, decimalType, ytDecimalType);
            } else if (*dataType->GetDataSlot() == NYql::NUdf::EDataSlot::Yson) {
                // YQL & YT type systems disagree here:
                // it's possible to have non-optional Yson in YQL, but Any in YT
                // can only be optional
                auto* ytOptionalType = static_cast<
                    const NYT::NTableClient::TOptionalLogicalType*>(ytType);

                auto* ytUnderlyingType = ytOptionalType->GetElement().Get();
                auto* ytDataType = static_cast<
                    const NYT::NTableClient::TSimpleLogicalType*>(ytUnderlyingType);

                unboxedValue = Convert(ysonParser, dataType, ytDataType);
            } else {
                auto* ytDataType = static_cast<
                    const NYT::NTableClient::TSimpleLogicalType*>(ytType);

                unboxedValue = Convert(ysonParser, dataType, ytDataType);
            }
        } else if (type->IsTuple()) {
            auto* tupleType = static_cast<const NKikimr::NMiniKQL::TTupleType*>(type);
            auto* ytTupleType = static_cast<
                const NYT::NTableClient::TTupleLogicalType*>(ytType);

            unboxedValue = Convert(ysonParser, tupleType, ytTupleType);
        } else if (type->IsStruct()) {
            auto* structType = static_cast<const NKikimr::NMiniKQL::TStructType*>(type);
            auto* ytStructType = static_cast<
                const NYT::NTableClient::TStructLogicalType*>(ytType);

            unboxedValue = Convert(ysonParser, structType, ytStructType);
        } else if (type->IsList()) {
            auto* listType = static_cast<const NKikimr::NMiniKQL::TListType*>(type);
            auto* ytListType = static_cast<
                const NYT::NTableClient::TListLogicalType*>(ytType);

            unboxedValue = Convert(ysonParser, listType, ytListType);
        } else if (type->IsOptional()) {
            auto* optionalType = static_cast<
                const NKikimr::NMiniKQL::TOptionalType*>(type);

            auto* ytOptionalType = static_cast<
                const NYT::NTableClient::TOptionalLogicalType*>(ytType);

            unboxedValue = Convert(ysonParser, optionalType, ytOptionalType);
        } else if (type->IsDict()) {
            auto* dictType = static_cast<const NKikimr::NMiniKQL::TDictType*>(type);
            auto* ytDictType = static_cast<
                const NYT::NTableClient::TDictLogicalType*>(ytType);

            unboxedValue = Convert(ysonParser, dictType, ytDictType);
        } else if (type->IsVoid()) {
            auto* voidType = static_cast<const NKikimr::NMiniKQL::TVoidType*>(type);
            auto* ytVoidType = static_cast<
                const NYT::NTableClient::TSimpleLogicalType*>(ytType);

            unboxedValue = Convert(ysonParser, voidType, ytVoidType);
        } else if (type->IsNull()) {
            auto* nullType = static_cast<const NKikimr::NMiniKQL::TNullType*>(type);
            auto* ytNullType = static_cast<
                const NYT::NTableClient::TSimpleLogicalType*>(ytType);

            unboxedValue = Convert(ysonParser, nullType, ytNullType);
        } else if (type->IsTagged()) {
            auto* taggedType = static_cast<const NKikimr::NMiniKQL::TTaggedType*>(type);
            auto* ytTaggedType = static_cast<
                const NYT::NTableClient::TTaggedLogicalType*>(ytType);

            unboxedValue = Convert(ysonParser, taggedType, ytTaggedType);
        } else if (type->IsVariant()) {
            auto* variantType = static_cast<
                const NKikimr::NMiniKQL::TVariantType*>(type);

            auto* underlyingType = variantType->GetUnderlyingType();

            if (underlyingType->IsTuple()) {
                auto* ytVariantType = static_cast<
                    const NYT::NTableClient::TVariantTupleLogicalType*>(ytType);

                unboxedValue = Convert(ysonParser, variantType, ytVariantType);
            } else {
                auto* ytVariantType = static_cast<
                    const NYT::NTableClient::TVariantStructLogicalType*>(ytType);

                unboxedValue = Convert(ysonParser, variantType, ytVariantType);
            }
        } else {
            YQL_ENSURE(false, "Unsupported type: " << type->GetKindAsStr());
        }

        return unboxedValue;
    }

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NKikimr::NMiniKQL::TDataType* type,
        const NYT::NTableClient::TSimpleLogicalType* /* ytType */
    ) {
        NYql::NUdf::TUnboxedValue unboxedValue;

        switch (*type->GetDataSlot()) {
        case NYql::NUdf::EDataSlot::String:
        case NYql::NUdf::EDataSlot::Json:
        case NYql::NUdf::EDataSlot::Utf8:
        case NYql::NUdf::EDataSlot::Yson:
            unboxedValue = NYql::NUdf::TUnboxedValuePod(
                NYql::NUdf::TStringValue(
                    NYql::NUdf::TStringRef(ysonParser.ParseString())));

            break;

        case NYql::NUdf::EDataSlot::Uuid:
            unboxedValue = ConvertUuid(ysonParser.ParseString());
            break;

        case NYql::NUdf::EDataSlot::Int8:
            unboxedValue = NYql::NUdf::TUnboxedValuePod(
                static_cast<i8>(ysonParser.ParseInt64()));

            break;

        case NYql::NUdf::EDataSlot::Int16:
            unboxedValue = NYql::NUdf::TUnboxedValuePod(
                static_cast<i16>(ysonParser.ParseInt64()));

            break;

        case NYql::NUdf::EDataSlot::Int32:
            unboxedValue = NYql::NUdf::TUnboxedValuePod(
                static_cast<i32>(ysonParser.ParseInt64()));

            break;

        case NYql::NUdf::EDataSlot::Int64:
        case NYql::NUdf::EDataSlot::Interval:
        case NYql::NUdf::EDataSlot::Date32:
        case NYql::NUdf::EDataSlot::Datetime64:
        case NYql::NUdf::EDataSlot::Timestamp64:
        case NYql::NUdf::EDataSlot::Interval64:
            unboxedValue = NYql::NUdf::TUnboxedValuePod(ysonParser.ParseInt64());
            break;

        case NYql::NUdf::EDataSlot::Uint8:
            unboxedValue = NYql::NUdf::TUnboxedValuePod(
                static_cast<ui8>(ysonParser.ParseUint64()));

            break;

        case NYql::NUdf::EDataSlot::Uint16:
            unboxedValue = NYql::NUdf::TUnboxedValuePod(
                static_cast<ui16>(ysonParser.ParseUint64()));

            break;

        case NYql::NUdf::EDataSlot::Uint32:
            unboxedValue = NYql::NUdf::TUnboxedValuePod(
                static_cast<ui32>(ysonParser.ParseUint64()));

            break;

        case NYql::NUdf::EDataSlot::Uint64:
        case NYql::NUdf::EDataSlot::Date:
        case NYql::NUdf::EDataSlot::Datetime:
        case NYql::NUdf::EDataSlot::Timestamp:
            unboxedValue = NYql::NUdf::TUnboxedValuePod(ysonParser.ParseUint64());
            break;

        case NYql::NUdf::EDataSlot::Float:
            unboxedValue = NYql::NUdf::TUnboxedValuePod(
                static_cast<float>(ysonParser.ParseDouble()));

            break;

        case NYql::NUdf::EDataSlot::Double:
            unboxedValue = NYql::NUdf::TUnboxedValuePod(ysonParser.ParseDouble());
            break;

        case NYql::NUdf::EDataSlot::Bool:
            unboxedValue = NYql::NUdf::TUnboxedValuePod(ysonParser.ParseBoolean());
            break;

        default:
            YQL_ENSURE(false, "Unsupported type: " << *type->GetDataSlot());
        }

        return unboxedValue;
    }

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NKikimr::NMiniKQL::TDataDecimalType* type,
        const NYT::NTableClient::TDecimalLogicalType* /*ytType*/
    ) {
        auto [precision, scale] = type->GetParams();
        auto binaryValue = ysonParser.ParseString();
        return ConvertDecimal(binaryValue, precision);
    }

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NKikimr::NMiniKQL::TTupleType* type,
        const NYT::NTableClient::TTupleLogicalType* ytType
    ) {
        TVector<NYql::NUdf::TUnboxedValue> items;
        items.reserve(type->GetElementsCount());

        ysonParser.ParseBeginList();

        for (size_t index = 0; index < type->GetElementsCount(); ++index) {
            auto* itemType = type->GetElementType(index);
            auto* ytItemType = ytType->GetElements()[index].Get();

            auto item = Convert(ysonParser, itemType, ytItemType);
            items.push_back(std::move(item));
        }

        ysonParser.ParseEndList();

        return ValueBuilder.NewList(items.data(), items.size());
    }

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NKikimr::NMiniKQL::TStructType* type,
        const NYT::NTableClient::TStructLogicalType* ytType
    ) {
        TVector<NYql::NUdf::TUnboxedValue> items;
        items.resize(type->GetMembersCount());

        ysonParser.ParseBeginList();

        const auto& ytFields = ytType->GetFields();

        for (
            ui32 ytMemberIndex = 0;
            ytMemberIndex < ytFields.size();
            ++ytMemberIndex
        ) {
            auto memberDescriptorKey = std::pair(ytType, ytMemberIndex);
            auto memberDescriptor = StructPrecomputes.YtMemberDescriptors[memberDescriptorKey];
            auto memberIndex = memberDescriptor.Index;

            auto* memberType = memberDescriptor.Type;
            auto* ytMemberType = memberDescriptor.YtType;

            if (memberIndex) {
                items[*memberIndex] = Convert(ysonParser, memberType, ytMemberType);
            } else {
                ValueSkipper->SkipValue(ysonParser, ytMemberType);
            }
        }

        for (const auto& memberIndex: StructPrecomputes.ExtraMembers[type]) {
            items[memberIndex] = NYql::NUdf::TUnboxedValue();
        }

        ysonParser.ParseEndList();

        return ValueBuilder.NewList(items.data(), items.size());
    }

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NKikimr::NMiniKQL::TListType* type,
        const NYT::NTableClient::TListLogicalType* ytType
    ) {
        auto* itemType = type->GetItemType();
        auto* ytItemType = ytType->GetElement().Get();

        TVector<NYql::NUdf::TUnboxedValue> items;

        ysonParser.ParseBeginList();

        while (!ysonParser.IsEndList()) {
            auto item = Convert(ysonParser, itemType, ytItemType);
            items.push_back(std::move(item));
        }

        ysonParser.ParseEndList();

        return ValueBuilder.NewList(items.data(), items.size());
    }

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NKikimr::NMiniKQL::TOptionalType* type,
        const NYT::NTableClient::TOptionalLogicalType* ytType
    ) {
        if (ysonParser.IsEntity()) {
            ysonParser.Next();
            return NYql::NUdf::TUnboxedValue();
        }

        auto* itemType = type->GetItemType();
        auto* ytItemType = ytType->GetElement().Get();

        if (ytType->IsElementNullable()) {
            ysonParser.ParseBeginList();
        }

        auto item = Convert(ysonParser, itemType, ytItemType);

        if (ytType->IsElementNullable()) {
            ysonParser.ParseEndList();
        }

        return item.MakeOptional();
    }

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NKikimr::NMiniKQL::TDictType* type,
        const NYT::NTableClient::TDictLogicalType* ytType
    ) {
        auto* keyType = type->GetKeyType();
        auto* payloadType = type->GetPayloadType();

        auto* ytKeyType = ytType->GetKey().Get();
        auto* ytPayloadType = ytType->GetValue().Get();

        auto dictValueBuilder = ValueBuilder.NewDict(
            DictUdfTypes[type], NYql::NUdf::TDictFlags::Hashed);

        ysonParser.ParseBeginList();

        while (!ysonParser.IsEndList()) {
            ysonParser.ParseBeginList();

            dictValueBuilder->Add(
                Convert(ysonParser, keyType, ytKeyType),
                Convert(ysonParser, payloadType, ytPayloadType));

            ysonParser.ParseEndList();
        }

        ysonParser.ParseEndList();

        return dictValueBuilder->Build();
    }

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NKikimr::NMiniKQL::TVoidType* /*type*/,
        const NYT::NTableClient::TSimpleLogicalType* /*ytType*/
    ) {
        ysonParser.ParseEntity();
        return NYql::NUdf::TUnboxedValuePod::Void();
    }

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NKikimr::NMiniKQL::TNullType* /*type*/,
        const NYT::NTableClient::TSimpleLogicalType* /*ytType*/
    ) {
        ysonParser.ParseEntity();
        return NYql::NUdf::TUnboxedValuePod::Zero();
    }

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NKikimr::NMiniKQL::TTaggedType* type,
        const NYT::NTableClient::TTaggedLogicalType* ytType
    ) {
        auto* baseType = type->GetBaseType();
        auto* ytBaseType = ytType->GetElement().Get();
        return Convert(ysonParser, baseType, ytBaseType);
    }

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NKikimr::NMiniKQL::TVariantType* type,
        const NYT::NTableClient::TVariantTupleLogicalType* ytType
    ) {
        ysonParser.ParseBeginList();

        auto ytAlternativeIndex = static_cast<ui32>(ysonParser.ParseInt64());
        auto alternativeIndex = ytAlternativeIndex;

        auto* alternativeType = type->GetAlternativeType(alternativeIndex);
        auto* ytAlternativeType = ytType->GetElements()[ytAlternativeIndex].Get();

        auto alternative = Convert(ysonParser, alternativeType, ytAlternativeType);

        ysonParser.ParseEndList();

        return ValueBuilder.NewVariant(alternativeIndex, std::move(alternative));
    }

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NKikimr::NMiniKQL::TVariantType* /*type*/,
        const NYT::NTableClient::TVariantStructLogicalType* ytType
    ) {
        ysonParser.ParseBeginList();

        auto ytAlternativeIndex = static_cast<ui32>(ysonParser.ParseInt64());
        auto ytField = ytType->GetFields()[ytAlternativeIndex];

        auto memberDescriptorKey = std::pair(ytType, ytAlternativeIndex);
        auto memberDescriptor = StructPrecomputes.YtMemberDescriptors[memberDescriptorKey];

        auto alternativeIndex = memberDescriptor.Index;

        auto* alternativeType = memberDescriptor.Type;
        auto* ytAlternativeType = memberDescriptor.YtType;

        auto alternative = Convert(ysonParser, alternativeType, ytAlternativeType);

        ysonParser.ParseEndList();

        return ValueBuilder.NewVariant(*alternativeIndex, std::move(alternative));
    }

    NYql::NUdf::TUnboxedValue ConvertUuid(TStringBuf ytBytes) {
        // uuid value's byte representation is different for YT & YQL:
        // YT: bytes of original uuid are stored from LSB to MSB (from 0 to 15)
        // YQL: bytes of original uuid are permuted as [3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15]
        // (details: https://a.yandex-team.ru/arcadia/yql/essentials/types/uuid/uuid.h?rev=r15590989#L94-97)
        // so a bit of byte magic is required

        union {
            ui16 dw[8];
            ui64 half[2];
        } data;

        NKikimr::NUuid::UuidBytesToHalfs(
            ytBytes.data(), ytBytes.size(),
            data.half[1], data.half[0]);

        std::swap(data.dw[0], data.dw[1]);
        for (ui32 i = 0; i < 4; ++i) {
            data.dw[i] = ((data.dw[i] >> 8) & 0xff) | ((data.dw[i] & 0xff) << 8);
        }

        TBuffer buffer(NKikimr::NUuid::UUID_LEN);
        NKikimr::NUuid::UuidHalfsToBytes(
            buffer.data(), NKikimr::NUuid::UUID_LEN,
            data.half[1], data.half[0]);

        return ValueBuilder.NewString(
            NYql::NUdf::TStringRef(buffer.data(), NKikimr::NUuid::UUID_LEN));
    }

    NYql::NUdf::TUnboxedValue ConvertDecimal(TStringBuf binaryValue, int precision) {
        i128 value;
        auto valueBinarySize = NYT::NDecimal::TDecimal::GetValueBinarySize(precision);

        switch (valueBinarySize) {
        case 4:
            value = NYT::NDecimal::TDecimal::ParseBinary32(precision, binaryValue);
            break;

        case 16:
            value = NYT::NDecimal::TDecimal::ParseBinary64(precision, binaryValue);
            break;

        case 32: {
            NYT::NDecimal::TDecimal::TValue128 ytValue =
                NYT::NDecimal::TDecimal::ParseBinary128(precision, binaryValue);

            value = static_cast<i128>(ytValue.High) << 64 | ytValue.Low;
            break;
        }

        default:
            YQL_ENSURE(
                false,
                "Unsupported value binary size: " << valueBinarySize
                    << " (precision: " << precision << ")");
        }

        return NYql::NUdf::TUnboxedValuePod(NYql::NDecimal::TInt128(value));
    }

private:
    const NKikimr::NMiniKQL::TType* Type;
    NYql::NUdf::IValueBuilder& ValueBuilder;
    TConvertOptions ConvertOptions;
    THolder<IValueSkipper> ValueSkipper;

    NYT::NTableClient::TLogicalTypePtr YtType;
    TStructPrecomputes StructPrecomputes;
    THashMap<const NKikimr::NMiniKQL::TType*, const NYql::NUdf::TType*> DictUdfTypes;
};

class TRowInputCodec final
    : public TInputCodecBase
    , public IRowInputCodec
{
public:
    using TInputCodecBase::TInputCodecBase;

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NTableClient::TUnversionedRow unversionedRow
    ) override {
        return ConvertRow(unversionedRow);
    }
};

class TValueInputCodec final
    : public TInputCodecBase
    , public IValueInputCodec
{
public:
    using TInputCodecBase::TInputCodecBase;

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NTableClient::TUnversionedValue unversionedValue
    ) override {
        return ConvertValue(unversionedValue);
    }

    NYql::NUdf::TUnboxedValue Convert(
        NYT::NTableClient::TUnversionedValueRange unversionedValues
    ) override {
        return ConvertValueRange(unversionedValues);
    }
};

} // namespace NYql::NYtflow::NCodec::NPrivate


namespace NYql::NYtflow::NCodec {

THolder<IRowInputCodec> CreateRowInputCodec(
    const NKikimr::NMiniKQL::TType* type,
    NYT::NTableClient::TTableSchemaPtr ytSchema,
    NYql::NUdf::IValueBuilder& valueBuilder,
    NYql::NUdf::IFunctionTypeInfoBuilder& functionTypeInfoBuilder,
    const TConvertOptions& convertOptions
) {
    YQL_ENSURE(
        type->IsStruct(),
        "Row input codec is only supported for struct input types");

    return MakeHolder<NPrivate::TRowInputCodec>(
        type,
        NPrivate::TInputCodecBase::BuildStructYtType(ytSchema),
        valueBuilder,
        functionTypeInfoBuilder,
        convertOptions);
}

THolder<IValueInputCodec> CreateValueInputCodec(
    const NKikimr::NMiniKQL::TType* type,
    NYT::NTableClient::TLogicalTypePtr ytType,
    NYql::NUdf::IValueBuilder& valueBuilder,
    NYql::NUdf::IFunctionTypeInfoBuilder& functionTypeInfoBuilder,
    const TConvertOptions& convertOptions
) {
    return MakeHolder<NPrivate::TValueInputCodec>(
        type,
        std::move(ytType),
        valueBuilder,
        functionTypeInfoBuilder,
        convertOptions);
}

} // namespace NYql::NYtflow::NCodec

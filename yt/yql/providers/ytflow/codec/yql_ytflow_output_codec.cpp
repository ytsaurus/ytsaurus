#include "yql_ytflow_output_codec.h"
#include "yql_ytflow_member_descriptor.h"
#include "yql_ytflow_struct_precomputes.h"
#include "yql_ytflow_type_helpers.h"

#include <library/cpp/yt/memory/new.h>

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/types/uuid/uuid.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/core/yson/writer.h>
#include <yt/yt/library/decimal/decimal.h>

#include <util/generic/hash.h>
#include <util/stream/str.h>


namespace NYql::NYtflow::NCodec::NPrivate {

class TOutputCodecBase {
public:
    TOutputCodecBase(
        const NKikimr::NMiniKQL::TType* type,
        NYT::NTableClient::TLogicalTypePtr ytType,
        NYT::NTableClient::TRowBufferPtr rowBuffer,
        TConvertOptions convertOptions
    )
        : RowBuffer(std::move(rowBuffer))
        , ConvertOptions(convertOptions.WithConvertDirection(EConvertDirection::YtToYql))
    {
        Type = type;
        YtType = std::move(ytType);

        ValidateTypesCorrespondence(Type, YtType.Get(), ConvertOptions);

        StructPrecomputes = BuildStructPrecomputes(Type, YtType.Get(), ConvertOptions);
    }

    virtual ~TOutputCodecBase() = default;

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
    NYT::NTableClient::TUnversionedRow ConvertRow(
        NYql::NUdf::TUnboxedValue unboxedValue
    ) {
        YQL_ENSURE(
            Type->IsStruct(),
            "Method is only supported for struct output types");

        auto* ytStructType = static_cast<
            const NYT::NTableClient::TStructLogicalType*>(YtType.Get());

        const auto& ytFields = ytStructType->GetFields();

        auto mutableUnversionedRow = RowBuffer->AllocateUnversioned(ytFields.size());

        for (
            ui32 ytMemberIndex = 0;
            ytMemberIndex < ytFields.size();
            ++ytMemberIndex
        ) {
            auto memberDescriptorKey = std::pair(YtType.Get(), ytMemberIndex);
            auto memberDescriptor = StructPrecomputes.YtMemberDescriptors[memberDescriptorKey];
            auto memberIndex = memberDescriptor.Index;

            auto* memberType = memberDescriptor.Type;
            auto* ytMemberType = memberDescriptor.YtType;

            NYT::NTableClient::TUnversionedValue unversionedValue;

            if (!memberIndex) {
                unversionedValue = NYT::NTableClient::MakeUnversionedNullValue();
            } else {
                unversionedValue = Convert(
                    unboxedValue.GetElement(*memberIndex),
                    memberType,
                    ytMemberType);
            }

            unversionedValue.Id = ytMemberIndex;
            mutableUnversionedRow[ytMemberIndex] = std::move(unversionedValue);
        }

        return mutableUnversionedRow;
    }

    NYT::NTableClient::TUnversionedValue ConvertValue(
        NYql::NUdf::TUnboxedValue unboxedValue
    ) {
        return Convert(unboxedValue, Type, YtType.Get());
    }

private:
    NYT::NTableClient::TUnversionedValue Convert(
        NYql::NUdf::TUnboxedValue unboxedValue,
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

                return Convert(unboxedValue, decimalType, ytDecimalType);
            } else if (*dataType->GetDataSlot() == NYql::NUdf::EDataSlot::Yson) {
                // YQL & YT type systems disagree here:
                // it's possible to have non-optional Yson in YQL, but Any in YT
                // can only be optional
                auto* ytOptionalType = static_cast<
                    const NYT::NTableClient::TOptionalLogicalType*>(ytType);

                auto* ytUnderlyingType = ytOptionalType->GetElement().Get();
                auto* ytDataType = static_cast<
                    const NYT::NTableClient::TSimpleLogicalType*>(ytUnderlyingType);

                return Convert(unboxedValue, dataType, ytDataType);
            } else {
                auto* ytDataType = static_cast<
                    const NYT::NTableClient::TSimpleLogicalType*>(ytType);

                return Convert(unboxedValue, dataType, ytDataType);
            }
        } else if (type->IsVoid()) {
            auto* voidType = static_cast<const NKikimr::NMiniKQL::TVoidType*>(type);
            auto* ytVoidType = static_cast<
                const NYT::NTableClient::TSimpleLogicalType*>(ytType);

            return Convert(unboxedValue, voidType, ytVoidType);
        } else if (type->IsNull()) {
            auto* nullType = static_cast<const NKikimr::NMiniKQL::TNullType*>(type);
            auto* ytNullType = static_cast<
                const NYT::NTableClient::TSimpleLogicalType*>(ytType);

            return Convert(unboxedValue, nullType, ytNullType);
        } else if (type->IsOptional()) {
            auto* ytOptionalType = static_cast<
                const NYT::NTableClient::TOptionalLogicalType*>(ytType);

            if (!ytOptionalType->IsElementNullable()) {
                auto* optionalType = static_cast<
                    const NKikimr::NMiniKQL::TOptionalType*>(type);

                return Convert(unboxedValue, optionalType, ytOptionalType);
            }
        } else if (type->IsTagged()) {
            auto* taggedType = static_cast<const NKikimr::NMiniKQL::TTaggedType*>(type);
            auto* ytTaggedType = static_cast<
                const NYT::NTableClient::TTaggedLogicalType*>(ytType);

            return Convert(unboxedValue, taggedType, ytTaggedType);
        }

        auto stream = TStringStream();
        auto writer = NYT::NYson::TYsonWriter(&stream);

        Convert(unboxedValue, writer, type, ytType);

        auto unversionedValue = RowBuffer->CaptureValue(
            NYT::NTableClient::MakeUnversionedCompositeValue(stream.Str()));

        return unversionedValue;
    }

    NYT::NTableClient::TUnversionedValue Convert(
        NYql::NUdf::TUnboxedValue unboxedValue,
        const NKikimr::NMiniKQL::TDataType* type,
        const NYT::NTableClient::TSimpleLogicalType* /*ytType*/
    ) {
        NYT::NTableClient::TUnversionedValue unversionedValue;

        switch (*type->GetDataSlot()) {
        case NYql::NUdf::EDataSlot::String:
        case NYql::NUdf::EDataSlot::Json:
        case NYql::NUdf::EDataSlot::Utf8:
            unversionedValue = RowBuffer->CaptureValue(
                NYT::NTableClient::MakeUnversionedStringValue(
                    TStringBuf(unboxedValue.AsStringRef())));

            break;

        case NYql::NUdf::EDataSlot::Uuid: {
            auto convertedUuid = ConvertUuid(unboxedValue.AsStringRef());
            unversionedValue = RowBuffer->CaptureValue(
                NYT::NTableClient::MakeUnversionedStringValue(convertedUuid));

            break;
        }

        case NYql::NUdf::EDataSlot::Int8:
            unversionedValue = NYT::NTableClient::MakeUnversionedInt64Value(
                unboxedValue.Get<i8>());

            break;

        case NYql::NUdf::EDataSlot::Int16:
            unversionedValue = NYT::NTableClient::MakeUnversionedInt64Value(
                unboxedValue.Get<i16>());

            break;

        case NYql::NUdf::EDataSlot::Int32:
            unversionedValue = NYT::NTableClient::MakeUnversionedInt64Value(
                unboxedValue.Get<i32>());

            break;

        case NYql::NUdf::EDataSlot::Int64:
        case NYql::NUdf::EDataSlot::Interval:
        case NYql::NUdf::EDataSlot::Date32:
        case NYql::NUdf::EDataSlot::Datetime64:
        case NYql::NUdf::EDataSlot::Timestamp64:
        case NYql::NUdf::EDataSlot::Interval64:
            unversionedValue = NYT::NTableClient::MakeUnversionedInt64Value(
                unboxedValue.Get<i64>());

            break;

        case NYql::NUdf::EDataSlot::Uint8:
            unversionedValue = NYT::NTableClient::MakeUnversionedUint64Value(
                unboxedValue.Get<i8>());

            break;

        case NYql::NUdf::EDataSlot::Uint16:
            unversionedValue = NYT::NTableClient::MakeUnversionedUint64Value(
                unboxedValue.Get<i16>());

            break;

        case NYql::NUdf::EDataSlot::Uint32:
            unversionedValue = NYT::NTableClient::MakeUnversionedUint64Value(
                unboxedValue.Get<i32>());

            break;

        case NYql::NUdf::EDataSlot::Uint64:
        case NYql::NUdf::EDataSlot::Date:
        case NYql::NUdf::EDataSlot::Datetime:
        case NYql::NUdf::EDataSlot::Timestamp:
            unversionedValue = NYT::NTableClient::MakeUnversionedUint64Value(
                unboxedValue.Get<i64>());

            break;

        case NYql::NUdf::EDataSlot::Float:
            unversionedValue = NYT::NTableClient::MakeUnversionedDoubleValue(
                unboxedValue.Get<float>());

            break;

        case NYql::NUdf::EDataSlot::Double:
            unversionedValue = NYT::NTableClient::MakeUnversionedDoubleValue(
                unboxedValue.Get<double>());

            break;

        case NYql::NUdf::EDataSlot::Bool:
            unversionedValue = NYT::NTableClient::MakeUnversionedBooleanValue(
                unboxedValue.Get<bool>());

            break;

        case NYql::NUdf::EDataSlot::Yson:
            unversionedValue = RowBuffer->CaptureValue(
                NYT::NTableClient::MakeUnversionedAnyValue(
                    TStringBuf(unboxedValue.AsStringRef())));

            break;

        default:
            YQL_ENSURE(false, "Unsupported type: " << *type->GetDataSlot());
        }

        return unversionedValue;
    }

    NYT::NTableClient::TUnversionedValue Convert(
        NYql::NUdf::TUnboxedValue unboxedValue,
        const NKikimr::NMiniKQL::TDataDecimalType* type,
        const NYT::NTableClient::TDecimalLogicalType* /*ytType*/
    ) {
        auto [precision, scale] = type->GetParams();
        auto convertedDecimal = ConvertDecimal(unboxedValue.GetInt128(), precision);

        return RowBuffer->CaptureValue(
            NYT::NTableClient::MakeUnversionedStringValue(convertedDecimal));
    }

    NYT::NTableClient::TUnversionedValue Convert(
        NYql::NUdf::TUnboxedValue unboxedValue,
        const NKikimr::NMiniKQL::TOptionalType* type,
        const NYT::NTableClient::TOptionalLogicalType* ytType
    ) {
        // immediate top-level version only, composite & nested are handled via ysonWriter
        if (!unboxedValue) {
            return NYT::NTableClient::MakeUnversionedNullValue();
        }

        auto* itemType = type->GetItemType();
        auto* ytItemType = ytType->GetElement().Get();

        return Convert(unboxedValue.GetOptionalValue(), itemType, ytItemType);
    }

    NYT::NTableClient::TUnversionedValue Convert(
        NYql::NUdf::TUnboxedValue unboxedValue,
        const NKikimr::NMiniKQL::TVoidType* /*type*/,
        const NYT::NTableClient::TSimpleLogicalType* /*ytType*/
    ) {
        YQL_ENSURE(unboxedValue.IsEmbedded());
        return NYT::NTableClient::MakeUnversionedNullValue();
    }

    NYT::NTableClient::TUnversionedValue Convert(
        NYql::NUdf::TUnboxedValue unboxedValue,
        const NKikimr::NMiniKQL::TNullType* /*type*/,
        const NYT::NTableClient::TSimpleLogicalType* /*ytType*/
    ) {
        YQL_ENSURE(unboxedValue.IsEmbedded());
        return NYT::NTableClient::MakeUnversionedNullValue();
    }

    NYT::NTableClient::TUnversionedValue Convert(
        NYql::NUdf::TUnboxedValue unboxedValue,
        const NKikimr::NMiniKQL::TTaggedType* type,
        const NYT::NTableClient::TTaggedLogicalType* ytType
    ) {
        auto* baseType = type->GetBaseType();
        auto* ytBaseType = ytType->GetElement().Get();
        return Convert(unboxedValue, baseType, ytBaseType);
    }

    void Convert(
        NYql::NUdf::TUnboxedValue unboxedValue,
        NYT::NYson::TYsonWriter& ysonWriter,
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

                Convert(unboxedValue, ysonWriter, decimalType, ytDecimalType);
            } else if (*dataType->GetDataSlot() == NYql::NUdf::EDataSlot::Yson) {
                // YQL & YT type systems disagree here:
                // it's possible to have non-optional Yson in YQL, but Any in YT
                // can only be optional
                auto* ytOptionalType = static_cast<
                    const NYT::NTableClient::TOptionalLogicalType*>(ytType);

                auto* ytUnderlyingType = ytOptionalType->GetElement().Get();
                auto* ytDataType = static_cast<
                    const NYT::NTableClient::TSimpleLogicalType*>(ytUnderlyingType);

                Convert(unboxedValue, ysonWriter, dataType, ytDataType);
            } else {
                auto* ytDataType = static_cast<
                    const NYT::NTableClient::TSimpleLogicalType*>(ytType);

                Convert(unboxedValue, ysonWriter, dataType, ytDataType);
            }
        } else if (type->IsTuple()) {
            auto* tupleType = static_cast<const NKikimr::NMiniKQL::TTupleType*>(type);
            auto* ytTupleType = static_cast<
                const NYT::NTableClient::TTupleLogicalType*>(ytType);

            Convert(unboxedValue, ysonWriter, tupleType, ytTupleType);
        } else if (type->IsStruct()) {
            auto* structType = static_cast<const NKikimr::NMiniKQL::TStructType*>(type);
            auto* ytStructType = static_cast<
                const NYT::NTableClient::TStructLogicalType*>(ytType);

            Convert(unboxedValue, ysonWriter, structType, ytStructType);
        } else if (type->IsList()) {
            auto* listType = static_cast<const NKikimr::NMiniKQL::TListType*>(type);
            auto* ytListType = static_cast<
                const NYT::NTableClient::TListLogicalType*>(ytType);

            Convert(unboxedValue, ysonWriter, listType, ytListType);
        } else if (type->IsOptional()) {
            auto* optionalType = static_cast<
                const NKikimr::NMiniKQL::TOptionalType*>(type);

            auto* ytOptionalType = static_cast<
                const NYT::NTableClient::TOptionalLogicalType*>(ytType);

            Convert(unboxedValue, ysonWriter, optionalType, ytOptionalType);
        } else if (type->IsDict()) {
            auto* dictType = static_cast<const NKikimr::NMiniKQL::TDictType*>(type);
            auto* ytDictType = static_cast<
                const NYT::NTableClient::TDictLogicalType*>(ytType);

            Convert(unboxedValue, ysonWriter, dictType, ytDictType);
        } else if (type->IsVoid()) {
            auto* voidType = static_cast<const NKikimr::NMiniKQL::TVoidType*>(type);
            auto* ytVoidType = static_cast<
                const NYT::NTableClient::TSimpleLogicalType*>(ytType);

            Convert(unboxedValue, ysonWriter, voidType, ytVoidType);
        } else if (type->IsNull()) {
            auto* nullType = static_cast<const NKikimr::NMiniKQL::TNullType*>(type);
            auto* ytNullType = static_cast<
                const NYT::NTableClient::TSimpleLogicalType*>(ytType);

            Convert(unboxedValue, ysonWriter, nullType, ytNullType);
        } else if (type->IsTagged()) {
            auto* taggedType = static_cast<const NKikimr::NMiniKQL::TTaggedType*>(type);
            auto* ytTaggedType = static_cast<
                const NYT::NTableClient::TTaggedLogicalType*>(ytType);

            Convert(unboxedValue, ysonWriter, taggedType, ytTaggedType);
        } else if (type->IsVariant()) {
            auto* variantType = static_cast<
                const NKikimr::NMiniKQL::TVariantType*>(type);

            auto* underlyingType = variantType->GetUnderlyingType();

            if (underlyingType->IsTuple()) {
                auto* ytVariantType = static_cast<
                    const NYT::NTableClient::TVariantTupleLogicalType*>(ytType);

                Convert(unboxedValue, ysonWriter, variantType, ytVariantType);
            } else {
                auto* ytVariantType = static_cast<
                    const NYT::NTableClient::TVariantStructLogicalType*>(ytType);

                Convert(unboxedValue, ysonWriter, variantType, ytVariantType);
            }
        } else {
            YQL_ENSURE(false, "Unsupported type: " << type->GetKindAsStr());
        }
    }

    void Convert(
        NYql::NUdf::TUnboxedValue unboxedValue,
        NYT::NYson::TYsonWriter& ysonWriter,
        const NKikimr::NMiniKQL::TDataType* type,
        const NYT::NTableClient::TSimpleLogicalType* /* ytType */
    ) {
        switch (*type->GetDataSlot()) {
        case NYql::NUdf::EDataSlot::String:
        case NYql::NUdf::EDataSlot::Json:
        case NYql::NUdf::EDataSlot::Utf8:
        case NYql::NUdf::EDataSlot::Yson:
            ysonWriter.OnStringScalar(TStringBuf(unboxedValue.AsStringRef()));
            break;

        case NYql::NUdf::EDataSlot::Uuid: {
            auto convertedUuid = ConvertUuid(unboxedValue.AsStringRef());
            ysonWriter.OnStringScalar(convertedUuid);
            break;
        }

        case NYql::NUdf::EDataSlot::Int8:
            ysonWriter.OnInt64Scalar(unboxedValue.Get<i8>());
            break;

        case NYql::NUdf::EDataSlot::Int16:
            ysonWriter.OnInt64Scalar(unboxedValue.Get<i16>());
            break;

        case NYql::NUdf::EDataSlot::Int32:
            ysonWriter.OnInt64Scalar(unboxedValue.Get<i32>());
            break;

        case NYql::NUdf::EDataSlot::Int64:
        case NYql::NUdf::EDataSlot::Interval:
        case NYql::NUdf::EDataSlot::Date32:
        case NYql::NUdf::EDataSlot::Datetime64:
        case NYql::NUdf::EDataSlot::Timestamp64:
        case NYql::NUdf::EDataSlot::Interval64:
            ysonWriter.OnInt64Scalar(unboxedValue.Get<i64>());
            break;

        case NYql::NUdf::EDataSlot::Uint8:
            ysonWriter.OnUint64Scalar(unboxedValue.Get<ui8>());
            break;

        case NYql::NUdf::EDataSlot::Uint16:
            ysonWriter.OnUint64Scalar(unboxedValue.Get<ui16>());
            break;

        case NYql::NUdf::EDataSlot::Uint32:
            ysonWriter.OnUint64Scalar(unboxedValue.Get<ui32>());
            break;

        case NYql::NUdf::EDataSlot::Uint64:
        case NYql::NUdf::EDataSlot::Date:
        case NYql::NUdf::EDataSlot::Datetime:
        case NYql::NUdf::EDataSlot::Timestamp:
            ysonWriter.OnUint64Scalar(unboxedValue.Get<ui64>());
            break;

        case NYql::NUdf::EDataSlot::Float:
            ysonWriter.OnDoubleScalar(unboxedValue.Get<float>());
            break;

        case NYql::NUdf::EDataSlot::Double:
            ysonWriter.OnDoubleScalar(unboxedValue.Get<double>());
            break;

        case NYql::NUdf::EDataSlot::Bool:
            ysonWriter.OnBooleanScalar(unboxedValue.Get<bool>());
            break;

        default:
            YQL_ENSURE(false, "Unsupported type: " << *type->GetDataSlot());
        }
    }

    void Convert(
        NYql::NUdf::TUnboxedValue unboxedValue,
        NYT::NYson::TYsonWriter& ysonWriter,
        const NKikimr::NMiniKQL::TDataDecimalType* type,
        const NYT::NTableClient::TDecimalLogicalType* /*ytType*/
    ) {
        auto [precision, scale] = type->GetParams();
        auto convertedDecimal = ConvertDecimal(unboxedValue.GetInt128(), precision);
        ysonWriter.OnStringScalar(convertedDecimal);
    }

    void Convert(
        NYql::NUdf::TUnboxedValue unboxedValue,
        NYT::NYson::TYsonWriter& ysonWriter,
        const NKikimr::NMiniKQL::TTupleType* type,
        const NYT::NTableClient::TTupleLogicalType* ytType
    ) {
        ysonWriter.OnBeginList();

        for (size_t index = 0; index < type->GetElementsCount(); ++index) {
            ysonWriter.OnListItem();

            auto* itemType = type->GetElementType(index);
            auto* ytItemType = ytType->GetElements()[index].Get();
            Convert(unboxedValue.GetElement(index), ysonWriter, itemType, ytItemType);
        }

        ysonWriter.OnEndList();
    }

    void Convert(
        NYql::NUdf::TUnboxedValue unboxedValue,
        NYT::NYson::TYsonWriter& ysonWriter,
        const NKikimr::NMiniKQL::TStructType* /*type*/,
        const NYT::NTableClient::TStructLogicalType* ytType
    ) {
        ysonWriter.OnBeginList();

        const auto& ytFields = ytType->GetFields();

        for (
            ui32 ytMemberIndex = 0;
            ytMemberIndex < ytFields.size();
            ++ytMemberIndex
        ) {
            ysonWriter.OnListItem();

            auto memberDescriptorKey = std::pair(ytType, ytMemberIndex);
            auto memberDescriptor = StructPrecomputes.YtMemberDescriptors[memberDescriptorKey];
            auto memberIndex = memberDescriptor.Index;

            auto* memberType = memberDescriptor.Type;
            auto* ytMemberType = memberDescriptor.YtType;

            if (!memberIndex) {
                ysonWriter.OnEntity();
                continue;
            }

            Convert(
                unboxedValue.GetElement(*memberIndex),
                ysonWriter,
                memberType,
                ytMemberType);
        }

        ysonWriter.OnEndList();
    }

    void Convert(
        NYql::NUdf::TUnboxedValue unboxedValue,
        NYT::NYson::TYsonWriter& ysonWriter,
        const NKikimr::NMiniKQL::TListType* type,
        const NYT::NTableClient::TListLogicalType* ytType
    ) {
        auto* itemType = type->GetItemType();
        auto* ytItemType = ytType->GetElement().Get();

        auto listIterator = unboxedValue.GetListIterator();
        NYql::NUdf::TUnboxedValue item;

        ysonWriter.OnBeginList();

        while (listIterator.Next(item)) {
            ysonWriter.OnListItem();
            Convert(item, ysonWriter, itemType, ytItemType);
        }

        ysonWriter.OnEndList();
    }

    void Convert(
        NYql::NUdf::TUnboxedValue unboxedValue,
        NYT::NYson::TYsonWriter& ysonWriter,
        const NKikimr::NMiniKQL::TOptionalType* type,
        const NYT::NTableClient::TOptionalLogicalType* ytType
    ) {
        if (!unboxedValue) {
            ysonWriter.OnEntity();
            return;
        }

        auto* itemType = type->GetItemType();
        auto* ytItemType = ytType->GetElement().Get();

        if (ytType->IsElementNullable()) {
            ysonWriter.OnBeginList();
            ysonWriter.OnListItem();
        }

        Convert(unboxedValue.GetOptionalValue(), ysonWriter, itemType, ytItemType);

        if (ytType->IsElementNullable()) {
            ysonWriter.OnEndList();
        }
    }

    void Convert(
        NYql::NUdf::TUnboxedValue unboxedValue,
        NYT::NYson::TYsonWriter& ysonWriter,
        const NKikimr::NMiniKQL::TDictType* type,
        const NYT::NTableClient::TDictLogicalType* ytType
    ) {
        auto* keyType = type->GetKeyType();
        auto* payloadType = type->GetPayloadType();

        auto* ytKeyType = ytType->GetKey().Get();
        auto* ytPayloadType = ytType->GetValue().Get();

        auto dictIterator = unboxedValue.GetDictIterator();
        NYql::NUdf::TUnboxedValue key, payload;

        ysonWriter.OnBeginList();

        while (dictIterator.NextPair(key, payload)) {
            ysonWriter.OnListItem();

            ysonWriter.OnBeginList();

            ysonWriter.OnListItem();
            Convert(key, ysonWriter, keyType, ytKeyType);

            ysonWriter.OnListItem();
            Convert(payload, ysonWriter, payloadType, ytPayloadType);

            ysonWriter.OnEndList();
        }

        ysonWriter.OnEndList();
    }

    void Convert(
        NYql::NUdf::TUnboxedValue unboxedValue,
        NYT::NYson::TYsonWriter& ysonWriter,
        const NKikimr::NMiniKQL::TVoidType* /*type*/,
        const NYT::NTableClient::TSimpleLogicalType* /*ytType*/
    ) {
        YQL_ENSURE(unboxedValue.IsEmbedded());
        ysonWriter.OnEntity();
    }

    void Convert(
        NYql::NUdf::TUnboxedValue unboxedValue,
        NYT::NYson::TYsonWriter& ysonWriter,
        const NKikimr::NMiniKQL::TNullType* /*type*/,
        const NYT::NTableClient::TSimpleLogicalType* /*ytType*/
    ) {
        YQL_ENSURE(unboxedValue.IsEmbedded());
        ysonWriter.OnEntity();
    }

    void Convert(
        NYql::NUdf::TUnboxedValue unboxedValue,
        NYT::NYson::TYsonWriter& ysonWriter,
        const NKikimr::NMiniKQL::TTaggedType* type,
        const NYT::NTableClient::TTaggedLogicalType* ytType
    ) {
        auto* baseType = type->GetBaseType();
        auto* ytBaseType = ytType->GetElement().Get();
        Convert(unboxedValue, ysonWriter, baseType, ytBaseType);
    }

    void Convert(
        NYql::NUdf::TUnboxedValue unboxedValue,
        NYT::NYson::TYsonWriter& ysonWriter,
        const NKikimr::NMiniKQL::TVariantType* type,
        const NYT::NTableClient::TVariantTupleLogicalType* ytType
    ) {
        auto alternativeIndex = unboxedValue.GetVariantIndex();
        auto alternative = unboxedValue.GetVariantItem();

        auto ytAlternativeIndex = alternativeIndex;

        auto* alternativeType = type->GetAlternativeType(alternativeIndex);
        auto* ytAlternativeType = ytType->GetElements()[ytAlternativeIndex].Get();

        ysonWriter.OnBeginList();

        ysonWriter.OnListItem();
        ysonWriter.OnInt64Scalar(alternativeIndex);

        ysonWriter.OnListItem();
        Convert(alternative, ysonWriter, alternativeType, ytAlternativeType);

        ysonWriter.OnEndList();
    }

    void Convert(
        NYql::NUdf::TUnboxedValue unboxedValue,
        NYT::NYson::TYsonWriter& ysonWriter,
        const NKikimr::NMiniKQL::TVariantType* type,
        const NYT::NTableClient::TVariantStructLogicalType* /*ytType*/
    ) {
        auto alternativeIndex = unboxedValue.GetVariantIndex();
        auto alternative = unboxedValue.GetVariantItem();

        auto* underlyingStructType = static_cast<const NKikimr::NMiniKQL::TStructType*>(
            type->GetUnderlyingType());

        auto memberDescriptorKey = std::pair(underlyingStructType, alternativeIndex);
        auto memberDescriptor = StructPrecomputes.MemberDescriptors[memberDescriptorKey];

        auto ytAlternativeIndex = *memberDescriptor.YtIndex;

        auto* alternativeType = memberDescriptor.Type;
        auto* ytAlternativeType = memberDescriptor.YtType;

        ysonWriter.OnBeginList();

        ysonWriter.OnListItem();
        ysonWriter.OnInt64Scalar(ytAlternativeIndex);

        ysonWriter.OnListItem();
        Convert(alternative, ysonWriter, alternativeType, ytAlternativeType);

        ysonWriter.OnEndList();
    }

    TString ConvertUuid(TStringBuf yqlBytes) {
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
            yqlBytes.data(), yqlBytes.size(),
            data.half[1], data.half[0]);

        std::swap(data.dw[0], data.dw[1]);
        for (ui32 i = 0; i < 4; ++i) {
            data.dw[i] = ((data.dw[i] >> 8) & 0xff) | ((data.dw[i] & 0xff) << 8);
        }

        TBuffer buffer(NKikimr::NUuid::UUID_LEN);
        NKikimr::NUuid::UuidHalfsToBytes(
            buffer.data(), NKikimr::NUuid::UUID_LEN,
            data.half[1], data.half[0]);

        buffer.Advance(NKikimr::NUuid::UUID_LEN);

        TString uuid;
        buffer.AsString(uuid);

        return uuid;
    }

    TString ConvertDecimal(NYql::NDecimal::TInt128 value, int precision) {
        auto ytValue = NYT::NDecimal::TDecimal::TValue128{
            .Low = static_cast<ui64>(value & 0x0000000000000000ffffffffffffffff),
            .High = static_cast<i64>(value >> 64),
        };

        auto ytValueBinarySize = NYT::NDecimal::TDecimal::GetValueBinarySize(precision);

        TBuffer buffer(ytValueBinarySize);

        NYT::NDecimal::TDecimal::WriteBinary128Variadic(
            precision, ytValue, buffer.data(), ytValueBinarySize);

        buffer.Advance(ytValueBinarySize);

        TString decimal;
        buffer.AsString(decimal);

        return decimal;
    }

private:
    const NKikimr::NMiniKQL::TType* Type;
    NYT::NTableClient::TRowBufferPtr RowBuffer;
    TConvertOptions ConvertOptions;

    NYT::NTableClient::TLogicalTypePtr YtType;
    TStructPrecomputes StructPrecomputes;
};

class TRowOutputCodec final
    : public TOutputCodecBase
    , public IRowOutputCodec
{
public:
    using TOutputCodecBase::TOutputCodecBase;

    NYT::NTableClient::TUnversionedRow Convert(
        NYql::NUdf::TUnboxedValue unboxedValue
    ) override {
        return ConvertRow(std::move(unboxedValue));
    }
};

class TValueOutputCodec final
    : public TOutputCodecBase
    , public IValueOutputCodec
{
public:
    using TOutputCodecBase::TOutputCodecBase;

    NYT::NTableClient::TUnversionedValue Convert(
        NYql::NUdf::TUnboxedValue unboxedValue
    ) override {
        return ConvertValue(std::move(unboxedValue));
    }
};

} // namespace NYql::NYtflow::NCodec::NPrivate


namespace NYql::NYtflow::NCodec {

THolder<IRowOutputCodec> CreateRowOutputCodec(
    const NKikimr::NMiniKQL::TType* type,
    NYT::NTableClient::TTableSchemaPtr ytSchema,
    NYT::NTableClient::TRowBufferPtr rowBuffer,
    const TConvertOptions& convertOptions
) {
    YQL_ENSURE(
        type->IsStruct(),
        "Row output codec is only supported for struct output types");

    return MakeHolder<NPrivate::TRowOutputCodec>(
        type,
        NPrivate::TOutputCodecBase::BuildStructYtType(ytSchema),
        std::move(rowBuffer),
        convertOptions);
}

THolder<IValueOutputCodec> CreateValueOutputCodec(
    const NKikimr::NMiniKQL::TType* type,
    NYT::NTableClient::TLogicalTypePtr ytType,
    NYT::NTableClient::TRowBufferPtr rowBuffer,
    const TConvertOptions& convertOptions
) {
    return MakeHolder<NPrivate::TValueOutputCodec>(
        type, std::move(ytType), std::move(rowBuffer), convertOptions);
}

} // namespace NYql::NYtflow::NCodec

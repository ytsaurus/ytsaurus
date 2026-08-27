#include "yql_ytflow_type_helpers.h"

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/utils/yql_panic.h>

#include <yt/yt/client/table_client/logical_type.h>


namespace NYql::NYtflow::NCodec::NPrivate {

template <typename TTarget, typename TSource>
    requires std::is_pointer_v<TTarget> &&
        std::is_const_v<std::remove_pointer_t<TTarget>> &&
        std::is_base_of_v<
            TSource,
            std::remove_const_t<std::remove_pointer_t<TTarget>>>
TTarget SafeCast(const TSource* source) {
    auto* target = dynamic_cast<TTarget>(source);
    YQL_ENSURE(target);
    return target;
}

namespace {

struct TOnlySameFieldsTag {
};

struct TAllowExtraYtFieldsTag {
};

struct TAllowExtraYqlFieldsTag {
};

} // anonymous namespace

class TTypesCorrespondenceValidator {
public:
    TTypesCorrespondenceValidator(TConvertOptions convertOptions)
        : ConvertOptions(std::move(convertOptions))
    {
    }

    void Validate(
        const NKikimr::NMiniKQL::TType* type,
        const NYT::NTableClient::TLogicalType* ytType
    ) {
        if (type->IsData()) {
            auto* dataType = static_cast<const NKikimr::NMiniKQL::TDataType*>(type);

            if (*dataType->GetDataSlot() == NYql::NUdf::EDataSlot::Decimal) {
                auto* decimalType = static_cast<
                    const NKikimr::NMiniKQL::TDataDecimalType*>(dataType);

                auto* ytDecimalType = SafeCast<
                    const NYT::NTableClient::TDecimalLogicalType*>(ytType);

                Validate(decimalType, ytDecimalType);
            } else if (*dataType->GetDataSlot() == NYql::NUdf::EDataSlot::Yson) {
                // YQL & YT type systems disagree here:
                // it's possible to have non-optional Yson in YQL, but Any in YT
                // can only be optional
                if (auto* ytOptionalType = dynamic_cast<
                    const NYT::NTableClient::TOptionalLogicalType*>(ytType)
                ) {
                    auto* ytUnderlyingType = ytOptionalType->GetElement().Get();
                    auto* ytDataType = SafeCast<
                        const NYT::NTableClient::TSimpleLogicalType*>(ytUnderlyingType);

                    Validate(dataType, ytDataType);
                } else {
                    auto* ytDataType = SafeCast<
                        const NYT::NTableClient::TSimpleLogicalType*>(ytType);

                    Validate(dataType, ytDataType);
                }
            } else {
                auto* ytDataType = SafeCast<
                    const NYT::NTableClient::TSimpleLogicalType*>(ytType);

                Validate(dataType, ytDataType);
            }
        } else if (type->IsTuple()) {
            auto* tupleType = static_cast<const NKikimr::NMiniKQL::TTupleType*>(type);
            auto* ytTupleType = SafeCast<
                const NYT::NTableClient::TTupleLogicalType*>(ytType);

            Validate(tupleType, ytTupleType);
        } else if (type->IsStruct()) {
            auto* structType = static_cast<const NKikimr::NMiniKQL::TStructType*>(type);
            auto* ytStructType = SafeCast<
                const NYT::NTableClient::TStructLogicalType*>(ytType);

            Validate(structType, ytStructType, /*forceExact*/ false);
        } else if (type->IsList()) {
            auto* listType = static_cast<const NKikimr::NMiniKQL::TListType*>(type);
            auto* ytListType = SafeCast<
                const NYT::NTableClient::TListLogicalType*>(ytType);

            Validate(listType, ytListType);
        } else if (type->IsOptional()) {
            auto* optionalType = static_cast<
                const NKikimr::NMiniKQL::TOptionalType*>(type);

            auto* ytOptionalType = SafeCast<
                const NYT::NTableClient::TOptionalLogicalType*>(ytType);

            Validate(optionalType, ytOptionalType);
        } else if (type->IsDict()) {
            auto* dictType = static_cast<
                const NKikimr::NMiniKQL::TDictType*>(type);

            auto* ytDictType = SafeCast<
                const NYT::NTableClient::TDictLogicalType*>(ytType);

            Validate(dictType, ytDictType);
        } else if (type->IsVoid()) {
            auto* voidType = static_cast<
                const NKikimr::NMiniKQL::TVoidType*>(type);

            auto* ytVoidType = SafeCast<
                const NYT::NTableClient::TSimpleLogicalType*>(ytType);

            Validate(voidType, ytVoidType);
        } else if (type->IsNull()) {
            auto* nullType = static_cast<
                const NKikimr::NMiniKQL::TNullType*>(type);

            auto* ytNullType = SafeCast<
                const NYT::NTableClient::TSimpleLogicalType*>(ytType);

            Validate(nullType, ytNullType);
        } else if (type->IsTagged()) {
            auto* taggedType = static_cast<const NKikimr::NMiniKQL::TTaggedType*>(type);
            auto* ytTaggedType = SafeCast<
                const NYT::NTableClient::TTaggedLogicalType*>(ytType);

            Validate(taggedType, ytTaggedType);
        } else if (type->IsVariant()) {
            auto* variantType = static_cast<const NKikimr::NMiniKQL::TVariantType*>(type);
            auto* underlyingType = variantType->GetUnderlyingType();

            if (underlyingType->IsTuple()) {
                auto* ytVariantType = SafeCast<
                    const NYT::NTableClient::TVariantTupleLogicalType*>(ytType);

                Validate(variantType, ytVariantType);
            } else {
                auto* ytVariantType = SafeCast<
                    const NYT::NTableClient::TVariantStructLogicalType*>(ytType);

                Validate(variantType, ytVariantType);
            }
        } else {
            YQL_ENSURE(false, "Unsupported type: " << type->GetKindAsStr());
        }
    }

private:
    void Validate(
        const NKikimr::NMiniKQL::TDataType* type,
        const NYT::NTableClient::TSimpleLogicalType* ytType
    ) {
        auto ensureType = [&](auto enumValue) {
            YQL_ENSURE(ytType->GetElement() == enumValue);
        };

        switch (*type->GetDataSlot()) {
        case NYql::NUdf::EDataSlot::String:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::String);
            break;

        case NYql::NUdf::EDataSlot::Uuid:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Uuid);
            break;

        case NYql::NUdf::EDataSlot::Json:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Json);
            break;

        case NYql::NUdf::EDataSlot::Utf8:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Utf8);
            break;

        case NYql::NUdf::EDataSlot::Int8:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Int8);
            break;

        case NYql::NUdf::EDataSlot::Int16:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Int16);
            break;

        case NYql::NUdf::EDataSlot::Int32:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Int32);
            break;

        case NYql::NUdf::EDataSlot::Int64:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Int64);
            break;

        case NYql::NUdf::EDataSlot::Uint8:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Uint8);
            break;

        case NYql::NUdf::EDataSlot::Uint16:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Uint16);
            break;

        case NYql::NUdf::EDataSlot::Uint32:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Uint32);
            break;

        case NYql::NUdf::EDataSlot::Uint64:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Uint64);
            break;

        case NYql::NUdf::EDataSlot::Date:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Date);
            break;

        case NYql::NUdf::EDataSlot::Datetime:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Datetime);
            break;

        case NYql::NUdf::EDataSlot::Timestamp:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Timestamp);
            break;

        case NYql::NUdf::EDataSlot::Interval:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Interval);
            break;

        case NYql::NUdf::EDataSlot::Date32:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Date32);
            break;

        case NYql::NUdf::EDataSlot::Datetime64:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Datetime64);
            break;

        case NYql::NUdf::EDataSlot::Timestamp64:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Timestamp64);
            break;

        case NYql::NUdf::EDataSlot::Interval64:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Interval64);
            break;

        case NYql::NUdf::EDataSlot::Float:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Float);
            break;

        case NYql::NUdf::EDataSlot::Double:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Double);
            break;

        case NYql::NUdf::EDataSlot::Bool:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Boolean);
            break;

        case NYql::NUdf::EDataSlot::Yson:
            ensureType(NYT::NTableClient::ESimpleLogicalValueType::Any);
            break;

        default:
            YQL_ENSURE(false, "Unsupported type: " << *type->GetDataSlot());
        }
    }

    void Validate(
        const NKikimr::NMiniKQL::TDataDecimalType* type,
        const NYT::NTableClient::TDecimalLogicalType* ytType
    ) {
        auto [precision, scale] = type->GetParams();

        YQL_ENSURE(ytType->GetPrecision() == precision);
        YQL_ENSURE(ytType->GetScale() == scale);
    }

    void Validate(
        const NKikimr::NMiniKQL::TTupleType* type,
        const NYT::NTableClient::TTupleLogicalTypeBase* ytType
    ) {
        YQL_ENSURE(type->GetElementsCount() == ytType->GetElements().size());

        for (size_t index = 0; index < type->GetElementsCount(); ++index) {
            auto* itemType = type->GetElementType(index);
            auto* ytItemType = ytType->GetElements()[index].Get();
            Validate(itemType, ytItemType);
        }
    }

    void Validate(
        const NKikimr::NMiniKQL::TStructType* type,
        const NYT::NTableClient::TStructLogicalTypeBase* ytType,
        bool forceExact
    ) {
        bool allowExtraYtFields = ConvertOptions.GetAllowExtraYtFields();
        bool allowExtraYqlFields = ConvertOptions.GetAllowExtraYqlFields();

        if (!allowExtraYtFields && !allowExtraYqlFields || forceExact) {
            Validate(type, ytType, TOnlySameFieldsTag{});
        } else if (allowExtraYtFields) {
            Validate(type, ytType, TAllowExtraYtFieldsTag{});
        } else {
            Validate(type, ytType, TAllowExtraYqlFieldsTag{});
        }
    }

    void Validate(
        const NKikimr::NMiniKQL::TStructType* type,
        const NYT::NTableClient::TStructLogicalTypeBase* ytType,
        TOnlySameFieldsTag
    ) {
        YQL_ENSURE(type->GetMembersCount() == ytType->GetFields().size());

        for (const auto& ytField: ytType->GetFields()) {
            auto memberIndex = type->GetMemberIndex(ytField.Name);
            auto* memberType = type->GetMemberType(memberIndex);
            auto* ytMemberType = ytField.Type.Get();

            Validate(memberType, ytMemberType);
        }
    }

    void Validate(
        const NKikimr::NMiniKQL::TStructType* type,
        const NYT::NTableClient::TStructLogicalTypeBase* ytType,
        TAllowExtraYtFieldsTag
    ) {
        YQL_ENSURE(type->GetMembersCount() <= ytType->GetFields().size());

        for (const auto& ytField: ytType->GetFields()) {
            auto maybeMemberIndex = type->FindMemberIndex(ytField.Name);
            auto* ytMemberType = ytField.Type.Get();

            if (!maybeMemberIndex) {
                if (ConvertOptions.GetConvertDirection() == EConvertDirection::YqlToYt) {
                    YQL_ENSURE(ytMemberType->IsNullable());
                }

                continue;
            }

            auto* memberType = type->GetMemberType(*maybeMemberIndex);

            Validate(memberType, ytMemberType);
        }
    }

    void Validate(
        const NKikimr::NMiniKQL::TStructType* type,
        const NYT::NTableClient::TStructLogicalTypeBase* ytType,
        TAllowExtraYqlFieldsTag
    ) {
        YQL_ENSURE(type->GetMembersCount() >= ytType->GetFields().size());

        const auto& ytFields = ytType->GetFields();
        THashMap<TStringBuf, ui32> ytFieldIndices;

        for (
            ui32 ytMemberIndex = 0;
            ytMemberIndex < ytFields.size();
            ++ytMemberIndex
        ) {
            ytFieldIndices.emplace(ytFields[ytMemberIndex].Name, ytMemberIndex);
        }

        for (
            ui32 memberIndex = 0;
            memberIndex < type->GetMembersCount();
            ++memberIndex
        ) {
            auto memberName = type->GetMemberName(memberIndex);
            auto* memberType = type->GetMemberType(memberIndex);

            auto ytMemberIndexIterator = ytFieldIndices.find(memberName);
            if (ytMemberIndexIterator == ytFieldIndices.end()) {
                if (ConvertOptions.GetConvertDirection() == EConvertDirection::YtToYql) {
                    YQL_ENSURE(memberType->IsOptional());
                }

                continue;
            }

            auto* ytMemberType = ytFields[ytMemberIndexIterator->second].Type.Get();

            Validate(memberType, ytMemberType);
        }
    }

    void Validate(
        const NKikimr::NMiniKQL::TListType* type,
        const NYT::NTableClient::TListLogicalType* ytType
    ) {
        auto* itemType = type->GetItemType();
        auto* ytItemType = ytType->GetElement().Get();
        Validate(itemType, ytItemType);
    }

    void Validate(
        const NKikimr::NMiniKQL::TOptionalType* type,
        const NYT::NTableClient::TOptionalLogicalType* ytType
    ) {
        auto* itemType = type->GetItemType();
        auto* ytItemType = ytType->GetElement().Get();
        Validate(itemType, ytItemType);
    }

    void Validate(
        const NKikimr::NMiniKQL::TDictType* type,
        const NYT::NTableClient::TDictLogicalType* ytType
    ) {
        auto* keyType = type->GetKeyType();
        auto* payloadType = type->GetPayloadType();

        auto* ytKeyType = ytType->GetKey().Get();
        auto* ytPayloadType = ytType->GetValue().Get();

        Validate(keyType, ytKeyType);
        Validate(payloadType, ytPayloadType);
    }

    void Validate(
        const NKikimr::NMiniKQL::TVoidType* /*type*/,
        const NYT::NTableClient::TSimpleLogicalType* ytType
    ) {
        YQL_ENSURE(
            ytType->GetElement() == NYT::NTableClient::ESimpleLogicalValueType::Void);
    }

    void Validate(
        const NKikimr::NMiniKQL::TNullType* /*type*/,
        const NYT::NTableClient::TSimpleLogicalType* ytType
    ) {
        YQL_ENSURE(
            ytType->GetElement() == NYT::NTableClient::ESimpleLogicalValueType::Null);
    }

    void Validate(
        const NKikimr::NMiniKQL::TTaggedType* type,
        const NYT::NTableClient::TTaggedLogicalType* ytType
    ) {
        YQL_ENSURE(type->GetTag() == ytType->GetTag());

        auto* baseType = type->GetBaseType();
        auto* ytBaseType = ytType->GetElement().Get();
        Validate(baseType, ytBaseType);
    }

    void Validate(
        const NKikimr::NMiniKQL::TVariantType* type,
        const NYT::NTableClient::TVariantTupleLogicalType* ytType
    ) {
        auto* underlyingType = static_cast<
            const NKikimr::NMiniKQL::TTupleType*>(type->GetUnderlyingType());

        auto* ytUnderlyingType = static_cast<
            const NYT::NTableClient::TTupleLogicalTypeBase*>(ytType);

        Validate(underlyingType, ytUnderlyingType);
    }

    void Validate(
        const NKikimr::NMiniKQL::TVariantType* type,
        const NYT::NTableClient::TVariantStructLogicalType* ytType
    ) {
        auto* underlyingType = static_cast<
            const NKikimr::NMiniKQL::TStructType*>(type->GetUnderlyingType());

        auto* ytUnderlyingType = static_cast<
            const NYT::NTableClient::TStructLogicalTypeBase*>(ytType);

        Validate(underlyingType, ytUnderlyingType, /*forceExact*/ true);
    }

private:
    TConvertOptions ConvertOptions;
};

void ValidateTypesCorrespondence(
    const NKikimr::NMiniKQL::TType* type,
    const NYT::NTableClient::TLogicalType* ytType,
    const TConvertOptions& convertOptions
) {
    TTypesCorrespondenceValidator(convertOptions).Validate(type, ytType);
}

} // namespace NYql::NYtflow::NCodec::NPrivate

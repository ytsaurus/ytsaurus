#include "yql_ytflow_type_helpers.h"

#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_type_builder.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/utils/yql_panic.h>

#include <yt/yt/client/table_client/logical_type.h>


namespace NYql::NYtflow::NCodec::NPrivate {

struct TDictUdfTypesBuilder {
public:
    NYql::NUdf::IFunctionTypeInfoBuilder& FunctionTypeInfoBuilder;
    THashMap<const NKikimr::NMiniKQL::TType*, const NYql::NUdf::TType*> DictUdfTypes;

    TDictUdfTypesBuilder(NYql::NUdf::IFunctionTypeInfoBuilder& functionTypeInfoBuilder)
        : FunctionTypeInfoBuilder(functionTypeInfoBuilder)
    {
    }

public:
    const NYql::NUdf::TType* BuildUdfType(const NKikimr::NMiniKQL::TType* type) {
        const NYql::NUdf::TType* udfType = nullptr;

        if (type->IsData()) {
            auto* dataType = static_cast<const NKikimr::NMiniKQL::TDataType*>(type);

            if (*dataType->GetDataSlot() == NYql::NUdf::EDataSlot::Decimal) {
                auto* decimalType = static_cast<
                    const NKikimr::NMiniKQL::TDataDecimalType*>(dataType);

                udfType = BuildUdfType(decimalType);
            } else {
                udfType = BuildUdfType(dataType);
            }
        } else if (type->IsTuple()) {
            auto* tupleType = static_cast<const NKikimr::NMiniKQL::TTupleType*>(type);
            udfType = BuildUdfType(tupleType);
        } else if (type->IsStruct()) {
            auto* structType = static_cast<const NKikimr::NMiniKQL::TStructType*>(type);
            udfType = BuildUdfType(structType);
        } else if (type->IsList()) {
            auto* listType = static_cast<const NKikimr::NMiniKQL::TListType*>(type);
            udfType = BuildUdfType(listType);
        } else if (type->IsOptional()) {
            auto* optionalType = static_cast<
                const NKikimr::NMiniKQL::TOptionalType*>(type);

            udfType = BuildUdfType(optionalType);
        } else if (type->IsDict()) {
            auto* dictType = static_cast<
                const NKikimr::NMiniKQL::TDictType*>(type);

            udfType = BuildUdfType(dictType);
        } else if (type->IsVoid()) {
            auto* voidType = static_cast<
                const NKikimr::NMiniKQL::TVoidType*>(type);

            udfType = BuildUdfType(voidType);
        } else if (type->IsNull()) {
            auto* nullType = static_cast<
                const NKikimr::NMiniKQL::TNullType*>(type);

            udfType = BuildUdfType(nullType);
        } else if (type->IsTagged()) {
            auto* taggedType = static_cast<const NKikimr::NMiniKQL::TTaggedType*>(type);
            udfType = BuildUdfType(taggedType);
        } else if (type->IsVariant()) {
            auto* variantType = static_cast<const NKikimr::NMiniKQL::TVariantType*>(type);
            udfType = BuildUdfType(variantType);
        } else {
            YQL_ENSURE(false, "Unsupported type: " << type->GetKindAsStr());
        }

        return udfType;
    }

    const NYql::NUdf::TType* BuildUdfType(const NKikimr::NMiniKQL::TDataType* type) {
        NYql::NUdf::TDataTypeId dataTypeId;

        switch (*type->GetDataSlot()) {
        case NYql::NUdf::EDataSlot::String:
            dataTypeId = NYql::NUdf::TDataType<char*>::Id;
            break;

        case NYql::NUdf::EDataSlot::Uuid:
            dataTypeId = NYql::NUdf::TDataType<NYql::NUdf::TUuid>::Id;
            break;

        case NYql::NUdf::EDataSlot::Json:
            dataTypeId = NYql::NUdf::TDataType<NYql::NUdf::TJson>::Id;
            break;

        case NYql::NUdf::EDataSlot::Utf8:
            dataTypeId = NYql::NUdf::TDataType<NYql::NUdf::TUtf8>::Id;
            break;

        case NYql::NUdf::EDataSlot::Int8:
            dataTypeId = NYql::NUdf::TDataType<i8>::Id;
            break;

        case NYql::NUdf::EDataSlot::Int16:
            dataTypeId = NYql::NUdf::TDataType<i16>::Id;
            break;

        case NYql::NUdf::EDataSlot::Int32:
            dataTypeId = NYql::NUdf::TDataType<i32>::Id;
            break;

        case NYql::NUdf::EDataSlot::Int64:
            dataTypeId = NYql::NUdf::TDataType<i64>::Id;
            break;

        case NYql::NUdf::EDataSlot::Uint8:
            dataTypeId = NYql::NUdf::TDataType<ui8>::Id;
            break;

        case NYql::NUdf::EDataSlot::Uint16:
            dataTypeId = NYql::NUdf::TDataType<ui16>::Id;
            break;

        case NYql::NUdf::EDataSlot::Uint32:
            dataTypeId = NYql::NUdf::TDataType<ui32>::Id;
            break;

        case NYql::NUdf::EDataSlot::Uint64:
            dataTypeId = NYql::NUdf::TDataType<ui64>::Id;
            break;

        case NYql::NUdf::EDataSlot::Date:
            dataTypeId = NYql::NUdf::TDataType<NYql::NUdf::TDate>::Id;
            break;

        case NYql::NUdf::EDataSlot::Datetime:
            dataTypeId = NYql::NUdf::TDataType<NYql::NUdf::TDatetime>::Id;
            break;

        case NYql::NUdf::EDataSlot::Timestamp:
            dataTypeId = NYql::NUdf::TDataType<NYql::NUdf::TTimestamp>::Id;
            break;

        case NYql::NUdf::EDataSlot::Interval:
            dataTypeId = NYql::NUdf::TDataType<NYql::NUdf::TInterval>::Id;
            break;

        case NYql::NUdf::EDataSlot::Date32:
            dataTypeId = NYql::NUdf::TDataType<NYql::NUdf::TDate32>::Id;
            break;

        case NYql::NUdf::EDataSlot::Datetime64:
            dataTypeId = NYql::NUdf::TDataType<NYql::NUdf::TDatetime64>::Id;
            break;

        case NYql::NUdf::EDataSlot::Timestamp64:
            dataTypeId = NYql::NUdf::TDataType<NYql::NUdf::TTimestamp64>::Id;
            break;

        case NYql::NUdf::EDataSlot::Interval64:
            dataTypeId = NYql::NUdf::TDataType<NYql::NUdf::TInterval64>::Id;
            break;

        case NYql::NUdf::EDataSlot::Float:
            dataTypeId = NYql::NUdf::TDataType<float>::Id;
            break;

        case NYql::NUdf::EDataSlot::Double:
            dataTypeId = NYql::NUdf::TDataType<double>::Id;
            break;

        case NYql::NUdf::EDataSlot::Bool:
            dataTypeId = NYql::NUdf::TDataType<bool>::Id;
            break;

        case NYql::NUdf::EDataSlot::Yson:
            dataTypeId = NYql::NUdf::TDataType<NYql::NUdf::TYson>::Id;
            break;

        default:
            YQL_ENSURE(false, "Unsupported type: " << *type->GetDataSlot());
        }

        return FunctionTypeInfoBuilder.Primitive(dataTypeId);
    }

    const NYql::NUdf::TType* BuildUdfType(
        const NKikimr::NMiniKQL::TDataDecimalType* type
    ) {
        auto [precision, scale] = type->GetParams();
        return FunctionTypeInfoBuilder.Decimal(precision, scale);
    }

    const NYql::NUdf::TType* BuildUdfType(
        const NKikimr::NMiniKQL::TTupleType* type
    ) {
        auto tupleTypeBuilder = FunctionTypeInfoBuilder.Tuple(type->GetElementsCount());

        for (size_t index = 0; index < type->GetElementsCount(); ++index) {
            tupleTypeBuilder->Add(BuildUdfType(type->GetElementType(index)));
        }

        return tupleTypeBuilder->Build();
    }

    const NYql::NUdf::TType* BuildUdfType(
        const NKikimr::NMiniKQL::TStructType* type
    ) {
        auto structTypeBuilder = FunctionTypeInfoBuilder.Struct(type->GetMembersCount());

        for (size_t index = 0; index < type->GetMembersCount(); ++index) {
            auto memberType = type->GetMemberType(index);
            auto memberName = type->GetMemberName(index);

            structTypeBuilder->AddField(memberName, BuildUdfType(memberType), nullptr);
        }

        return structTypeBuilder->Build();
    }

    const NYql::NUdf::TType* BuildUdfType(
        const NKikimr::NMiniKQL::TListType* type
    ) {
        return FunctionTypeInfoBuilder
            .List()
                ->Item(BuildUdfType(type->GetItemType()))
                .Build();
    }

    const NYql::NUdf::TType* BuildUdfType(
        const NKikimr::NMiniKQL::TOptionalType* type
    ) {
        return FunctionTypeInfoBuilder
            .Optional()
                ->Item(BuildUdfType(type->GetItemType()))
                .Build();
    }

    const NYql::NUdf::TType* BuildUdfType(
        const NKikimr::NMiniKQL::TDictType* type
    ) {
        auto* udfType = FunctionTypeInfoBuilder
            .Dict()
                ->Key(BuildUdfType(type->GetKeyType()))
                .Value(BuildUdfType(type->GetPayloadType()))
                .Build();

        DictUdfTypes[type] = udfType;

        return udfType;
    }

    const NYql::NUdf::TType* BuildUdfType(
        const NKikimr::NMiniKQL::TVoidType* /*type*/
    ) {
        return FunctionTypeInfoBuilder.Void();
    }

    const NYql::NUdf::TType* BuildUdfType(
        const NKikimr::NMiniKQL::TNullType* /*type*/
    ) {
        return FunctionTypeInfoBuilder.Null();
    }

    const NYql::NUdf::TType* BuildUdfType(
        const NKikimr::NMiniKQL::TTaggedType* type
    ) {
        return FunctionTypeInfoBuilder
            .Tagged(BuildUdfType(type->GetBaseType()), type->GetTag());
    }

    const NYql::NUdf::TType* BuildUdfType(
        const NKikimr::NMiniKQL::TVariantType* type
    ) {
        return FunctionTypeInfoBuilder
            .Variant()
                ->Over(BuildUdfType(type->GetUnderlyingType()))
                .Build();
    }
};

THashMap<const NKikimr::NMiniKQL::TType*, const NYql::NUdf::TType*> BuildDictUdfTypes(
    const NKikimr::NMiniKQL::TType* type,
    NYql::NUdf::IFunctionTypeInfoBuilder& functionTypeInfoBuilder
) {
    TDictUdfTypesBuilder builder(functionTypeInfoBuilder);
    builder.BuildUdfType(type);

    return std::move(builder.DictUdfTypes);
}

} // namespace NYql::NYtflow::NCodec::NPrivate

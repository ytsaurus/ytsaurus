#include "yql_ytflow_type_helpers.h"

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/utils/yql_panic.h>

#include <yt/yt/client/table_client/logical_type.h>


namespace NYql::NYtflow::NCodec::NPrivate {

namespace {

struct TOnlySameFieldsTag {
};

struct TAllowExtraYtFieldsTag {
};

struct TAllowExtraYqlFieldsTag {
};

} // anonymous namespace

class TStructPrecomputesBuilder {
public:
    TStructPrecomputesBuilder(TConvertOptions convertOptions)
        : ConvertOptions(std::move(convertOptions))
    {
    }

public:
    TStructPrecomputes&& ExtractStructPrecomputes() && {
        return std::move(StructPrecomputes);
    }

    void Visit(
        const NKikimr::NMiniKQL::TType* type,
        const NYT::NTableClient::TLogicalType* ytType
    ) {
        if (type->IsData()) {
            /* pass */
        } else if (type->IsTuple()) {
            auto* tupleType = static_cast<const NKikimr::NMiniKQL::TTupleType*>(type);
            auto* ytTupleType = static_cast<
                const NYT::NTableClient::TTupleLogicalType*>(ytType);

            Visit(tupleType, ytTupleType);
        } else if (type->IsStruct()) {
            auto* structType = static_cast<const NKikimr::NMiniKQL::TStructType*>(type);
            auto* ytStructType = static_cast<
                const NYT::NTableClient::TStructLogicalType*>(ytType);

            Visit(structType, ytStructType);
        } else if (type->IsList()) {
            auto* listType = static_cast<const NKikimr::NMiniKQL::TListType*>(type);
            auto* ytListType = static_cast<
                const NYT::NTableClient::TListLogicalType*>(ytType);

            Visit(listType, ytListType);
        } else if (type->IsOptional()) {
            auto* optionalType = static_cast<
                const NKikimr::NMiniKQL::TOptionalType*>(type);

            auto* ytOptionalType = static_cast<
                const NYT::NTableClient::TOptionalLogicalType*>(ytType);

            Visit(optionalType, ytOptionalType);
        } else if (type->IsDict()) {
            auto* dictType = static_cast<const NKikimr::NMiniKQL::TDictType*>(type);
            auto* ytDictType = static_cast<
                const NYT::NTableClient::TDictLogicalType*>(ytType);

            Visit(dictType, ytDictType);
        } else if (type->IsVoid()) {
            /* pass */
        } else if (type->IsNull()) {
            /* pass */
        } else if (type->IsTagged()) {
            auto* taggedType = static_cast<const NKikimr::NMiniKQL::TTaggedType*>(type);
            auto* ytTaggedType = static_cast<
                const NYT::NTableClient::TTaggedLogicalType*>(ytType);

            Visit(taggedType, ytTaggedType);
        } else if (type->IsVariant()) {
            auto* variantType = static_cast<
                const NKikimr::NMiniKQL::TVariantType*>(type);

            auto* underlyingType = variantType->GetUnderlyingType();

            if (underlyingType->IsTuple()) {
                auto* ytVariantType = static_cast<
                    const NYT::NTableClient::TVariantTupleLogicalType*>(ytType);

                Visit(variantType, ytVariantType);
            } else {
                auto* ytVariantType = static_cast<
                    const NYT::NTableClient::TVariantStructLogicalType*>(ytType);

                Visit(variantType, ytVariantType);
            }
        } else {
            YQL_ENSURE(false, "Unsupported type: " << type->GetKindAsStr());
        }
    }

private:
    void Visit(
        const NKikimr::NMiniKQL::TTupleType* type,
        const NYT::NTableClient::TTupleLogicalTypeBase* ytType
    ) {
        for (size_t index = 0; index < type->GetElementsCount(); ++index) {
            auto* itemType = type->GetElementType(index);
            auto* ytItemType = ytType->GetElements()[index].Get();
            Visit(itemType, ytItemType);
        }
    }

    void Visit(
        const NKikimr::NMiniKQL::TStructType* type,
        const NYT::NTableClient::TStructLogicalTypeBase* ytType
    ) {
        auto allowExtraYtFields = ConvertOptions.GetAllowExtraYtFields();
        auto allowExtraYqlFields = ConvertOptions.GetAllowExtraYqlFields();

        if (!allowExtraYtFields && !allowExtraYqlFields) {
            Visit(type, ytType, TOnlySameFieldsTag{});
        } else if (allowExtraYtFields) {
            Visit(type, ytType, TAllowExtraYtFieldsTag{});
        } else {
            Visit(type, ytType, TAllowExtraYqlFieldsTag{});
        }
    }

    void Visit(
        const NKikimr::NMiniKQL::TStructType* type,
        const NYT::NTableClient::TStructLogicalTypeBase* ytType,
        TOnlySameFieldsTag
    ) {
        const auto& ytFields = ytType->GetFields();

        for (
            ui32 ytMemberIndex = 0;
            ytMemberIndex < ytFields.size();
            ++ytMemberIndex
        ) {
            const auto& ytField = ytFields[ytMemberIndex];

            auto memberIndex = type->GetMemberIndex(ytField.Name);
            auto* memberType = type->GetMemberType(memberIndex);
            auto* ytMemberType = ytField.Type.Get();

            auto memberDescriptor = TMemberDescriptor{
                .Name = ytField.Name,
                .Type = memberType,
                .Index = memberIndex,
                .YtType = ytMemberType,
                .YtIndex = ytMemberIndex,
            };

            {
                auto key = std::pair(type, memberIndex);
                StructPrecomputes.MemberDescriptors.emplace(
                    std::move(key), memberDescriptor);
            }

            {
                auto key = std::pair(ytType, ytMemberIndex);
                StructPrecomputes.YtMemberDescriptors.emplace(
                    std::move(key), std::move(memberDescriptor));
            }

            Visit(memberType, ytMemberType);
        }
    }

    void Visit(
        const NKikimr::NMiniKQL::TStructType* type,
        const NYT::NTableClient::TStructLogicalTypeBase* ytType,
        TAllowExtraYtFieldsTag
    ) {
        const auto& ytFields = ytType->GetFields();

        for (
            ui32 ytMemberIndex = 0;
            ytMemberIndex < ytFields.size();
            ++ytMemberIndex
        ) {
            const auto& ytField = ytFields[ytMemberIndex];
            const NKikimr::NMiniKQL::TType* memberType = nullptr;
            auto* ytMemberType = ytField.Type.Get();

            auto maybeMemberIndex = type->FindMemberIndex(ytField.Name);
            if (maybeMemberIndex) {
                memberType = type->GetMemberType(*maybeMemberIndex);
            }

            auto memberDescriptor = TMemberDescriptor{
                .Name = ytField.Name,
                .Type = memberType,
                .Index = maybeMemberIndex,
                .YtType = ytMemberType,
                .YtIndex = ytMemberIndex,
            };

            {
                auto key = std::pair(ytType, ytMemberIndex);
                StructPrecomputes.YtMemberDescriptors.emplace(
                    std::move(key), memberDescriptor);
            }

            if (maybeMemberIndex) {
                auto key = std::pair(type, *maybeMemberIndex);
                StructPrecomputes.MemberDescriptors.emplace(
                    std::move(key), std::move(memberDescriptor));

                Visit(memberType, ytMemberType);
            }
        }
    }

    void Visit(
        const NKikimr::NMiniKQL::TStructType* type,
        const NYT::NTableClient::TStructLogicalTypeBase* ytType,
        TAllowExtraYqlFieldsTag
    ) {
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
            const NYT::NTableClient::TLogicalType* ytMemberType = nullptr;

            TMaybe<ui32> maybeYtMemberIndex;

            if (
                auto ytMemberIndexIterator = ytFieldIndices.find(memberName);
                ytMemberIndexIterator != ytFieldIndices.end()
            ) {
                maybeYtMemberIndex = ytMemberIndexIterator->second;
                ytMemberType = ytFields[*maybeYtMemberIndex].Type.Get();
            }

            auto memberDescriptor = TMemberDescriptor{
                .Name = memberName,
                .Type = memberType,
                .Index = memberIndex,
                .YtType = ytMemberType,
                .YtIndex = maybeYtMemberIndex,
            };

            {
                auto key = std::pair(type, memberIndex);
                StructPrecomputes.MemberDescriptors.emplace(
                    std::move(key), memberDescriptor);
            }

            if (maybeYtMemberIndex) {
                auto key = std::pair(ytType, *maybeYtMemberIndex);
                StructPrecomputes.YtMemberDescriptors.emplace(
                    std::move(key), std::move(memberDescriptor));

                Visit(memberType, ytMemberType);
            } else {
                StructPrecomputes.ExtraMembers[type].push_back(memberIndex);
            }
        }
    }

    void Visit(
        const NKikimr::NMiniKQL::TListType* type,
        const NYT::NTableClient::TListLogicalType* ytType
    ) {
        auto* itemType = type->GetItemType();
        auto* ytItemType = ytType->GetElement().Get();
        Visit(itemType, ytItemType);
    }

    void Visit(
        const NKikimr::NMiniKQL::TOptionalType* type,
        const NYT::NTableClient::TOptionalLogicalType* ytType
    ) {
        auto* itemType = type->GetItemType();
        auto* ytItemType = ytType->GetElement().Get();
        Visit(itemType, ytItemType);
    }

    void Visit(
        const NKikimr::NMiniKQL::TDictType* type,
        const NYT::NTableClient::TDictLogicalType* ytType
    ) {
        auto* keyType = type->GetKeyType();
        auto* payloadType = type->GetPayloadType();

        auto* ytKeyType = ytType->GetKey().Get();
        auto* ytPayloadType = ytType->GetValue().Get();

        Visit(keyType, ytKeyType);
        Visit(payloadType, ytPayloadType);
    }

    void Visit(
        const NKikimr::NMiniKQL::TTaggedType* type,
        const NYT::NTableClient::TTaggedLogicalType* ytType
    ) {
        auto* baseType = type->GetBaseType();
        auto* ytBaseType = ytType->GetElement().Get();
        Visit(baseType, ytBaseType);
    }

    void Visit(
        const NKikimr::NMiniKQL::TVariantType* type,
        const NYT::NTableClient::TVariantTupleLogicalType* ytType
    ) {
        auto* underlyingType = static_cast<const NKikimr::NMiniKQL::TTupleType*>(
            type->GetUnderlyingType());

        auto* ytUnderlyingType = static_cast<
            const NYT::NTableClient::TTupleLogicalTypeBase*>(ytType);

        Visit(underlyingType, ytUnderlyingType);
    }

    void Visit(
        const NKikimr::NMiniKQL::TVariantType* type,
        const NYT::NTableClient::TVariantStructLogicalType* ytType
    ) {
        auto* underlyingType = static_cast<
            const NKikimr::NMiniKQL::TStructType*>(type->GetUnderlyingType());

        auto* ytUnderlyingType = static_cast<
            const NYT::NTableClient::TStructLogicalTypeBase*>(ytType);

        Visit(underlyingType, ytUnderlyingType);
    }

private:
    TConvertOptions ConvertOptions;

    TStructPrecomputes StructPrecomputes;
};

TStructPrecomputes BuildStructPrecomputes(
    const NKikimr::NMiniKQL::TType* type,
    const NYT::NTableClient::TLogicalType* ytType,
    const TConvertOptions& convertOptions
) {
    TStructPrecomputesBuilder builder(convertOptions);
    builder.Visit(type, ytType);

    return std::move(builder).ExtractStructPrecomputes();
}

} // namespace NYql::NYtflow::NCodec::NPrivate

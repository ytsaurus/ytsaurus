#include "schema_match.h"

#include <yt/core/ytree/node.h>
#include <yt/core/ytree/serialize.h>
#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NSkiff {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const TString KeySwitchColumnName = "$key_switch";
const TString OtherColumnsName = "$other_columns";
const TString SparseColumnsName = "$sparse_columns";
static const TSkiffSchemaPtr OptionalInt64 = CreateVariant8Schema({
    CreateSimpleTypeSchema(EWireType::Nothing),
    CreateSimpleTypeSchema(EWireType::Int64),
});

////////////////////////////////////////////////////////////////////////////////

static bool IsOptionalInt64(TSkiffSchemaPtr skiffSchema);
static bool IsSkiffSpecialColumn(TStringBuf columnName);
static TSkiffTableDescription CreateTableDescription(const NSkiff::TSkiffSchemaPtr& skiffSchema);

////////////////////////////////////////////////////////////////////////////////

static void ThrowInvalidSkiffTypeError(const TString& columnName, TSkiffSchemaPtr expectedType, TSkiffSchemaPtr actualType)
{
    THROW_ERROR_EXCEPTION("Column %Qv has unexpected Skiff type: expected %Qv, found type %Qv",
        columnName,
        GetShortDebugString(expectedType),
        GetShortDebugString(actualType));
}

static bool IsOptionalInt64(TSkiffSchemaPtr skiffSchema)
{
    if (skiffSchema->GetWireType() != EWireType::Variant8) {
        return false;
    }
    auto children = skiffSchema->GetChildren();
    if (children.size() != 2) {
        return false;
    }
    if (children[0]->GetWireType() != EWireType::Nothing ||
        children[1]->GetWireType() != EWireType::Int64)
    {
        return false;
    }
    return true;
}

static bool IsSkiffSpecialColumn(
    TStringBuf columnName,
    const TString& rangeIndexColumnName,
    const TString& rowIndexColumnName)
{
    static const THashSet<TString> specialColumns = {
        KeySwitchColumnName,
        OtherColumnsName,
        SparseColumnsName
    };
    return specialColumns.has(columnName) || columnName == rangeIndexColumnName || columnName == rowIndexColumnName;
}

static std::pair<TSkiffSchemaPtr, bool> DeoptionalizeSchema(TSkiffSchemaPtr skiffSchema)
{
    if (skiffSchema->GetWireType() != EWireType::Variant8) {
        return std::make_pair(skiffSchema, true);
    }
    auto children = skiffSchema->GetChildren();
    if (children.size() != 2) {
        return std::make_pair(skiffSchema, true);
    }
    if (children[0]->GetWireType() == EWireType::Nothing) {
        return std::make_pair(children[1], false);
    } else {
        return std::make_pair(skiffSchema, true);
    }
}

static void ValidateColumnSchema(const TString& columnName, TSkiffSchemaPtr skiffSchema)
{
    switch (skiffSchema->GetWireType()) {
        case EWireType::Yson32:
        case EWireType::Int64:
        case EWireType::Uint64:
        case EWireType::Double:
        case EWireType::Boolean:
        case EWireType::String32:
            break;

        case EWireType::Nothing:
        case EWireType::Tuple:
        case EWireType::Variant8:
        case EWireType::RepeatedVariant16:
            THROW_ERROR_EXCEPTION("Column %Qv cannot be encoded as %Qlv",
                columnName,
                skiffSchema->GetWireType());
        default:
            Y_UNREACHABLE();
    }
}

static TSkiffTableDescription CreateTableDescription(
    const NSkiff::TSkiffSchemaPtr& skiffSchema,
    const TString& rangeIndexColumnName,
    const TString& rowIndexColumnName)
{
    TSkiffTableDescription result;
    THashSet<TString> topLevelNames;;
    TSkiffSchemaPtr otherColumnsField;
    TSkiffSchemaPtr sparseColumnsField;

    if (skiffSchema->GetWireType() != EWireType::Tuple) {
        THROW_ERROR_EXCEPTION("Invalid wire type for table row: expected %Qlv, found %Qlv",
            EWireType::Tuple,
            skiffSchema->GetWireType());
    }

    auto children = skiffSchema->GetChildren();

    YCHECK(!children.empty());

    if (children.back()->GetName() == OtherColumnsName) {
        otherColumnsField = children.back();
        result.HasOtherColumns = true;
        children.pop_back();

        if (otherColumnsField->GetWireType() != EWireType::Yson32) {
            THROW_ERROR_EXCEPTION("Invalid wire type for column %Qv: expected %Qlv, found %Qlv",
                OtherColumnsName,
                EWireType::Yson32,
                otherColumnsField->GetWireType());
        }
    }

    if (!children.empty() && children.back()->GetName() == SparseColumnsName) {
        sparseColumnsField = children.back();
        children.pop_back();
        if (sparseColumnsField->GetWireType() != EWireType::RepeatedVariant16) {
            THROW_ERROR_EXCEPTION("Invalid wire type for column %Qv: expected %Qlv, found %Qlv",
                SparseColumnsName,
                EWireType::RepeatedVariant16,
                sparseColumnsField->GetWireType());
        }
    }

    // Dense fields.
    for (size_t i = 0; i != children.size(); ++i) {
        const auto& child = children[i];
        const auto& childName = child->GetName();
        if (childName.empty()) {
            THROW_ERROR_EXCEPTION("Element #%v of row Skiff schema must have a name",
                i);
        } else if (childName == OtherColumnsName || childName == SparseColumnsName) {
            THROW_ERROR_EXCEPTION("Invalid placement of special column %Qv",
                childName);
        }
        auto res = topLevelNames.emplace(childName);
        if (!res.second) {
            THROW_ERROR_EXCEPTION("Name %Qv is found multiple times",
                childName);
        }
        if (childName == KeySwitchColumnName) {
            if (child->GetWireType() != EWireType::Boolean) {
                ThrowInvalidSkiffTypeError(
                    childName,
                    CreateSimpleTypeSchema(EWireType::Boolean),
                    child);
            }
            result.KeySwitchFieldIndex = i;
        } else if (childName == rowIndexColumnName) {
            if (!IsOptionalInt64(child)) {
                ThrowInvalidSkiffTypeError(
                    childName,
                    OptionalInt64,
                    child);
            }
            result.RowIndexFieldIndex = i;
        } else if (childName == rangeIndexColumnName) {
            if (!IsOptionalInt64(child)) {
                ThrowInvalidSkiffTypeError(
                    childName,
                    OptionalInt64,
                    child);
            }
            result.RangeIndexFieldIndex = i;
        }
        bool isRequired;
        TSkiffSchemaPtr deoptionalizedSchema;
        std::tie(deoptionalizedSchema, isRequired) = DeoptionalizeSchema(child);
        ValidateColumnSchema(childName, deoptionalizedSchema);
        result.DenseFieldDescriptionList.emplace_back(childName, deoptionalizedSchema, isRequired);
    }

    // Sparse fields.
    if (sparseColumnsField) {
        for (const auto& child : sparseColumnsField->GetChildren()) {
            const auto& name = child->GetName();
            if (name.empty()) {
                THROW_ERROR_EXCEPTION("Children of %Qv must have nonempty name",
                    SparseColumnsName);
            }
            if (IsSkiffSpecialColumn(name, rangeIndexColumnName, rowIndexColumnName)) {
                THROW_ERROR_EXCEPTION("Skiff special column %Qv cannot be a child of %Qv",
                    name,
                    SparseColumnsName);
            }
            auto res = topLevelNames.emplace(name);
            if (!res.second) {
                THROW_ERROR_EXCEPTION("Name %Qv is found multiple times",
                    name);
            }
            auto deoptionalizedSchema = DeoptionalizeSchema(child).first;
            ValidateColumnSchema(name, child);
            result.SparseFieldDescriptionList.emplace_back(name, deoptionalizedSchema);
        }
    }

    return result;
}

std::vector<TSkiffTableDescription> CreateTableDescriptionList(
    const std::vector<NSkiff::TSkiffSchemaPtr>& skiffSchemas,
    const TString& rangeIndexColumnName,
    const TString& rowIndexColumnName)
{
    std::vector<TSkiffTableDescription> result;
    for (ui16 index = 0; index < skiffSchemas.size(); ++index) {
        result.emplace_back(CreateTableDescription(
            skiffSchemas[index],
            rangeIndexColumnName,
            rowIndexColumnName));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

static constexpr char ReferencePrefix = '$';

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSkiffSchemaRepresentation);

class TSkiffSchemaRepresentation
    : public TYsonSerializable
{
public:
    TString Name;
    NSkiff::EWireType WireType;
    TNullable<std::vector<INodePtr>> Children;

    TSkiffSchemaRepresentation()
    {
        RegisterParameter("name", Name)
            .Default();
        RegisterParameter("wire_type", WireType);
        RegisterParameter("children", Children)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TSkiffSchemaRepresentation);

////////////////////////////////////////////////////////////////////////////////

NSkiff::TSkiffSchemaPtr ParseSchema(
    const INodePtr& schemaNode,
    const IMapNodePtr& registry,
    THashMap<TString, TSkiffSchemaPtr>* parsedRegistry,
    THashSet<TString>* parseInProgressNames)
{
    auto schemaNodeType = schemaNode->GetType();
    if (schemaNodeType == ENodeType::String) {
        auto name = schemaNode->AsString()->GetValue();
        if (!name.StartsWith(ReferencePrefix)) {
            THROW_ERROR_EXCEPTION(
                "Invalid reference %Qv, reference must start with %Qv",
                name,
                ReferencePrefix);
        }
        name = name.substr(1);
        auto it = parsedRegistry->find(name);
        if (it != parsedRegistry->end()) {
            return it->second;
        } else if (parseInProgressNames->has(name)) {
            THROW_ERROR_EXCEPTION(
                "Type %Qv is recursive, recursive types are forbiden",
                name);
        } else {
            auto schemaFromRegistry = registry->FindChild(name);
            if (!schemaFromRegistry) {
                THROW_ERROR_EXCEPTION(
                    "Cannot resolve type reference: %Qv",
                    name);
            }
            parseInProgressNames->insert(name);
            auto result = ParseSchema(schemaFromRegistry, registry, parsedRegistry, parseInProgressNames);
            parseInProgressNames->erase(name);
            parsedRegistry->emplace(name, result);
            return result;
        }
    } else if (schemaNodeType == ENodeType::Map) {
        auto schemaMapNode = schemaNode->AsMap();
        auto schemaRepresentation = ConvertTo<TSkiffSchemaRepresentationPtr>(schemaMapNode);
        if (IsSimpleType(schemaRepresentation->WireType)) {
            return CreateSimpleTypeSchema(schemaRepresentation->WireType)->SetName(schemaRepresentation->Name);
        } else {
            if (!schemaRepresentation->Children) {
                THROW_ERROR_EXCEPTION(
                    "Complex type %Qlv lacks children",
                    schemaRepresentation->WireType);
            }
            std::vector<TSkiffSchemaPtr> childSchemaList;
            for (const auto& childNode : *schemaRepresentation->Children) {
                auto childSchema = ParseSchema(childNode, registry, parsedRegistry, parseInProgressNames);
                childSchemaList.push_back(childSchema);
            }

            switch (schemaRepresentation->WireType) {
                case EWireType::Variant8:
                    return CreateVariant8Schema(childSchemaList)->SetName(schemaRepresentation->Name);
                case EWireType::RepeatedVariant16:
                    return CreateRepeatedVariant16Schema(childSchemaList)->SetName(schemaRepresentation->Name);
                case EWireType::Tuple:
                    return CreateTupleSchema(childSchemaList)->SetName(schemaRepresentation->Name);
                default:
                    Y_UNREACHABLE();
            }
        }
    } else {
        THROW_ERROR_EXCEPTION(
            "Invalid type for Skiff schema description; expected %Qlv or %Qlv, found %Qlv",
            ENodeType::Map,
            ENodeType::String,
            schemaNodeType
        );
    }
}

std::vector<NSkiff::TSkiffSchemaPtr> ParseSkiffSchemas(
    const NYTree::IMapNodePtr& skiffSchemaRegistry,
    const NYTree::IListNodePtr& tableSkiffSchemas)
{
    THashMap<TString, TSkiffSchemaPtr> parsedRegistry;
    std::vector<NSkiff::TSkiffSchemaPtr> result;
    for (const auto& node : tableSkiffSchemas->GetChildren()) {
        THashSet<TString> parseInProgressNames;
        auto skiffSchema = ParseSchema(node, skiffSchemaRegistry, &parsedRegistry, &parseInProgressNames);
        result.push_back(skiffSchema);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TDenseFieldDescription::TDenseFieldDescription(TString name, const NSkiff::TSkiffSchemaPtr& deoptionalizedSchema, bool isRequired)
    : Name(std::move(name))
    , DeoptionalizedSchema(deoptionalizedSchema)
    , Required(isRequired)
{ }

////////////////////////////////////////////////////////////////////////////////

TSparseFieldDescription::TSparseFieldDescription(TString name, const NSkiff::TSkiffSchemaPtr& deoptionalizedSchema)
    : Name(std::move(name))
    , DeoptionalizedSchema(deoptionalizedSchema)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff
} // namespace NYT

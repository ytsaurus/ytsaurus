#include "type.h"
#include "private.h"

#include <library/cpp/yt/string/format.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NOrm::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TObjectTypeRegistry
    : public IObjectTypeRegistry
{
public:
    TObjectTypeRegistry(std::vector<const TObjectType> objectTypes)
        : Types_(std::move(objectTypes))
    {
        for (auto& type : Types_) {
            YT_LOG_DEBUG("Adding object type ("
                "Value: %v, "
                "Name: %v, "
                "HumanReadableName: %v, "
                "CapitalizedHumanReadableName: %v)",
                type.Value,
                type.Name,
                type.HumanReadableName,
                type.CapitalizedHumanReadableName);

            EmplaceOrCrash(ByValue_, type.Value, &type);
            EmplaceOrCrash(ByName_, type.Name, &type);
        }
    }

    const TObjectType* FindTypeByValue(TObjectTypeValue value) const override
    {
        auto it = ByValue_.find(value);
        if (it == ByValue_.end()) {
            return nullptr;
        }
        return it->second;
    }

    const TObjectType* GetTypeByValueOrCrash(TObjectTypeValue value) const override
    {
        const auto* result = FindTypeByValue(value);
        YT_VERIFY(result);
        return result;
    }

    const TObjectType* GetTypeByValueOrThrow(TObjectTypeValue value) const override
    {
        const auto* result = FindTypeByValue(value);
        if (!result) {
            THROW_ERROR_EXCEPTION(EErrorCode::InvalidObjectType,
                "Unknown object type value %v",
                value);
        }
        return result;
    }

    const TObjectType* FindTypeByName(const TObjectTypeName& name) const override
    {
        auto it = ByName_.find(name);
        if (it == ByName_.end()) {
            return nullptr;
        }
        return it->second;
    }

    const TObjectType* GetTypeByNameOrCrash(const TObjectTypeName& name) const override
    {
        const auto* result = FindTypeByName(name);
        YT_VERIFY(result);
        return result;
    }

    const TObjectType* GetTypeByNameOrThrow(const TObjectTypeName& name) const override
    {
        const auto* result = FindTypeByName(name);
        if (!result) {
            THROW_ERROR_EXCEPTION(EErrorCode::InvalidObjectType,
                "Unknown object type name %Qv",
                name);
        }
        return result;
    }

    const std::vector<const TObjectType>& GetTypes() const override
    {
        return Types_;
    }

private:
    const std::vector<const TObjectType> Types_;

    THashMap<TObjectTypeValue, const TObjectType*> ByValue_;
    THashMap<TObjectTypeName, const TObjectType*> ByName_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

IObjectTypeRegistryPtr CreateObjectTypeRegistry(std::vector<const TObjectType> types)
{
    return New<TObjectTypeRegistry>(std::move(types));
}

////////////////////////////////////////////////////////////////////////////////

const TObjectTypeName& IObjectTypeRegistry::GetHumanReadableTypeNameOrCrash(TObjectTypeValue type) const
{
    return GetTypeByValueOrCrash(type)->HumanReadableName;
}

const TObjectTypeName& IObjectTypeRegistry::GetHumanReadableTypeNameOrThrow(TObjectTypeValue type) const
{
    return GetTypeByValueOrThrow(type)->HumanReadableName;
}

const TObjectTypeName& IObjectTypeRegistry::GetCapitalizedHumanReadableTypeNameOrCrash(TObjectTypeValue type) const
{
    return GetTypeByValueOrCrash(type)->CapitalizedHumanReadableName;
}

const TObjectTypeName& IObjectTypeRegistry::GetCapitalizedHumanReadableTypeNameOrThrow(TObjectTypeValue type) const
{
    return GetTypeByValueOrThrow(type)->CapitalizedHumanReadableName;
}

TObjectTypeValue IObjectTypeRegistry::GetTypeValueByNameOrCrash(const TObjectTypeName& name) const
{
    return GetTypeByNameOrCrash(name)->Value;
}

TObjectTypeValue IObjectTypeRegistry::GetTypeValueByNameOrThrow(const TObjectTypeName& name) const
{
    return GetTypeByNameOrThrow(name)->Value;
}

const TObjectTypeName& IObjectTypeRegistry::GetTypeNameByValueOrCrash(TObjectTypeValue value) const
{
    return GetTypeByValueOrCrash(value)->Name;
}

const TObjectTypeName& IObjectTypeRegistry::GetTypeNameByValueOrThrow(TObjectTypeValue value) const
{
    return GetTypeByValueOrThrow(value)->Name;
}

TObjectTypeName IObjectTypeRegistry::FormatTypeValue(TObjectTypeValue value) const
{
    const auto* type = FindTypeByValue(value);
    if (!type) {
        return Format("EObjectType(%v)",
            value);
    }
    return type->Name;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects

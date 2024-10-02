#pragma once

#include "public.h"

#include <yt/yt/core/logging/public.h>

namespace NYT::NOrm::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TObjectType
{
    // 0, 1, 2, -1.
    TObjectTypeValue Value;

    // Corresponds to the snake_case_name proto option.
    // E.g.: "pod_set", "dns_record_set", "multi_cluster_replica_set".
    TObjectTypeName Name;

    // E.g.: "pod set", "DNS record set", "multi-cluster replica set".
    TObjectTypeName HumanReadableName;

    // E.g.: "Pod set", "DNS record set", "Multi-cluster replica set".
    TObjectTypeName CapitalizedHumanReadableName;
};

////////////////////////////////////////////////////////////////////////////////

struct IObjectTypeRegistry
    : public TRefCounted
{
    virtual const TObjectType* FindTypeByValue(TObjectTypeValue value) const = 0;
    virtual const TObjectType* GetTypeByValueOrCrash(TObjectTypeValue value) const = 0;
    virtual const TObjectType* GetTypeByValueOrThrow(TObjectTypeValue value) const = 0;

    virtual const TObjectType* FindTypeByName(const TObjectTypeName& name) const = 0;
    virtual const TObjectType* GetTypeByNameOrCrash(const TObjectTypeName& name) const = 0;
    virtual const TObjectType* GetTypeByNameOrThrow(const TObjectTypeName& name) const = 0;

    // NB! Null object type is included.
    virtual const std::vector<const TObjectType>& GetTypes() const = 0;

    // Helpers.

    const TObjectTypeName& GetHumanReadableTypeNameOrCrash(TObjectTypeValue type) const;
    const TObjectTypeName& GetHumanReadableTypeNameOrThrow(TObjectTypeValue type) const;
    const TObjectTypeName& GetCapitalizedHumanReadableTypeNameOrCrash(TObjectTypeValue type) const;
    const TObjectTypeName& GetCapitalizedHumanReadableTypeNameOrThrow(TObjectTypeValue type) const;

    TObjectTypeValue GetTypeValueByNameOrCrash(const TObjectTypeName& name) const;
    TObjectTypeValue GetTypeValueByNameOrThrow(const TObjectTypeName& name) const;

    const TObjectTypeName& GetTypeNameByValueOrCrash(TObjectTypeValue value) const;
    const TObjectTypeName& GetTypeNameByValueOrThrow(TObjectTypeValue value) const;

    //! Best-effort conversion of object type value to object name.
    //! Does not throw or crash even when #value is unknown.
    TObjectTypeName FormatTypeValue(TObjectTypeValue value) const;
};

DEFINE_REFCOUNTED_TYPE(IObjectTypeRegistry)

////////////////////////////////////////////////////////////////////////////////

IObjectTypeRegistryPtr CreateObjectTypeRegistry(std::vector<const TObjectType> types);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects

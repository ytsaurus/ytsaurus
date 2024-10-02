#pragma once

#include "public.h"

#include <yt/yt/orm/server/access_control/public.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

// Does not check read permissions. Call |Validated|, process permissions in the callback or
// explicitly supply an empty callback.
TResolveAttributeResult ResolveAttribute(
    const IObjectTypeHandler* typeHandler,
    const NYPath::TYPath& path,
    TAttributeSchemaCallback callback = {},
    bool validateProtoSchemaCompliance = true);

TResolveAttributeResult ResolveAttributeValidated(
    const IObjectTypeHandler* typeHandler,
    const NYPath::TYPath& path);

////////////////////////////////////////////////////////////////////////////////

class TAttributePermissionsCollector
{
public:
    TAttributePermissionsCollector() = default;
    TAttributePermissionsCollector(const TAttributePermissionsCollector&) = delete;
    TAttributePermissionsCollector& operator=(const TAttributePermissionsCollector&) = delete;

    using TPermissions = THashSet<NAccessControl::TAccessControlPermissionValue>;

    operator TAttributeSchemaCallback();

    const TPermissions& GetReadPermissions() const;

private:
    TPermissions ReadPermissions_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TValue>
TValue GetScalarValueOrThrow(TTransaction* transaction, TObject* object, const TString& path);

template <typename TValue>
void SetScalarValueOrThrow(
    TTransaction* transaction,
    TObject* object,
    const TString& path,
    const TValue& value,
    bool recursive,
    std::optional<bool> sharedWrite,
    EAggregateMode aggregateMode,
    const TTransactionCallContext& transactionCallContext);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define OBJECT_REFLECTION_INL_H_
#include "object_reflection-inl.h"
#undef OBJECT_REFLECTION_INL_H_

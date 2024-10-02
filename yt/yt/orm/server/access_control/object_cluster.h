#pragma once

#include "public.h"

#include <yt/yt/orm/client/objects/key.h>

namespace NYT::NOrm::NServer::NAccessControl {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotObject
    : public TRefTracked<TSnapshotObject>
{
public:
    TSnapshotObject(
        TObjectTypeValue type,
        TObjectKey key,
        TObjectKey accessControlParentKey,
        TAccessControlList acl,
        bool inheritAcl,
        NYson::TYsonString labels);

    DEFINE_BYVAL_RO_PROPERTY(TObjectTypeValue, Type);
    DEFINE_BYREF_RO_PROPERTY(TObjectKey, Key);
    DEFINE_BYREF_RO_PROPERTY(TObjectKey, AccessControlParentKey);
    DEFINE_BYREF_RO_PROPERTY(TAccessControlList, Acl);
    DEFINE_BYVAL_RO_PROPERTY(bool, InheritAcl);
    DEFINE_BYREF_RO_PROPERTY(NYson::TYsonString, Labels);
};

////////////////////////////////////////////////////////////////////////////////

class TClusterObjectSnapshot
    : public TRefCounted
{
public:
    void AddObjects(
        TObjectTypeValue objectType,
        std::vector<std::unique_ptr<TSnapshotObject>> objects);

    TSnapshotObject* FindObject(
        TObjectTypeValue type,
        const TObjectKey& key) const;
    TRange<TSnapshotObject*> GetSortedObjects(
        TObjectTypeValue type) const;

    bool ContainsObjectType(
        TObjectTypeValue objectType) const;
    void ValidateContainsObjectType(
        TObjectTypeValue objectType) const;

private:
    THashSet<TObjectTypeValue> ObjectTypes_;
    THashMap<TObjectTypeValue, THashMap<TObjectKey, std::unique_ptr<TSnapshotObject>>> Objects_;
    THashMap<TObjectTypeValue, std::vector<TSnapshotObject*>> SortedObjects_;
};

DEFINE_REFCOUNTED_TYPE(TClusterObjectSnapshot)

////////////////////////////////////////////////////////////////////////////////

TClusterObjectSnapshotPtr BuildClusterObjectSnapshot(
    const IDataModelInteropPtr& dataModelInterop,
    const NObjects::TTransactionPtr& transaction,
    const std::vector<TObjectTypeValue>& leafObjectTypes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl

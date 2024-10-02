#pragma once

#include "public.h"

namespace NYT::NOrm::NServer::NAccessControl {

////////////////////////////////////////////////////////////////////////////////

class TAccessControlHierarchyBase
{
public:
    virtual ~TAccessControlHierarchyBase() = default;

    template <std::invocable<TObjectTypeValue> TFunction>
    void ForEachImmediateParentType(
        NObjects::IObjectTypeHandler* typeHandler,
        const TFunction& function) const;
};

////////////////////////////////////////////////////////////////////////////////

class TTransactionAccessControlHierarchy
    : public TAccessControlHierarchyBase
{
public:
    template <std::invocable<NObjects::TObject*> TFunction>
    void ForEachImmediateParent(
        const NObjects::TObject* object,
        const TFunction& function,
        std::source_location location = std::source_location::current()) const;
};

////////////////////////////////////////////////////////////////////////////////

class TSnapshotAccessControlHierarchy
    : public TAccessControlHierarchyBase
{
public:
    TSnapshotAccessControlHierarchy(
        NObjects::TObjectManagerPtr objectManager,
        TClusterObjectSnapshotPtr clusterObjectSnapshot);

    template <std::invocable<const TSnapshotObject*> TFunction>
    void ForEachImmediateParent(
        const TSnapshotObject* object,
        const TFunction& function,
        std::source_location location = std::source_location::current()) const;

private:
    const NObjects::TObjectManagerPtr ObjectManager_;
    const TClusterObjectSnapshotPtr ClusterObjectSnapshot_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl

#define ACCESS_CONTROL_HIERARCHY_INL_H_
#include "access_control_hierarchy-inl.h"
#undef ACCESS_CONTROL_HIERARCHY_INL_H_

#ifndef ACCESS_CONTROL_HIERARCHY_INL_H_
#error "Direct inclusion of this file is not allowed, include access_control_hierarchy.h"
// For the sake of sane code completion.
#include "access_control_hierarchy.h"
#endif

#include "object_cluster.h"

#include <yt/yt/orm/server/objects/helpers.h>
#include <yt/yt/orm/server/objects/object.h>
#include <yt/yt/orm/server/objects/object_manager.h>
#include <yt/yt/orm/server/objects/type_handler.h>

namespace NYT::NOrm::NServer::NAccessControl {

////////////////////////////////////////////////////////////////////////////////

template <std::invocable<TObjectTypeValue> TFunction>
void TAccessControlHierarchyBase::ForEachImmediateParentType(
    NObjects::IObjectTypeHandler* typeHandler,
    const TFunction& function) const
{
    if (typeHandler->GetType() == NClient::NObjects::TObjectTypeValues::Schema) {
        return;
    }
    auto accessControlParentType = typeHandler->GetAccessControlParentType();
    if (accessControlParentType != TObjectTypeValues::Null) {
        function(accessControlParentType);
    }
    function(NClient::NObjects::TObjectTypeValues::Schema);
}

////////////////////////////////////////////////////////////////////////////////

template <std::invocable<NObjects::TObject*> TFunction>
void TTransactionAccessControlHierarchy::ForEachImmediateParent(
    const NObjects::TObject* object,
    const TFunction& function,
    std::source_location location) const
{
    auto* typeHandler = object->GetTypeHandler();
    if (typeHandler->GetType() == NClient::NObjects::TObjectTypeValues::Schema) {
        return;
    }
    if (const auto* accessControlParent = typeHandler->GetAccessControlParent(object, location)) {
        function(accessControlParent);
    }
    const auto* schema = typeHandler->GetSchemaObject(object);
    YT_VERIFY(schema);
    function(schema);
}

////////////////////////////////////////////////////////////////////////////////

template <std::invocable<const TSnapshotObject*> TFunction>
void TSnapshotAccessControlHierarchy::ForEachImmediateParent(
    const TSnapshotObject* object,
    const TFunction& function,
    std::source_location /*location*/) const
{
    if (object->GetType() == NClient::NObjects::TObjectTypeValues::Schema) {
        return;
    }
    auto typeHandler = ObjectManager_->GetTypeHandlerOrCrash(object->GetType());
    const auto* accessControlParent = ClusterObjectSnapshot_->FindObject(
        typeHandler->GetAccessControlParentType(),
        object->AccessControlParentKey());
    if (accessControlParent) {
        function(accessControlParent);
    }
    const auto* schema = ClusterObjectSnapshot_->FindObject(
        NClient::NObjects::TObjectTypeValues::Schema,
        TObjectKey(typeHandler->GetSchemaObjectId()));
    YT_VERIFY(schema);
    function(schema);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl

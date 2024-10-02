#include "object_reflection.h"

#include "attribute_schema.h"

#include <yt/yt/orm/server/access_control/access_control_manager.h>

#include <yt/yt/orm/server/master/bootstrap.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

TResolveAttributeResult ResolveAttribute(
    const IObjectTypeHandler* typeHandler,
    const NYPath::TYPath& path,
    std::function<void(const TAttributeSchema*)> callback,
    bool validateProtoSchemaCompliance)
{
    auto result = typeHandler->ResolveAttribute(path, callback, validateProtoSchemaCompliance);
    return TResolveAttributeResult{result.Attribute, result.SuffixPath};
}

TResolveAttributeResult ResolveAttributeValidated(
    const IObjectTypeHandler* typeHandler,
    const NYPath::TYPath& path)
{
    auto validator = [typeHandler] (const TAttributeSchema* schema) {
        auto readPermission = schema->GetReadPermission();
        if (readPermission == NAccessControl::TAccessControlPermissionValues::None) {
            return;
        }
        const auto& accessControlManager = typeHandler->GetBootstrap()->GetAccessControlManager();
        accessControlManager->ValidateSuperuser(Format(
            "reference attribute %v in a broad query "
            "since it requires special read permission %v",
            schema->FormatPathEtc(),
            accessControlManager->FormatPermissionValue(readPermission)));
    };

    return ResolveAttribute(typeHandler, path, validator);
}

////////////////////////////////////////////////////////////////////////////////

TAttributePermissionsCollector::operator TAttributeSchemaCallback()
{
    return [this] (const TAttributeSchema* schema) {
        auto readPermission = schema->GetReadPermission();
        if (readPermission == NAccessControl::TAccessControlPermissionValues::None) {
            return;
        }
        ReadPermissions_.insert(readPermission);
    };
}

const TAttributePermissionsCollector::TPermissions&
TAttributePermissionsCollector::GetReadPermissions() const
{
    return ReadPermissions_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

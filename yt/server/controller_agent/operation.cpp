#include "operation.h"

#include <yt/ytlib/controller_agent/proto/controller_agent_service.pb.h>

#include <yt/core/ytree/convert.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT::NControllerAgent {

using namespace NYTree;
using namespace NYson;
using namespace NSecurityClient;
using namespace NScheduler;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TOperation::TOperation(const NProto::TOperationDescriptor& descriptor)
    : Id_(FromProto<TOperationId>(descriptor.operation_id()))
    , Type_(static_cast<EOperationType>(descriptor.operation_type()))
    , Spec_(ConvertToNode(TYsonString(descriptor.spec(), EYsonType::Node))->AsMap())
    , StartTime_(FromProto<TInstant>(descriptor.start_time()))
    , AuthenticatedUser_(descriptor.authenticated_user())
    , SecureVault_(descriptor.has_secure_vault()
        ? ConvertToNode(TYsonString(descriptor.secure_vault(), EYsonType::Node))->AsMap()
        : NYTree::IMapNodePtr())
    , Acl_(ConvertTo<TSerializableAccessControlList>(TYsonString(descriptor.acl())))
    , UserTransactionId_(FromProto<NTransactionClient::TTransactionId>(descriptor.user_transaction_id()))
    , PoolTreeControllerSettingsMap_(FromProto<TPoolTreeControllerSettingsMap>(descriptor.pool_tree_controller_settings_map()))
{ }

const IOperationControllerPtr& TOperation::GetControllerOrThrow() const
{
    if (!Controller_) {
        THROW_ERROR_EXCEPTION("Operation %v is missing controller",
            Id_);
    }
    return Controller_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

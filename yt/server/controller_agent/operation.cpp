#include "operation.h"

#include <yt/server/controller_agent/proto/controller_agent_service.pb.h>

#include <yt/core/ytree/convert.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NControllerAgent {

using namespace NYTree;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TOperation::TOperation(const NProto::TOperationDescriptor& descriptor)
    : Id_(FromProto<TOperationId>(descriptor.operation_id()))
    , Type_(static_cast<EOperationType>(descriptor.operation_type()))
    , Spec_(ConvertToNode(NYson::TYsonString(descriptor.spec(), NYson::EYsonType::Node))->AsMap())
    , StartTime_(FromProto<TInstant>(descriptor.start_time()))
    , AuthenticatedUser_(descriptor.authenticated_user())
    , SecureVault_(descriptor.has_secure_vault()
        ? ConvertToNode(NYson::TYsonString(descriptor.secure_vault(), NYson::EYsonType::Node))->AsMap()
        : NYTree::IMapNodePtr())
    , Owners_(FromProto<std::vector<TString>>(descriptor.owners()))
    , UserTransactionId_(FromProto<NTransactionClient::TTransactionId>(descriptor.user_transaction_id()))
    , PoolTreeSchedulingTagFilters_(FromProto<std::vector<NScheduler::TSchedulingTagFilter>>(descriptor.pool_tree_scheduling_tag_filters()))
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

} // namespace NControllerAgent
} // namespace NYT

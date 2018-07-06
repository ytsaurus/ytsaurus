#include "operation.h"

#include <yt/server/controller_agent/proto/controller_agent_service.pb.h>

#include <yt/core/ytree/convert.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {

using namespace NYTree;
using namespace NScheduler;

namespace NControllerAgent {

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
    , PoolTreeToSchedulingTagFilter_(FromProto<TPoolTreeToSchedulingTagFilter>(descriptor.pool_tree_scheduling_tag_filters()))
{
    auto node = Spec_->FindChild("enable_compatible_storage_mode");
    EnableCompatibleStorageMode_ = node ? node->AsBoolean()->GetValue() : true;
}

const IOperationControllerPtr& TOperation::GetControllerOrThrow() const
{
    if (!Controller_) {
        THROW_ERROR_EXCEPTION("Operation %v is missing controller",
            Id_);
    }
    return Controller_;
}

} // namespace NControllerAgent

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NControllerAgent::NProto::TOperationDescriptor::TPoolTreeSchedulingTagFilters* protoTreeFilters,
    const TPoolTreeToSchedulingTagFilter& treeFilters)
{
    for (const auto& pair : treeFilters) {
        auto* protoTreeFilter = protoTreeFilters->add_tree_filter();
        protoTreeFilter->set_tree_name(pair.first);
        ToProto(protoTreeFilter->mutable_filter(), pair.second);
    }
}

void FromProto(
    TPoolTreeToSchedulingTagFilter* treeFilters,
    const NControllerAgent::NProto::TOperationDescriptor::TPoolTreeSchedulingTagFilters protoTreeFilters)
{
    for (const auto& protoTreeFilter : protoTreeFilters.tree_filter()) {
        treeFilters->insert(std::make_pair(protoTreeFilter.tree_name(), FromProto<TSchedulingTagFilter>(protoTreeFilter.filter())));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

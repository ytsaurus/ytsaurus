#include "operation.h"

#include "operation_controller.h"

#include <yt/yt/ytlib/controller_agent/proto/controller_agent_service.pb.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

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
    , ControllerEpoch_(descriptor.controller_epoch())
    , ExperimentAssignments_(ConvertTo<std::vector<TExperimentAssignmentPtr>>(TYsonString(descriptor.experiment_assignments())))
{ }

const IOperationControllerPtr& TOperation::GetControllerOrThrow() const
{
    if (!Controller_) {
        THROW_ERROR_EXCEPTION(
            "Controller of operation %v is missing",
            Id_);
    }
    return Controller_;
}

void TOperation::UpdateJobShellOptions(const TJobShellOptionsUpdeteMap& update)
{
    ApplyJobShellOptionsUpdate(&OptionsPerJobShell_, update);
}

std::optional<NScheduler::TJobShellInfo> TOperation::GetJobShellInfo(const TString& jobShellName)
{
    const auto& controller = GetControllerOrThrow();

    TJobShellPtr jobShell;
    for (const auto& shell : controller->GetJobShells()) {
        if (shell->Name == jobShellName) {
            jobShell = shell;
        }
    }

    if (!jobShell) {
        return {};
    }

    TOperationJobShellRuntimeParametersPtr runtimeParameters;
    if (auto it = OptionsPerJobShell_.find(jobShellName); it != OptionsPerJobShell_.end()) {
        runtimeParameters = it->second;
    }

    return NScheduler::TJobShellInfo(
        std::move(jobShell),
        std::move(runtimeParameters));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

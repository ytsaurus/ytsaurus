#include "operation.h"
#include "operation_controller.h"

#include <yt/ytlib/scheduler/config.h>

namespace NYT {
namespace NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

TOperation::TOperation(const TOperationDescription& description)
    : Id_(description.Id)
    , Spec_(description.Spec)
    , AuthenticatedUser_(description.AuthenticatedUser)
    , StartTime_(description.StartTime)
    , RuntimeParams_(New<NScheduler::TOperationRuntimeParameters>())
{
    auto spec = NYTree::ConvertTo<NScheduler::TOperationSpecBasePtr>(Spec_);

    RuntimeParams_->Owners = spec->Owners;
    // Other runtime params is filled with scheduler strategy with respect to
    // configured fair-share trees.
}

const NScheduler::TOperationId& TOperation::GetId() const
{
    return Id_;
}

bool TOperation::IsSchedulable() const
{
    return true;
}

TInstant TOperation::GetStartTime() const
{
    return StartTime_;
}

TString TOperation::GetAuthenticatedUser() const
{
    return AuthenticatedUser_;
}

TNullable<int> TOperation::FindSlotIndex(const TString& /* treeId */) const
{
    return 0;
}

int TOperation::GetSlotIndex(const TString& /* treeId */) const
{
    return 0;
}

void TOperation::SetSlotIndex(const TString& /* treeId */, int /* index */)
{ }

NScheduler::IOperationControllerStrategyHostPtr TOperation::GetControllerStrategyHost() const
{
    return Controller_;
}

NYTree::IMapNodePtr TOperation::GetSpec() const
{
    return Spec_;
}

NScheduler::TOperationRuntimeParametersPtr TOperation::GetRuntimeParameters() const
{
    return RuntimeParams_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSchedulerSimulator
} // namespace NYT

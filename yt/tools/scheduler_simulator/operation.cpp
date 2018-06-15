#include "operation.h"
#include "operation_controller.h"

#include <yt/ytlib/scheduler/config.h>
#include <yt/ytlib/scheduler/public.h>

namespace NYT {
namespace NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

TOperation::TOperation(const TOperationDescription& description)
    : Id_(description.Id)
    , Type_(description.Type)
    , Spec_(description.Spec)
    , AuthenticatedUser_(description.AuthenticatedUser)
    , StartTime_(description.StartTime)
    , RuntimeParams_(New<NScheduler::TOperationRuntimeParameters>())
{
    RuntimeParams_->FillFromSpec(NYTree::ConvertTo<NScheduler::TOperationSpecBasePtr>(Spec_), Null);
}

const NScheduler::TOperationId& TOperation::GetId() const
{
    return Id_;
}

NScheduler::EOperationType TOperation::GetType() const
{
    return Type_;
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

bool TOperation::SetCompleting()
{
    return !Completing_.exchange(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSchedulerSimulator
} // namespace NYT

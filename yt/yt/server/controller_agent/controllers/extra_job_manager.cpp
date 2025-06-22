#include "extra_job_manager.h"

namespace NYT::NControllerAgent::NControllers {

void IExtraJobManager::OnOperationRevived(THashMap<TJobId, EAbortReason>* /* jobsToAbort */)
{
}
/////////////////////////////////////////////////////////////////////////////////

} // NYT::NControllerAgent::NControllers

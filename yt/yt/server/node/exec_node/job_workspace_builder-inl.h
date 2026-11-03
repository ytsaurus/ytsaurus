#ifndef JOB_WORKSPACE_BUILDER_INL_H_
#error "Direct inclusion of this file is not allowed, include job_workspace_builder.h"
// For the sake of sane code completion.
#include "job_workspace_builder.h"
#endif

#include <yt/yt/core/actions/bind.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

template <TJobWorkspaceBuilder::EStep Step>
TCallback<TFuture<void>()> TJobWorkspaceBuilder::MakeStep()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return BIND([this, this_ = MakeStrong(this)] {
        return GuardedAction(Step);
    }).AsyncVia(Invoker_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

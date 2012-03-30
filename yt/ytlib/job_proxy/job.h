#pragma once

#include <ytlib/scheduler/jobs.pb.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct IJob
{
    virtual NScheduler::NProto::TJobResult Run() = 0;
    // virtual TProgress GetProgress() = 0 const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
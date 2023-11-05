#pragma once

#include <yt/yt/client/job_tracker_client/public.h>

namespace NYT::NJobTrackerClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TJobSpec;
class TReqHeartbeat;
class TRspHeartbeat;
class TJobResult;
class TJobStatus;

} // namespace NProto

DEFINE_ENUM(EJobPhase,
    ((Missing)                      (100))

    ((Created)                      (  0))
    ((PreparingNodeDirectory)       (  5))
    ((DownloadingArtifacts)         ( 10))
    ((PreparingSandboxDirectories)  ( 15))
    ((PreparingRootVolume)          ( 25))
    ((RunningSetupCommands)         ( 26))
    ((RunningGpuCheckCommand)       ( 27))
    ((SpawningJobProxy)             ( 28))
    ((PreparingArtifacts)           ( 29))
    ((PreparingJob)                 ( 30))
    ((Running)                      ( 40))
    ((FinalizingJobProxy)           ( 50))
    ((RunningExtraGpuCheckCommand)  ( 55))
    ((WaitingForCleanup)            ( 60))
    ((Cleanup)                      ( 70))
    ((Finished)                     ( 80))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobTrackerClient

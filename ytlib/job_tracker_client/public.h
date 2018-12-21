#pragma once

#include <yt/client/job_tracker_client/public.h>

namespace NYT::NJobTrackerClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TJobSpec;
class TReqHeartbeat;
class TRspHeartbeat;
class TJobResult;
class TJobStatus;

} // namespace NProto

class TStatistics;

DEFINE_ENUM(EJobPhase,
    ((Missing)                      (100))

    ((Created)                      (  0))
    ((PreparingNodeDirectory)       (  5))
    ((DownloadingArtifacts)         ( 10))
    ((PreparingSandboxDirectories)  ( 15))
    ((PreparingArtifacts)           ( 20))
    ((PreparingRootVolume)          ( 25))
    ((PreparingProxy)               ( 30))
    ((Running)                      ( 40))
    ((FinalizingProxy)              ( 50))
    ((WaitingAbort)                 ( 60))
    ((Cleanup)                      ( 70))
    ((Finished)                     ( 80))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobTrackerClient

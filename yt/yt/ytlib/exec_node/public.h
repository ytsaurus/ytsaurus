#pragma once

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

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

} // namespace NYT::NExecNode

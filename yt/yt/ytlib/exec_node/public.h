#pragma once

#include <yt/yt/core/misc/error_code.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((ConfigCreationFailed)                  (1100))
    ((AbortByScheduler)                      (1101))
    ((ResourceOverdraft)                     (1102))
    ((WaitingJobTimeout)                     (1103))
    ((SlotNotFound)                          (1104))
    ((JobEnvironmentDisabled)                (1105))
    ((JobProxyConnectionFailed)              (1106))
    ((ArtifactCopyingFailed)                 (1107))
    ((NodeDirectoryPreparationFailed)        (1108))
    ((SlotLocationDisabled)                  (1109))
    ((QuotaSettingFailed)                    (1110))
    ((RootVolumePreparationFailed)           (1111))
    ((NotEnoughDiskSpace)                    (1112))
    ((ArtifactDownloadFailed)                (1113))
    ((JobProxyPreparationTimeout)            (1114))
    ((JobPreparationTimeout)                 (1115))
    ((FatalJobPreparationTimeout)            (1116))
    ((JobProxyFailed)                        (1120))
    ((SetupCommandFailed)                    (1121))
    ((GpuLayerNotFetched)                    (1122))
    ((GpuJobWithoutLayers)                   (1123))
    ((TmpfsOverflow)                         (1124))
    ((GpuCheckCommandFailed)                 (1125))
    ((GpuCheckCommandPreparationFailed)      (1126))
    ((JobProxyUnavailable)                   (1127))
    ((NodeResourceOvercommit)                (1128))
    ((LayerUnpackingFailed)                  (1129))
    ((TmpfsLayerImportFailed)                (1130))
    ((SchedulerJobsDisabled)                 (1131))
    ((DockerImagePullingFailed)              (1132))
    ((InvalidImage)                          (1133))
    ((AbortByControllerAgent)                (1134))
    ((NoSuchJob)                             (1135))
    ((NoLayerLocationAvailable)              (1136))
    ((ArtifactFetchFailed)                   (1137))
    ((LayerLocationDisabled)                 (1138))
    ((PortoVolumeManagerFailure)             (1139))
    ((PortoHealthCheckFailed)                (1140))
    ((PortoExecutorFailure)                  (1141))
    ((JobCleanupTimeout)                     (1142))
    ((NotEnoughInitializedSlots)             (1143))
);

DEFINE_ENUM(EJobPhase,
    ((Missing)                      (100))

    ((Created)                      (  0))
    ((PreparingNodeDirectory)       (  5))
    ((DownloadingArtifacts)         ( 10))
    ((PreparingRootVolume)          ( 15))
    ((PreparingGpuCheckVolume)      ( 20))
    ((PreparingSandboxDirectories)  ( 25))
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

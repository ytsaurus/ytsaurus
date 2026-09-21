#pragma once

#include <library/cpp/yt/error/error_code.h>

#include <library/cpp/yt/misc/enum.h>

#include <util/generic/size_literals.h>

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
    ((VolumeSizeLimitExceeded)               (1124))
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
    ((WaitingForJobCleanupTimeout)           (1144))
    ((InterruptionFailed)                    (1145))
    ((SimpleVolumeManagerFailure)            (1146))
    ((NonRootVolumePreparationFailed)        (1147))
    ((NonRootVolumeLinkingFailed)            (1148))
    ((NbdServerDisabledOnNode)               (1149))
    ((OverlayLayerPreparationFailed)         (1150))
);

// COMPAT(pogorelov): Remove in 26.3.
DEFINE_ENUM(EJobPhaseOld,
    ((Missing)                      (100))

    ((Created)                      (  0))
    ((PreparingNodeDirectory)       (  5))
    ((DownloadingArtifacts)         ( 10))
    ((CachingArtifacts)             ( 11))
    ((PreparingLayers)              ( 12))
    ((PreparingSlotDirectories)     ( 13))
    ((PreparingVolumes)             ( 15))
    ((PreparingGpuCheckVolume)      ( 20))
    ((LinkingVolumes)               ( 21))
    ((ValidatingRootFS)             ( 22))
    ((RunningCustomPreparations)    ( 24))
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

DEFINE_ENUM_UNKNOWN_VALUE(EJobPhaseOld, Missing);

DEFINE_ENUM(EJobPhase,
    ((Missing)                      (100000))

    ((Created)                      (     0))
    ((PreparingNodeDirectory)       (  1000))
    ((DownloadingArtifacts)         (  2000))
    ((CachingArtifacts)             (  3000))
    ((PreparingSlotDirectories)     (  4000))
    ((PreparingLayers)              (  5000))
    ((PreparingVolumes)             (  6000))
    ((PreparingGpuCheckVolume)      (  7000))
    ((LinkingVolumes)               (  8000))
    ((ValidatingRootFS)             (  9000))
    ((PreparingSandboxDirectories)  ( 10000))
    ((RunningSetupCommands)         ( 11000))
    ((RunningCustomPreparations)    ( 12000))
    ((RunningGpuCheckCommand)       ( 13000))
    ((SpawningJobProxy)             ( 14000))
    ((PreparingArtifacts)           ( 15000))
    ((PreparingJob)                 ( 16000))
    ((Running)                      ( 17000))
    ((FinalizingJobProxy)           ( 18000))
    ((RunningExtraGpuCheckCommand)  ( 19000))
    ((WaitingForCleanup)            ( 20000))
    ((Cleanup)                      ( 21000))
    ((Finished)                     ( 22000))
);

DEFINE_ENUM_UNKNOWN_VALUE(EJobPhase, Missing);

EJobPhase ConvertJobPhaseFromOld(EJobPhaseOld phase);
EJobPhaseOld ConvertJobPhaseToOld(EJobPhase phase);

////////////////////////////////////////////////////////////////////////////////

static constexpr i64 MaxNbdDiskSize = 512_GB;

//! Default number of TCP connections used for NBD RPC requests (multiplexing parallelism).
static constexpr int DefaultNbdMultiplexingParallelism = 3;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

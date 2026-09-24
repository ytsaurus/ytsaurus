#include "public.h"

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

EJobPhase ConvertJobPhaseFromOld(EJobPhaseOld phase)
{
    switch (phase) {
        case EJobPhaseOld::Missing:
            return EJobPhase::Missing;
        case EJobPhaseOld::Created:
            return EJobPhase::Created;
        case EJobPhaseOld::PreparingNodeDirectory:
            return EJobPhase::PreparingNodeDirectory;
        case EJobPhaseOld::DownloadingArtifacts:
            return EJobPhase::DownloadingArtifacts;
        case EJobPhaseOld::CachingArtifacts:
            return EJobPhase::CachingArtifacts;
        case EJobPhaseOld::PreparingLayers:
            return EJobPhase::PreparingLayers;
        case EJobPhaseOld::PreparingSlotDirectories:
            return EJobPhase::PreparingSlotDirectories;
        case EJobPhaseOld::PreparingVolumes:
            return EJobPhase::PreparingVolumes;
        case EJobPhaseOld::PreparingGpuCheckVolume:
            return EJobPhase::PreparingGpuCheckVolume;
        case EJobPhaseOld::LinkingVolumes:
            return EJobPhase::LinkingVolumes;
        case EJobPhaseOld::ValidatingRootFS:
            return EJobPhase::ValidatingRootFS;
        case EJobPhaseOld::RunningCustomPreparations:
            return EJobPhase::RunningCustomPreparations;
        case EJobPhaseOld::PreparingSandboxDirectories:
            return EJobPhase::PreparingSandboxDirectories;
        case EJobPhaseOld::RunningSetupCommands:
            return EJobPhase::RunningSetupCommands;
        case EJobPhaseOld::RunningGpuCheckCommand:
            return EJobPhase::RunningGpuCheckCommand;
        case EJobPhaseOld::SpawningJobProxy:
            return EJobPhase::SpawningJobProxy;
        case EJobPhaseOld::PreparingArtifacts:
            return EJobPhase::PreparingArtifacts;
        case EJobPhaseOld::PreparingJob:
            return EJobPhase::PreparingJob;
        case EJobPhaseOld::Running:
            return EJobPhase::Running;
        case EJobPhaseOld::FinalizingJobProxy:
            return EJobPhase::FinalizingJobProxy;
        case EJobPhaseOld::RunningExtraGpuCheckCommand:
            return EJobPhase::RunningExtraGpuCheckCommand;
        case EJobPhaseOld::WaitingForCleanup:
            return EJobPhase::WaitingForCleanup;
        case EJobPhaseOld::Cleanup:
            return EJobPhase::Cleanup;
        case EJobPhaseOld::Finished:
            return EJobPhase::Finished;
    }

    YT_ABORT();
}

EJobPhaseOld ConvertJobPhaseToOld(EJobPhase phase)
{
    switch (phase) {
        case EJobPhase::Missing:
            return EJobPhaseOld::Missing;
        case EJobPhase::Created:
            return EJobPhaseOld::Created;
        case EJobPhase::PreparingNodeDirectory:
            return EJobPhaseOld::PreparingNodeDirectory;
        case EJobPhase::DownloadingArtifacts:
            return EJobPhaseOld::DownloadingArtifacts;
        case EJobPhase::CachingArtifacts:
            return EJobPhaseOld::CachingArtifacts;
        case EJobPhase::PreparingSlotDirectories:
            return EJobPhaseOld::PreparingSlotDirectories;
        case EJobPhase::PreparingLayers:
            return EJobPhaseOld::PreparingLayers;
        case EJobPhase::PreparingVolumes:
            return EJobPhaseOld::PreparingVolumes;
        case EJobPhase::PreparingGpuCheckVolume:
            return EJobPhaseOld::PreparingGpuCheckVolume;
        case EJobPhase::LinkingVolumes:
            return EJobPhaseOld::LinkingVolumes;
        case EJobPhase::ValidatingRootFS:
            return EJobPhaseOld::ValidatingRootFS;
        case EJobPhase::PreparingSandboxDirectories:
            return EJobPhaseOld::PreparingSandboxDirectories;
        case EJobPhase::RunningSetupCommands:
            return EJobPhaseOld::RunningSetupCommands;
        case EJobPhase::RunningCustomPreparations:
            return EJobPhaseOld::RunningCustomPreparations;
        case EJobPhase::RunningGpuCheckCommand:
            return EJobPhaseOld::RunningGpuCheckCommand;
        case EJobPhase::SpawningJobProxy:
            return EJobPhaseOld::SpawningJobProxy;
        case EJobPhase::PreparingArtifacts:
            return EJobPhaseOld::PreparingArtifacts;
        case EJobPhase::PreparingJob:
            return EJobPhaseOld::PreparingJob;
        case EJobPhase::Running:
            return EJobPhaseOld::Running;
        case EJobPhase::FinalizingJobProxy:
            return EJobPhaseOld::FinalizingJobProxy;
        case EJobPhase::RunningExtraGpuCheckCommand:
            return EJobPhaseOld::RunningExtraGpuCheckCommand;
        case EJobPhase::WaitingForCleanup:
            return EJobPhaseOld::WaitingForCleanup;
        case EJobPhase::Cleanup:
            return EJobPhaseOld::Cleanup;
        case EJobPhase::Finished:
            return EJobPhaseOld::Finished;
    }

    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

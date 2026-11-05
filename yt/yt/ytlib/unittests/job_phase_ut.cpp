#include <yt/yt/ytlib/exec_node/public.h>

#include <yt/yt/core/test_framework/framework.h>

#include <array>
#include <utility>

namespace NYT::NExecNode {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TJobPhaseTest, LegacyValuesRoundTrip)
{
    constexpr std::array PhasePairs{
        std::pair(EJobPhaseOld::Missing, EJobPhase::Missing),
        std::pair(EJobPhaseOld::Created, EJobPhase::Created),
        std::pair(EJobPhaseOld::PreparingNodeDirectory, EJobPhase::PreparingNodeDirectory),
        std::pair(EJobPhaseOld::DownloadingArtifacts, EJobPhase::DownloadingArtifacts),
        std::pair(EJobPhaseOld::CachingArtifacts, EJobPhase::CachingArtifacts),
        std::pair(EJobPhaseOld::PreparingLayers, EJobPhase::PreparingLayers),
        std::pair(EJobPhaseOld::PreparingSlotDirectories, EJobPhase::PreparingSlotDirectories),
        std::pair(EJobPhaseOld::PreparingVolumes, EJobPhase::PreparingVolumes),
        std::pair(EJobPhaseOld::PreparingGpuCheckVolume, EJobPhase::PreparingGpuCheckVolume),
        std::pair(EJobPhaseOld::LinkingVolumes, EJobPhase::LinkingVolumes),
        std::pair(EJobPhaseOld::ValidatingRootFS, EJobPhase::ValidatingRootFS),
        std::pair(EJobPhaseOld::RunningCustomPreparations, EJobPhase::RunningCustomPreparations),
        std::pair(EJobPhaseOld::PreparingSandboxDirectories, EJobPhase::PreparingSandboxDirectories),
        std::pair(EJobPhaseOld::RunningSetupCommands, EJobPhase::RunningSetupCommands),
        std::pair(EJobPhaseOld::RunningGpuCheckCommand, EJobPhase::RunningGpuCheckCommand),
        std::pair(EJobPhaseOld::SpawningJobProxy, EJobPhase::SpawningJobProxy),
        std::pair(EJobPhaseOld::PreparingArtifacts, EJobPhase::PreparingArtifacts),
        std::pair(EJobPhaseOld::PreparingJob, EJobPhase::PreparingJob),
        std::pair(EJobPhaseOld::Running, EJobPhase::Running),
        std::pair(EJobPhaseOld::FinalizingJobProxy, EJobPhase::FinalizingJobProxy),
        std::pair(EJobPhaseOld::RunningExtraGpuCheckCommand, EJobPhase::RunningExtraGpuCheckCommand),
        std::pair(EJobPhaseOld::WaitingForCleanup, EJobPhase::WaitingForCleanup),
        std::pair(EJobPhaseOld::Cleanup, EJobPhase::Cleanup),
        std::pair(EJobPhaseOld::Finished, EJobPhase::Finished),
    };

    for (auto [oldPhase, phase] : PhasePairs) {
        EXPECT_EQ(ConvertJobPhaseFromOld(oldPhase), phase);
        EXPECT_EQ(ConvertJobPhaseToOld(phase), oldPhase);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NExecNode

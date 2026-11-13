#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/exec_node/helpers.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/core/net/public.h>

#include <library/cpp/yt/system/exit.h>

#include <util/generic/hash_set.h>

namespace NYT::NExecNode {

using namespace NScheduler;

using NJobProxy::EJobProxyExitCode;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TJobAbortReasonTest
    : public ::testing::Test
{
protected:
    const TJobCommonConfigPtr Config_ = New<TJobCommonConfig>();

    std::optional<EAbortReason> Deduce(
        const TError& error,
        bool hasFailedChunks = false,
        bool abortJobIfAccountLimitExceeded = false) const
    {
        return DeduceJobAbortReason(error, *Config_, abortJobIfAccountLimitExceeded, hasFailedChunks);
    }

    static TError MakeJobProxyError(int exitCode)
    {
        return TError(NExecNode::EErrorCode::JobProxyFailed, "Job proxy failed")
            .With(TError(EProcessErrorCode::NonZeroExitCode, "Process failed")
                .With("exit_code", exitCode));
    }
};

TEST_F(TJobAbortReasonTest, DetailedErrorReasons)
{
    const std::pair<TErrorCode, EAbortReason> cases[] = {
        {NChunkClient::EErrorCode::AllTargetNodesFailed, EAbortReason::AllTargetNodesFailed},
        {NChunkClient::EErrorCode::ReaderThrottlingFailed, EAbortReason::ReaderThrottlingFailed},
        {NChunkClient::EErrorCode::MasterCommunicationFailed, EAbortReason::MasterCommunicationFailed},
        {NChunkClient::EErrorCode::MasterNotConnected, EAbortReason::MasterNotConnected},
        {NChunkClient::EErrorCode::ReaderTimeout, EAbortReason::ReaderTimeout},
        {NChunkClient::EErrorCode::ReaderRetryCountLimitExceeded, EAbortReason::ReaderRetryCountLimitExceeded},
        {NChunkClient::EErrorCode::ChunkBlockFetchFailed, EAbortReason::ChunkBlockFetchFailed},
        {NChunkClient::EErrorCode::ChunkMetaFetchFailed, EAbortReason::ChunkMetaFetchFailed},
        {NChunkClient::EErrorCode::AutoRepairFailed, EAbortReason::AutoRepairFailed},
        {NExecNode::EErrorCode::ConfigCreationFailed, EAbortReason::ConfigCreationFailed},
        {NExecNode::EErrorCode::SlotNotFound, EAbortReason::SlotNotFound},
        {NExecNode::EErrorCode::JobEnvironmentDisabled, EAbortReason::JobEnvironmentDisabled},
        {NExecNode::EErrorCode::ArtifactCopyingFailed, EAbortReason::ArtifactCopyingFailed},
        {NExecNode::EErrorCode::ArtifactDownloadFailed, EAbortReason::ArtifactDownloadFailed},
        {NExecNode::EErrorCode::NodeDirectoryPreparationFailed, EAbortReason::NodeDirectoryPreparationFailed},
        {NExecNode::EErrorCode::SlotLocationDisabled, EAbortReason::SlotLocationDisabled},
        {NExecNode::EErrorCode::NoLayerLocationAvailable, EAbortReason::NoLayerLocationAvailable},
        {NExecNode::EErrorCode::NotEnoughDiskSpace, EAbortReason::NotEnoughDiskSpace},
        {NJobProxy::EErrorCode::MemoryCheckFailed, EAbortReason::MemoryCheckFailed},
        {NContainers::EErrorCode::FailedToStartContainer, EAbortReason::FailedToStartContainer},
        {EProcessErrorCode::CannotResolveBinary, EAbortReason::CannotResolveBinary},
        {NNet::EErrorCode::ResolveTimedOut, EAbortReason::ResolveTimedOut},
        {NExecNode::EErrorCode::JobProxyPreparationTimeout, EAbortReason::JobProxyPreparationTimeout},
        {NExecNode::EErrorCode::JobPreparationTimeout, EAbortReason::JobPreparationTimeout},
        {NExecNode::EErrorCode::GpuCheckCommandFailed, EAbortReason::GpuCheckCommandFailed},
        {NExecNode::EErrorCode::GpuLayerNotFetched, EAbortReason::GpuLayerNotFetched},
        {NJobProxy::EErrorCode::JobNotRunning, EAbortReason::JobNotRunning},
        {NExecNode::EErrorCode::ArtifactFetchFailed, EAbortReason::ArtifactFetchFailed},
        {NJobProxy::EErrorCode::UserJobPortoApiError, EAbortReason::UserJobPortoApiError},
    };

    for (const auto& [code, reason] : cases) {
        SCOPED_TRACE(code);

        auto error = TError(code, "Infrastructure error");
        EXPECT_EQ(Deduce(error), reason);
        EXPECT_EQ(Deduce(TError("Wrapped job error").With(error)), reason);
        EXPECT_FALSE(IsSchedulingReason(reason));
    }
}

TEST_F(TJobAbortReasonTest, ErrorTreePrecedence)
{
    auto readerError = TError(NChunkClient::EErrorCode::ReaderTimeout, "Reader timed out");
    auto artifactError = TError(NExecNode::EErrorCode::ArtifactDownloadFailed, "Artifact download failed")
        .With(readerError);
    EXPECT_EQ(Deduce(artifactError), EAbortReason::ArtifactDownloadFailed);

    auto wrappedError = TError("Job failed")
        .With(readerError)
        .With(artifactError);
    EXPECT_EQ(Deduce(wrappedError), EAbortReason::ReaderTimeout);

    auto volumeError = TError(NExecNode::EErrorCode::RootVolumePreparationFailed, "Volume preparation failed")
        .With(artifactError);
    EXPECT_EQ(Deduce(volumeError), EAbortReason::RootVolumePreparationFailed);

    artifactError.Add("abort_reason", EAbortReason::NodeOffline);
    EXPECT_EQ(Deduce(artifactError), EAbortReason::NodeOffline);
    EXPECT_EQ(Deduce(volumeError.With(artifactError)), EAbortReason::NodeOffline);
}

TEST_F(TJobAbortReasonTest, FailedChunksAndExceptions)
{
    auto artifactError = TError(NExecNode::EErrorCode::ArtifactDownloadFailed, "Artifact download failed");
    EXPECT_EQ(Deduce(artifactError, /*hasFailedChunks*/ true), EAbortReason::FailedChunks);

    auto resolveError = TError(NNet::EErrorCode::ResolveTimedOut, "Resolve timed out");
    EXPECT_EQ(Deduce(resolveError, /*hasFailedChunks*/ true), EAbortReason::ResolveTimedOut);

    auto throttlingError = TError(NChunkClient::EErrorCode::ReaderThrottlingFailed, "Reader throttling failed");
    EXPECT_EQ(Deduce(throttlingError, /*hasFailedChunks*/ true), EAbortReason::ReaderThrottlingFailed);

    auto nameTableError = TError(NTableClient::EErrorCode::NameTableUpdateFailed, "Name table update failed");
    EXPECT_EQ(Deduce(nameTableError, /*hasFailedChunks*/ true), std::nullopt);
}

TEST_F(TJobAbortReasonTest, LayerErrorsRemainFailures)
{
    const EErrorCode layerErrorCodes[] = {
        EErrorCode::LayerUnpackingFailed,
        EErrorCode::DockerImagePullingFailed,
        EErrorCode::InvalidImage,
    };

    for (auto code : layerErrorCodes) {
        SCOPED_TRACE(static_cast<int>(code));

        auto error = TError(code, "Bad layer")
            .With("abort_reason", EAbortReason::NodeOffline)
            .With(TError(EErrorCode::ArtifactDownloadFailed, "Download failed"));
        EXPECT_EQ(Deduce(error), std::nullopt);
    }
}

TEST_F(TJobAbortReasonTest, ControllerAgentAbortOverridesUserJobFailure)
{
    auto abortError = TError(EErrorCode::AbortByControllerAgent, "Controller agent aborted job");
    auto userError = TError(NJobProxy::EErrorCode::UserJobFailed, "User job failed")
        .With(TError(NChunkClient::EErrorCode::ReaderTimeout, "Reader timed out"));
    EXPECT_EQ(Deduce(userError.With(abortError)), std::nullopt);
    abortError.Add("abort_reason", EAbortReason::Preemption);
    EXPECT_EQ(Deduce(userError.With(abortError)), EAbortReason::Preemption);
}

TEST_F(TJobAbortReasonTest, AccountLimitPolicy)
{
    auto error = TError(NSecurityClient::EErrorCode::AccountLimitExceeded, "Account limit exceeded");
    EXPECT_EQ(Deduce(error), std::nullopt);
    EXPECT_EQ(
        Deduce(error, /*hasFailedChunks*/ false, /*abortJobIfAccountLimitExceeded*/ true),
        EAbortReason::AccountLimitExceeded);
}

TEST_F(TJobAbortReasonTest, ProcessStartupFailure)
{
    auto error = TError(EProcessErrorCode::CannotStartProcess, "Cannot start process");
    EXPECT_EQ(Deduce(error), std::nullopt);

    auto proxyError = TError(EErrorCode::JobProxyFailed, "Job proxy failed")
        .With(error);
    EXPECT_EQ(Deduce(proxyError), EAbortReason::CannotStartProcess);
}

TEST_F(TJobAbortReasonTest, DetailedJobProxyExitReasons)
{
    const std::pair<EJobProxyExitCode, EAbortReason> cases[] = {
        {EJobProxyExitCode::SupervisorCommunicationFailed, EAbortReason::JobProxySupervisorCommunicationFailed},
        {EJobProxyExitCode::ResultReportFailed, EAbortReason::JobProxyResultReportFailed},
        {EJobProxyExitCode::ResourcesUpdateFailed, EAbortReason::JobProxyResourcesUpdateFailed},
        {EJobProxyExitCode::GetJobSpecFailed, EAbortReason::JobProxyGetJobSpecFailed},
        {EJobProxyExitCode::InvalidSpecVersion, EAbortReason::JobProxyInvalidSpecVersion},
        {EJobProxyExitCode::PortoManagementFailed, EAbortReason::JobProxyPortoManagementFailed},
    };

    for (const auto& [code, reason] : cases) {
        SCOPED_TRACE(static_cast<int>(code));

        auto error = MakeJobProxyError(static_cast<int>(code));
        EXPECT_EQ(Deduce(error), reason);
        Config_->TreatJobProxyFailureAsAbort = true;
        EXPECT_EQ(Deduce(error), reason);
        Config_->TreatJobProxyFailureAsAbort = false;
    }
}

TEST_F(TJobAbortReasonTest, JobProxyFailurePolicies)
{
    auto preparationError = MakeJobProxyError(static_cast<int>(EJobProxyExitCode::JobProxyPrepareFailed));
    EXPECT_EQ(Deduce(preparationError), EAbortReason::JobProxyFailed);
    Config_->TreatJobProxyPreparationFailureAsAbort = false;
    EXPECT_EQ(Deduce(preparationError), std::nullopt);

    auto ioError = MakeJobProxyError(static_cast<int>(EProcessExitCode::IOError));
    EXPECT_EQ(Deduce(ioError), EAbortReason::JobProxyFailed);
    Config_->TreatJobProxyIOErrorAsAbort = false;
    EXPECT_EQ(Deduce(ioError), std::nullopt);

    auto unknownExitError = MakeJobProxyError(/*exitCode*/ 255);
    EXPECT_EQ(Deduce(unknownExitError), std::nullopt);
    Config_->TreatJobProxyFailureAsAbort = true;
    EXPECT_EQ(Deduce(unknownExitError), EAbortReason::JobProxyFailed);
    EXPECT_EQ(Deduce(preparationError), EAbortReason::JobProxyFailed);
}

TEST_F(TJobAbortReasonTest, JobProxyResourceFailures)
{
    auto overdraftError = MakeJobProxyError(static_cast<int>(EJobProxyExitCode::ResourceOverdraft));
    EXPECT_EQ(Deduce(overdraftError), EAbortReason::ResourceOverdraft);

    auto signalError = TError(EProcessErrorCode::Signal, "Process killed by signal");
    auto proxyError = TError(EErrorCode::JobProxyFailed, "Job proxy failed");
    EXPECT_EQ(Deduce(proxyError.With(signalError)), std::nullopt);
    signalError.Add("oom_killed", true);
    EXPECT_EQ(Deduce(proxyError.With(signalError)), EAbortReason::ResourceOverdraft);
    EXPECT_EQ(Deduce(signalError), std::nullopt);
}

TEST_F(TJobAbortReasonTest, UnrecognizedErrorsRemainFailures)
{
    EXPECT_EQ(Deduce(TError("Unknown job error")), std::nullopt);
    EXPECT_EQ(Deduce(TError()), std::nullopt);

    auto userError = TError(NJobProxy::EErrorCode::UserJobFailed, "User job failed")
        .With(TError(EProcessErrorCode::NonZeroExitCode, "User process failed")
            .With("exit_code", static_cast<int>(EJobProxyExitCode::SupervisorCommunicationFailed)));
    EXPECT_EQ(Deduce(userError), std::nullopt);
}

TEST(TAbortReasonTest, SchedulingClassification)
{
    const THashSet<EAbortReason> schedulingReasons = {
        EAbortReason::SchedulingTimeout,
        EAbortReason::SchedulingResourceOvercommit,
        EAbortReason::SchedulingOperationSuspended,
        EAbortReason::SchedulingJobSpecThrottling,
        EAbortReason::SchedulingOther,
        EAbortReason::SchedulingOperationDisabled,
        EAbortReason::SchedulingOperationIsNotAlive,
    };

    for (auto reason : TEnumTraits<EAbortReason>::GetDomainValues()) {
        SCOPED_TRACE(static_cast<int>(reason));

        EXPECT_EQ(IsSchedulingReason(reason), schedulingReasons.contains(reason));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

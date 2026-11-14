#include "helpers.h"

#include "config.h"

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/net/public.h>

#include <library/cpp/yt/error/error_helpers.h>

#include <library/cpp/yt/system/exit.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NYT::NExecNode {

using namespace NScheduler;

using NJobProxy::EJobProxyExitCode;

////////////////////////////////////////////////////////////////////////////////

const std::string& GetSandboxRelPath(ESandboxKind sandboxKind)
{
    const auto& sandboxName = SandboxDirectoryNames[sandboxKind];
    YT_VERIFY(!sandboxName.empty());
    return sandboxName;
}

////////////////////////////////////////////////////////////////////////////////

TErrorCode GetDisableErrorCode(
    const TError& error,
    TErrorCode fallbackErrorCode)
{
    if (NFS::IsOutOfDiskSpaceError(error) ||
        error.FindMatching(NContainers::EPortoErrorCode::NoSpace).has_value())
    {
        return EErrorCode::NotEnoughDiskSpace;
    }

    return fallbackErrorCode;
}

////////////////////////////////////////////////////////////////////////////////

EAllocationState JobStateToAllocationState(EJobState jobState)
{
    switch (jobState) {
        case EJobState::None:
            return EAllocationState::Scheduled;
        case EJobState::Waiting:
            return EAllocationState::Waiting;
        case EJobState::Running:
            return EAllocationState::Running;
        case EJobState::Aborting:
            return EAllocationState::Finishing;
        case EJobState::Completed:
        case EJobState::Failed:
        case EJobState::Aborted:
            return EAllocationState::Finished;
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

std::optional<EAbortReason> DeduceJobAbortReason(
    const TError& resultError,
    const TJobCommonConfig& config,
    bool abortJobIfAccountLimitExceeded,
    bool hasFailedChunks)
{
    static const THashSet<TErrorCode> LayerErrorCodes = {
        NExecNode::EErrorCode::LayerUnpackingFailed,
        NExecNode::EErrorCode::DockerImagePullingFailed,
        NExecNode::EErrorCode::InvalidImage,
    };

    static const THashSet<TErrorCode> IgnoredFailedChunksErrorCodes = {
        NNet::EErrorCode::ResolveTimedOut,
        NChunkClient::EErrorCode::ReaderThrottlingFailed,
        NTableClient::EErrorCode::NameTableUpdateFailed,
    };

    static const THashMap<TErrorCode, EAbortReason> AbortReasonsByErrorCode = {
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
    };

    if (hasFailedChunks && !resultError.FindMatching(IgnoredFailedChunksErrorCodes)) {
        return EAbortReason::FailedChunks;
    }

    // This is most probably user error, still we don't want to make it fatal.
    if (resultError.FindMatching(LayerErrorCodes)) {
        return std::nullopt;
    }

    if (resultError.GetCode() == NJobProxy::EErrorCode::UserJobFailed) {
        if (auto innerError = resultError.FindMatching(NExecNode::EErrorCode::AbortByControllerAgent)) {
            return innerError->Attributes().Find<EAbortReason>("abort_reason");
        }
    }

    if (auto abortReason = FindAttributeRecursive<EAbortReason>(resultError, "abort_reason")) {
        return *abortReason;
    }

    if (abortJobIfAccountLimitExceeded &&
        resultError.FindMatching(NSecurityClient::EErrorCode::AccountLimitExceeded))
    {
        return EAbortReason::AccountLimitExceeded;
    }

    if (resultError.FindMatching(NExecNode::EErrorCode::ResourceOverdraft)) {
        return EAbortReason::ResourceOverdraft;
    }

    if (resultError.FindMatching(NExecNode::EErrorCode::NodeResourceOvercommit)) {
        return EAbortReason::NodeResourceOvercommit;
    }

    if (resultError.FindMatching(NExecNode::EErrorCode::WaitingJobTimeout)) {
        return EAbortReason::WaitingTimeout;
    }

    if (resultError.FindMatching(NExecNode::EErrorCode::AbortByScheduler) ||
        resultError.FindMatching(NJobProxy::EErrorCode::JobNotPrepared))
    {
        return EAbortReason::Scheduler;
    }

    if (resultError.FindMatching(NJobProxy::EErrorCode::ShallowMergeFailed)) {
        return EAbortReason::ShallowMergeFailed;
    }

    if (resultError.FindMatching(NJobProxy::EErrorCode::InterruptionFailed)) {
        return EAbortReason::InterruptionFailed;
    }

    if (resultError.FindMatching(NJobProxy::EErrorCode::InterruptionTimeout)) {
        return EAbortReason::InterruptionTimeout;
    }

    if (resultError.FindMatching(NJobProxy::EErrorCode::InterruptionUnsupported)) {
        return EAbortReason::InterruptionUnsupported;
    }

    if (resultError.FindMatching(NExecNode::EErrorCode::RootVolumePreparationFailed)) {
        return EAbortReason::RootVolumePreparationFailed;
    }

    if (resultError.FindMatching(NExecNode::EErrorCode::OverlayLayerPreparationFailed)) {
        return EAbortReason::OverlayLayerPreparationFailed;
    }

    if (resultError.FindMatching(NJobProxy::EErrorCode::UserJobPortoApiError)) {
        return EAbortReason::UserJobPortoApiError;
    }

    if (auto abortError = resultError.FindMatching([] (TErrorCode code) {
        return AbortReasonsByErrorCode.contains(code);
    }))
    {
        return GetOrCrash(AbortReasonsByErrorCode, abortError->GetCode());
    }

    if (resultError.FindMatching(NExecNode::EErrorCode::JobProxyFailed)) {
        if (resultError.FindMatching(EProcessErrorCode::CannotStartProcess)) {
            return EAbortReason::CannotStartProcess;
        }

        if (auto processError = resultError.FindMatching(EProcessErrorCode::NonZeroExitCode)) {
            auto exitCode = processError->Attributes().Get<int>("exit_code");
            if (config.TreatJobProxyIOErrorAsAbort && exitCode == static_cast<int>(EProcessExitCode::IOError)) {
                return EAbortReason::JobProxyFailed;
            }

            auto jobProxyExitCode = EJobProxyExitCode(exitCode);
            if (TEnumTraits<EJobProxyExitCode>::IsKnownValue(jobProxyExitCode)) {
                switch (jobProxyExitCode) {
                    case EJobProxyExitCode::SupervisorCommunicationFailed:
                        return EAbortReason::JobProxySupervisorCommunicationFailed;

                    case EJobProxyExitCode::ResultReportFailed:
                        return EAbortReason::JobProxyResultReportFailed;

                    case EJobProxyExitCode::ResourcesUpdateFailed:
                        return EAbortReason::JobProxyResourcesUpdateFailed;

                    case EJobProxyExitCode::GetJobSpecFailed:
                        return EAbortReason::JobProxyGetJobSpecFailed;

                    case EJobProxyExitCode::InvalidSpecVersion:
                        return EAbortReason::JobProxyInvalidSpecVersion;

                    case EJobProxyExitCode::PortoManagementFailed:
                        return EAbortReason::JobProxyPortoManagementFailed;

                    case EJobProxyExitCode::JobProxyPrepareFailed:
                        if (config.TreatJobProxyPreparationFailureAsAbort) {
                            return EAbortReason::JobProxyFailed;
                        }

                        break;

                    case EJobProxyExitCode::ResourceOverdraft:
                        return EAbortReason::ResourceOverdraft;

                    default:
                        break;
                }
            }

            if (config.TreatJobProxyFailureAsAbort) {
                return EAbortReason::JobProxyFailed;
            }
        }

        if (auto processSignal = resultError.FindMatching(EProcessErrorCode::Signal)) {
            if (processSignal->Attributes().Find<bool>("oom_killed").value_or(false)) {
                return EAbortReason::ResourceOverdraft;
            }
        }
    }

    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

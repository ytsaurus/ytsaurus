#include "file_provider_postprocessor.h"

#include <yt/yt/flow/library/cpp/file_storage/file_storage.h>

#include <yt/yt/core/crypto/crypto.h>

#include <yt/yt/library/process/subprocess.h>

#include <library/cpp/yt/error/error_helpers.h>
#include <library/cpp/yt/memory/ref.h>

#include <util/folder/path.h>

#include <csignal>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

static constexpr size_t OutputTailSize = 16_KB;

std::string GetCommandDigest(TStringBuf command)
{
    NCrypto::TSha256Hasher hasher;
    hasher.Append(command);
    return hasher.GetHexDigestLowerCase();
}

std::string GetCommandWrapper()
{
    return Format("exec /bin/bash -e -o pipefail -c \"$1\" > >(/usr/bin/tail -c %v) 2> >(/usr/bin/tail -c %v >&2)",
        OutputTailSize,
        OutputTailSize);
}

TError MakePostprocessError(
    const TFileProviderId& providerId,
    const TFileProviderRevisionPtr& revision,
    const TFileProviderSpecPtr& providerSpec,
    TStringBuf phase,
    TDuration elapsed,
    const TError& innerError,
    TStringBuf stdoutTail = {},
    TStringBuf stderrTail = {})
{
    return TError("File provider %Qv postprocessing failed", providerId)
        .With("phase", phase)
        .With("raw_object_id", revision->ObjectId.Underlying())
        .With("command_digest", GetCommandDigest(*providerSpec->PostprocessCommand))
        .With("timeout", providerSpec->PostprocessTimeout)
        .With("elapsed", elapsed)
        .With("stdout_tail", stdoutTail)
        .With("stderr_tail", stderrTail)
        .With(innerError);
}

} // namespace

void PostprocessFileProvider(
    const TFileProviderId& providerId,
    const TFileProviderRevisionPtr& revision,
    const TFileProviderSpecPtr& providerSpec,
    NFileStorage::IFileStorageObjectPtr inputObject,
    const std::string& resultPath,
    const NLogging::TLogger& logger)
{
    YT_VERIFY(providerSpec->PostprocessCommand);

    const auto& Logger = logger;
    auto inputPath = TFsPath(inputObject->GetPath());
    auto postprocessPath = TFsPath(resultPath);

    TSubprocess subprocess("/bin/bash", /*copyEnv*/ false);
    subprocess.AddArguments({"-c", GetCommandWrapper(), "yt-flow-postprocessor", *providerSpec->PostprocessCommand});
    auto process = subprocess.GetProcess();
    process->AddEnvVar("PATH=/usr/bin:/bin");
    process->AddEnvVar("LANG=C");
    process->AddEnvVar("LC_ALL=C");
    process->AddEnvVar("TZ=UTC");
    process->AddEnvVar(Format("YT_FLOW_RESOURCE_PATH=%v", inputPath.GetPath()));
    process->AddEnvVar(Format("YT_FLOW_POSTPROCESSING_PATH=%v", postprocessPath.GetPath()));
    process->SetWorkingDirectory(postprocessPath.GetPath());
    process->CreateProcessGroup();

    auto startedAt = TInstant::Now();
    TSubprocessResult result;
    try {
        result = subprocess.Execute(TSharedRef::MakeEmpty(), providerSpec->PostprocessTimeout);
    } catch (const std::exception& ex) {
        THROW_ERROR MakePostprocessError(
            providerId,
            revision,
            providerSpec,
            "execute",
            TInstant::Now() - startedAt,
            TError(ex));
    }

    auto elapsed = TInstant::Now() - startedAt;
    if (!result.Status.IsOK()) {
        auto signal = FindAttributeRecursive<int>(result.Status, "signal");
        auto phase = signal == SIGKILL &&
                elapsed >= providerSpec->PostprocessTimeout
            ? "timeout"
            : "exit";
        THROW_ERROR MakePostprocessError(
            providerId,
            revision,
            providerSpec,
            phase,
            elapsed,
            result.Status,
            result.Output.ToStringBuf(),
            result.Error.ToStringBuf());
    }

    YT_LOG_INFO("File provider postprocessing completed (FileProvider: %v, RawObjectId: %v, CommandDigest: %v, Elapsed: %v)",
        providerId,
        revision->ObjectId,
        GetCommandDigest(*providerSpec->PostprocessCommand),
        elapsed);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

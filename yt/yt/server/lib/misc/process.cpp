#ifdef __linux__

#include "process.h"

#include <yt/yt/server/lib/containers/instance.h>

#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/fs.h>

namespace NYT {

using namespace NPipes;
using namespace NNet;
using namespace NConcurrency;
using namespace NContainers;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("Process");

static const pid_t InvalidProcessId = -1;

////////////////////////////////////////////////////////////////////////////////

TPortoProcess::TPortoProcess(
    const TString& path,
    IInstancePtr containerInstance,
    bool copyEnv,
    TDuration pollPeriod)
    : TProcessBase(path)
    , ContainerInstance_(containerInstance)
{
    AddArgument(NFS::GetFileName(path));
    if (copyEnv) {
        for (char** envIt = environ; *envIt; ++envIt) {
            Env_.push_back(Capture(*envIt));
        }
    }
}

void TPortoProcess::Kill(int signal)
{
    ContainerInstance_->Kill(signal);
}

void TPortoProcess::DoSpawn()
{
    YT_VERIFY(ProcessId_ == InvalidProcessId && !Finished_);
    YT_VERIFY(Args_.size());
    if (!WorkingDirectory_.empty()) {
        ContainerInstance_->SetCwd(WorkingDirectory_);
    }
    Started_ = true;
    TFuture<int> execFuture;

    try {
        // First argument must be path to binary.
        ResolvedPath_ = ContainerInstance_->HasRoot()
            ? Path_  // Do not resolve if inside rootfs
            : ResolveBinaryPath(Args_[0]).ValueOrThrow();
        Args_[0] = ResolvedPath_.c_str();
        execFuture = ContainerInstance_->Exec(Args_, Env_);
        try {
            ProcessId_ = ContainerInstance_->GetPid();
        } catch (const std::exception& ex) {
            // This could happen if Porto container has already died or pid namespace of
            // parent container is not a parent of pid namespace of child container.
            // It's not a problem, since for Porto process pid is used for logging purposes only.
            YT_LOG_DEBUG(ex, "Failed to get pid of root process (Container: %v)",
                ContainerInstance_->GetName());
        }
    } catch (const std::exception& ex) {
        Finished_ = true;
        THROW_ERROR_EXCEPTION("Failed to start child process inside Porto")
            << TErrorAttribute("path", Args_[0])
            << TErrorAttribute("container", ContainerInstance_->GetName())
            << ex;
    }
    YT_LOG_DEBUG("Process inside Porto spawned successfully (Path: %v, ExternalPid: %v, Container: %v)",
        Args_[0],
        ProcessId_,
        ContainerInstance_->GetName());

    YT_VERIFY(execFuture);
    execFuture.Subscribe(BIND([=, this_ = MakeStrong(this)](TErrorOr<int> exitCodeOrError) {
        Finished_ = true;
        if (exitCodeOrError.IsOK()) {
            auto& exitCode = exitCodeOrError.ValueOrThrow();
            YT_LOG_DEBUG("Process inside Porto exited (ExitCode: %v, ExternalPid: %v, Container: %v)",
                exitCode,
                ProcessId_,
                ContainerInstance_->GetName());

            FinishedPromise_.Set(StatusToError(exitCode));
        } else {
            YT_LOG_DEBUG("Process inside Porto exited with container error (Error: %v, ExternalPid: %v, Container: %v)",
                exitCodeOrError,
                ProcessId_,
                ContainerInstance_->GetName());

            FinishedPromise_.Set(exitCodeOrError);
        }
    }));
}

static TString CreateStdIONamedPipePath()
{
    const TString name = ToString(TGuid::Create());
    return NFS::GetRealPath(NFS::CombinePaths("/tmp", name));
}

IConnectionWriterPtr TPortoProcess::GetStdInWriter()
{
    auto pipe = TNamedPipe::Create(CreateStdIONamedPipePath());
    ContainerInstance_->SetStdIn(pipe->GetPath());
    NamedPipes_.push_back(pipe);
    return pipe->CreateAsyncWriter();
}

IConnectionReaderPtr TPortoProcess::GetStdOutReader()
{
    auto pipe = TNamedPipe::Create(CreateStdIONamedPipePath());
    ContainerInstance_->SetStdOut(pipe->GetPath());
    NamedPipes_.push_back(pipe);
    return pipe->CreateAsyncReader();
}

IConnectionReaderPtr TPortoProcess::GetStdErrReader()
{
    auto pipe = TNamedPipe::Create(CreateStdIONamedPipePath());
    ContainerInstance_->SetStdErr(pipe->GetPath());
    NamedPipes_.push_back(pipe);
    return pipe->CreateAsyncReader();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#endif

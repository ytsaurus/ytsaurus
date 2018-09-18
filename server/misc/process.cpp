#ifdef __linux__

#include "process.h"

#include <yt/server/containers/instance.h>

#include <yt/core/misc/proc.h>

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
    YCHECK(ProcessId_ == InvalidProcessId && !Finished_);
    YCHECK(Args_.size());
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
            // This could happen if porto container has already died.
            LOG_WARNING(ex, "Failed to get pid of root process (Container: %v)",
                ContainerInstance_->GetName());
        }
    } catch (const std::exception& ex) {
        Finished_ = true;
        THROW_ERROR_EXCEPTION("Failed to start child process inside porto")
            << TErrorAttribute("path", Args_[0])
            << TErrorAttribute("container", ContainerInstance_->GetName())
            << ex;
    }
    LOG_DEBUG("Process inside porto spawned successfully (Path: %v, ExternalPid: %v, Container: %v)",
        Args_[0],
        ProcessId_,
        ContainerInstance_->GetName());

    YCHECK(execFuture);
    execFuture.Apply(BIND([=, this_ = MakeStrong(this)](int exitCode) {
        LOG_DEBUG("Process inside porto exited (ExitCode: %v, ExternalPid: %v, Container: %v)",
            exitCode,
            ProcessId_,
            ContainerInstance_->GetName());

        Finished_ = true;
        FinishedPromise_.Set(StatusToError(exitCode));
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

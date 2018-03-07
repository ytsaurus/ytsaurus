#pragma once

#include "error.h"

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/pipes/pipe.h>

#include <atomic>
#include <vector>
#include <array>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TErrorOr<TString> ResolveBinaryPath(const TString& binary);

////////////////////////////////////////////////////////////////////////////////

class TProcessBase
    : public TRefCounted
{
public:
    explicit TProcessBase(const TString& path);

    void AddArgument(TStringBuf arg);
    void AddEnvVar(TStringBuf var);

    void AddArguments(std::initializer_list<TStringBuf> args);
    void AddArguments(const std::vector<TString>& args);

    void SetWorkingDirectory(const TString& path);

    virtual NNet::IConnectionWriterPtr GetStdInWriter() = 0;
    virtual NNet::IConnectionReaderPtr GetStdOutReader() = 0;
    virtual NNet::IConnectionReaderPtr GetStdErrReader() = 0;

    TFuture<void> Spawn();
    virtual void Kill(int signal) = 0;

    TString GetPath() const;
    int GetProcessId() const;
    bool IsStarted() const;
    bool IsFinished() const;

    TString GetCommandLine() const;

protected:
    const TString Path_;

    int ProcessId_;
    std::atomic<bool> Started_ = {false};
    std::atomic<bool> Finished_ = {false};
    int MaxSpawnActionFD_ = - 1;
    NPipes::TPipe Pipe_;
    std::vector<TString> StringHolders_;
    std::vector<const char*> Args_;
    std::vector<const char*> Env_;
    TString ResolvedPath_;
    TString WorkingDirectory_;
    TPromise<void> FinishedPromise_ = NewPromise<void>();

    virtual void DoSpawn() = 0;
    const char* Capture(const TStringBuf& arg);

private:
    void SpawnChild();
    void ValidateSpawnResult();
    void Child();
    void AsyncPeriodicTryWait();
};

DEFINE_REFCOUNTED_TYPE(TProcessBase)

////////////////////////////////////////////////////////////////////////////////

// Read this
// http://ewontfix.com/7/
// before making any changes.
class TSimpleProcess
    : public TProcessBase
{
public:
    explicit TSimpleProcess(
        const TString& path,
        bool copyEnv = true,
        TDuration pollPeriod = TDuration::MilliSeconds(100));
    virtual void Kill(int signal) override;
    virtual NNet::IConnectionWriterPtr GetStdInWriter() override;
    virtual NNet::IConnectionReaderPtr GetStdOutReader() override;
    virtual NNet::IConnectionReaderPtr GetStdErrReader() override;

private:
    const TDuration PollPeriod_;

    NPipes::TPipeFactory PipeFactory_;
    std::array<NPipes::TPipe, 3> StdPipes_;

    NConcurrency::TPeriodicExecutorPtr AsyncWaitExecutor_;
    struct TSpawnAction
    {
        std::function<bool()> Callback;
        TString ErrorMessage;
    };

    std::vector<TSpawnAction> SpawnActions_;

    void AddDup2FileAction(int oldFD, int newFD);
    virtual void DoSpawn() override;
    void SpawnChild();
    void ValidateSpawnResult();
    void AsyncPeriodicTryWait();
    void Child();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

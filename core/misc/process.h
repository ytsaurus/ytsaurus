#pragma once

#include "error.h"

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/pipes/pipe.h>

#include <atomic>
#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Read this
// http://ewontfix.com/7/
// before making any changes.
class TProcess
    : public TRefCounted
{
public:
    explicit TProcess(
        const Stroka& path,
        bool copyEnv = true,
        TDuration pollPeriod = TDuration::MilliSeconds(100));

    void AddArgument(TStringBuf arg);
    void AddEnvVar(TStringBuf var);

    void AddArguments(std::initializer_list<TStringBuf> args);
    void AddArguments(const std::vector<Stroka>& args);

    void AddCloseFileAction(int fd);
    void AddDup2FileAction(int oldFD, int newFD);

    TFuture<void> Spawn();
    void Kill(int signal);

    Stroka GetPath() const;
    int GetProcessId() const;
    bool IsStarted() const;
    bool IsFinished() const;

    Stroka GetCommandLine() const;

private:
    const Stroka Path_;
    const TDuration PollPeriod_;

    int ProcessId_;
    std::atomic<bool> Started_ = {false};
    std::atomic<bool> Finished_ = {false};

    int MaxSpawnActionFD_ = - 1;

    NPipes::TPipe Pipe_;
    std::vector<Stroka> StringHolders_;
    std::vector<const char*> Args_;
    std::vector<const char*> Env_;

    struct TSpawnAction
    {
        std::function<bool()> Callback;
        Stroka ErrorMessage;
    };

    std::vector<TSpawnAction> SpawnActions_;

    NConcurrency::TPeriodicExecutorPtr AsyncWaitExecutor_;
    TPromise<void> FinishedPromise_ = NewPromise<void>();

    const char* Capture(const TStringBuf& arg);

    void DoSpawn();
    void SpawnChild();
    void ValidateSpawnResult();
    void Child();
    void AsyncPeriodicTryWait();
};

DEFINE_REFCOUNTED_TYPE(TProcess)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

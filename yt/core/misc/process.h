#pragma once

#include "error.h"
#include "pipe.h"

#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Read this
// http://ewontfix.com/7/
// before modification

class TProcess
{
public:
    explicit TProcess(const Stroka& path, bool copyEnv = true);
    ~TProcess();

    TProcess(const TProcess& other) = delete;
    TProcess(TProcess&& other);

    static TProcess CreateCurrentProcessSpawner();

    void AddArgument(TStringBuf arg);
    void AddEnvVar(TStringBuf var);

    void AddArguments(std::initializer_list<TStringBuf> args);

    void AddCloseFileAction(int fd);
    void AddDup2FileAction(int oldFd, int newFd);

    void Spawn();
    TError Wait();
    void Kill(int signal);

    int GetProcessId() const;
    bool Started() const;
    bool Finished() const;

private:
    struct TSpawnAction
    {
        std::function<bool()> Callback;
        Stroka ErrorMessage;
    };

    TSpinLock LifecycleChangeLock_;
    bool Started_;
    bool Finished_;

    int ProcessId_;
    Stroka Path_;

    int MaxSpawnActionFD_;

    TPipe Pipe_;
    std::vector<Stroka> StringHolder_;
    std::vector<char*> Args_;
    std::vector<char*> Env_;
    std::vector<TSpawnAction> SpawnActions_;

    char* Capture(TStringBuf arg);

    void SpawnChild();
    void ThrowOnChildError();
    void Child();

    void Swap(TProcess& other);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

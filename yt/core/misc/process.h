#pragma once

#include "error.h"
#include "pipe.h"

#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

DECLARE_ENUM(EFileAction,
    ((Close) (1))
    ((Dup2) (2))
);

struct TSpawnFileAction
{
    EFileAction Type;
    int Param1;
    int Param2;
};

}

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
    TProcess(TProcess&& other) = delete;

    void AddArgument(TStringBuf arg);
    void AddEnvVar(TStringBuf var);

    void AddCloseFileAction(int fd);
    void AddDup2FileAction(int oldFd, int newFd);

    TError Spawn();
    TError Wait();

    int GetProcessId() const;

private:
    bool Finished_;
    int Status_;
    int ProcessId_;
    TPipe Pipe_;
    TPipe ChildPipe_;
    std::vector<char> Path_;
    std::vector<std::vector<char>> StringHolder_;
    std::vector<char*> Args_;
    std::vector<char*> Env_;
    std::vector<NDetail::TSpawnFileAction> FileActions_;

    const char* GetPath() const;
    char* Capture(TStringBuf arg);

    void DoSpawn();
    void ChildErrorHandler(int errorType);
};

////////////////////////////////////////////////////////////////////////////////

TError SafeAtomicCloseExecPipe(int pipefd[2]);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

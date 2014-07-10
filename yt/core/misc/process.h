#pragma once

#include "error.h"

#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TProcess
{
public:
    explicit TProcess(const Stroka& path);
    ~TProcess();

    TProcess(const TProcess& other) = delete;
    TProcess(TProcess&& other) = delete;

    void AddArgument(TStringBuf arg);

    TError Spawn();
    TError Wait();

    int GetProcessId() const;

private:
    bool Finished_;
    int Status_;
    int ProcessId_;
    int Pipe_[2];
    int ChildPipe_[2];
    std::vector<char> Path_;
    std::vector<std::vector<char>> StringHolder_;
    std::vector<char*> Args_;
    std::vector<char*> Env_;

    const char* GetPath() const;
    char* Capture(TStringBuf arg);

    int DoSpawn();

};

////////////////////////////////////////////////////////////////////////////////

TError SafeAtomicCloseExecPipe(int pipefd[2]);

////////////////////////////////////////////////////////////////////////////////

} // NYT

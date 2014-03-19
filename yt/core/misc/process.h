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
    TProcess(TProcess&& other) = default;

    void AddArgument(const Stroka& arg);

    TError Spawn(int flags=CLONE_VM);
    TError Wait();

    int GetProcessId() const;

private:
    bool Finished_;
    int Status_;
    int ProcessId_;
    int Pipe_[2];
    int ChildPipe_[2];
    std::vector<char> Path_;
    std::vector<std::vector<char>> Holder_;
    std::vector<char*> Args_;
    std::vector<char*> Env_;
    std::vector<char> Stack_;

    const char* GetPath() const;
    // TODO(babenko): rename (or better get rid of)
    char* Copy(const char* arg);

    static int ChildMain(void*);
    int DoSpawn();

};

////////////////////////////////////////////////////////////////////////////////

} // NYT

#pragma once

#include "error.h"

#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TProcess
    : private TNonCopyable
{
public:
    explicit TProcess(const char* path);
    ~TProcess();

    void AddArgument(const char* arg);

    TError Spawn();
    TError Wait();

    const char* GetPath() const;
    int GetProcessId() const;

private:
    bool Finished_;
    int Status_;
    int ProcessId_;
    int Pipe_[2];
    std::vector<char> Path_;
    std::vector<std::vector<char>> Holder_;
    std::vector<char* > Args_;
    std::vector<char* > Env_;
    std::vector<char> Stack_;

    // TODO(babenko): rename (or better get rid of)
    char* Copy(const char* arg);

    static int ChildMain(void*);
    int DoSpawn();

};

////////////////////////////////////////////////////////////////////////////////

} // NYT

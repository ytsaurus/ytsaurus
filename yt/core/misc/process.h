#pragma once

#include "error.h"

#include <vector>

namespace NYT {

class TProcess
{
public:
    TProcess(const char* path);

    void AddArgument(const char* arg);

    TError Spawn();
    TError Wait();

    const char* GetPath() const;
    int GetProcessId() const;
private:
    bool IsFinished_;
    int Status_;
    int ProcessId_;
    std::vector<char> Path_;
    std::vector<std::vector<char>> Holder_;
    std::vector<char* > Args_;
    std::vector<char* > Env_;
    std::vector<char> Stack_;
};

} // NYT

#pragma once

#include <library/cpp/yt/memory/safe_memory_reader.h>

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

class TFramePointerCursor
{
public:
    TFramePointerCursor(
        TSafeMemoryReader* memoryReader,
        ui64 rip,
        ui64 rsp,
        ui64 rbp);

    bool IsFinished() const;
    const void* GetCurrentIP() const;
    void MoveNext();

private:
    TSafeMemoryReader* MemoryReader_;
    bool Finished_ = false;
    bool First_ = true;

    const void *Rip_ = nullptr;
    const void *Rbp_ = nullptr;
    const void *StartRsp_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace

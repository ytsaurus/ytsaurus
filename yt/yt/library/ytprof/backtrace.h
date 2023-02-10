#pragma once

#include <yt/yt/core/misc/safe_memory_reader.h>

#include <contrib/libs/libunwind/include/libunwind.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

// We use libunwind directly, since we need PC information that is more precise information than usual.
class TUWCursor
{
public:
    TUWCursor();

    bool IsEnd();
    bool Next();

    void* GetIP();
    void* GetStartIP();

private:
    unw_context_t Context_;
    unw_cursor_t Cursor_;
    bool End_ = false;
    int StepReturnValue_;

    void* StartIP_ = nullptr;
    void* IP_ = nullptr;

    void ReadIP();
};

////////////////////////////////////////////////////////////////////////////////

// Manual implementation of frame pointer unwinder.
//
// Unlike libunwind based unwinder, this one does not segfault when invoked from signal handler.
class TFramePointerCursor
{
public:
    TFramePointerCursor(TSafeMemoryReader* mem, void* rip, void* rsp, void* rbp);

    bool IsEnd();
    bool Next();
    void* GetIP();

private:
    TSafeMemoryReader* Mem_;
    bool End_ = false;
    bool First_ = true;

    void *Rip_ = nullptr;
    void *Rbp_ = nullptr;

    void *StartRsp_ = nullptr;
};

bool IsProfileBuild();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf

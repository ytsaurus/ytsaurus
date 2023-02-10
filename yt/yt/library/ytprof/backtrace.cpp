#include "backtrace.h"

#include <fcntl.h>
#include <unistd.h>

#include <utility>
#include <array>

#include <sys/ucontext.h>

#include <util/generic/yexception.h>
#include <util/system/compiler.h>
#include <util/generic/size_literals.h>
#include <util/system/error.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

TUWCursor::TUWCursor()
{
    if (unw_getcontext(&Context_) != 0) {
        End_ = true;
        return;
    }

    if (unw_init_local(&Cursor_, &Context_) != 0) {
        End_ = true;
        return;
    }

    ReadIP();
}

bool TUWCursor::IsEnd()
{
    return End_;
}

bool TUWCursor::Next()
{
    if (End_) {
        return false;
    }

    StepReturnValue_ = unw_step(&Cursor_);
    if (StepReturnValue_ <= 0) {
        End_ = true;
        return false;
    }

    ReadIP();
    return !End_;
}

void* TUWCursor::GetIP()
{
    return IP_;
}

void* TUWCursor::GetStartIP()
{
    return StartIP_;
}

void TUWCursor::ReadIP()
{
    unw_word_t ip = 0;
    int rv = unw_get_reg(&Cursor_, UNW_REG_IP, &ip);
    if (rv < 0) {
        End_ = true;
        return;
    }

    IP_ = reinterpret_cast<void*>(ip);

    unw_proc_info_t procInfo;
    if (unw_get_proc_info(&Cursor_, &procInfo) < 0) {
        StartIP_ = nullptr;
    } else {
        StartIP_ = reinterpret_cast<void*>(procInfo.start_ip);
    }
}

////////////////////////////////////////////////////////////////////////////////

TFramePointerCursor::TFramePointerCursor(TSafeMemoryReader* mem, void* rip, void* rsp, void* rbp)
    : Mem_(mem)
    , Rip_(rip)
    , Rbp_(rbp)
    , StartRsp_(rsp)
{ }

bool TFramePointerCursor::IsEnd()
{
    return End_;
}

void Debug(const char *msg)
{
    Y_UNUSED(msg);
}

bool TFramePointerCursor::Next()
{
    if (End_) {
        return false;
    }

    auto add = [] (auto ptr, auto delta) {
        return reinterpret_cast<void*>(reinterpret_cast<intptr_t>(ptr) + delta);
    };

    auto checkPtr = [this] (auto ptr) {
        ui8 data;
        return Mem_->Read(ptr, &data);
    };

    // We try unwinding stack manually by following frame pointers.
    //
    // We assume that stack does not span more than 4mb.

    if (First_) {
        First_ = false;

        // For the first frame there are three special cases where naive
        // unwinding would skip the caller frame.
        //
        //   1) Right after call instruction, rbp points to frame of a caller.
        //   2) Right after "push rbp" instruction.
        //   3) Right before ret instruction, rbp points to frame of a caller.
        //
        // We read current instruction and try to detect such cases.
        //
        //  55               push %rbp
        //  48 89 e5         mov %rsp, %rbp
        //  c3               retq

        std::array<ui8, 3> data;
        if (!Mem_->Read(Rip_, &data)) {
            Debug("failed to read instruction");
            End_ = true;
            return false;
        }

        if (data[0] == 0xc3 || data[0] == 0x55) {
            void* savedRip;
            if (!Mem_->Read(StartRsp_, &savedRip)) {
                Debug("failed to read saved rip #1");
                End_ = true;
                return false;
            }

            // Avoid infinite loop.
            if (Rip_ == savedRip) {
                Debug("rip == savedRip #1");
                End_ = true;
                return false;
            }

            // Detect garbage pointer.
            ui8 data;
            if (!Mem_->Read(savedRip, &data)) {
                Debug("saved rip is garbage #1");
                End_ = true;
                return false;
            }

            Rip_ = savedRip;
            return true;
        } else if (data[0] == 0x48 && data[1] == 0x89 && data[2] == 0xe5) {
            void* savedRip;
            if (!Mem_->Read(add(StartRsp_, 8), &savedRip)) {
                Debug("failed to read saved rip #2");
                End_ = true;
                return false;
            }

            // Avoid infinite loop.
            if (Rip_ == savedRip) {
                Debug("rip == savedRip #2");
                End_ = true;
                return false;
            }

            // Detect garbage pointer.
            if (!checkPtr(savedRip)) {
                Debug("saved rip is garbage #2");
                End_ = true;
                return false;
            }

            Rip_ = savedRip;
            return true;
        }
    }

    void *savedRbp;
    void *savedRip;
    if (!Mem_->Read(Rbp_, &savedRbp) || !Mem_->Read(add(Rbp_, 8), &savedRip)) {
        Debug("failed to read savedRip or savedRpb");
        End_ = true;
        return false;
    }

    if (!checkPtr(savedRbp)) {
        Debug("savedRbp is garbage");
        End_ = true;
        return false;
    }

    if (!checkPtr(savedRip)) {
        Debug("savedRip is garbage");
        End_ = true;
        return false;
    }

    if (savedRbp < StartRsp_ || savedRbp > add(StartRsp_, 4_MB)) {
        Debug("savedRbp points outside of stack");
        End_ = true;
        return false;
    }

    Rip_ = savedRip;
    Rbp_ = savedRbp;
    return true;
}

void* TFramePointerCursor::GetIP()
{
    return Rip_;
}

bool IsProfileBuild()
{
#ifdef YTPROF_PROFILE_BUILD
    return true;
#else
    return false;
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf

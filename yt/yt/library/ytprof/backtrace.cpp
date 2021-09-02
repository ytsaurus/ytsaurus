#include "backtrace.h"

#include <sys/ucontext.h>

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

} // namespace NYT::NYTProf

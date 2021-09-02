#pragma once

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

} // namespace NYT::NYTProf

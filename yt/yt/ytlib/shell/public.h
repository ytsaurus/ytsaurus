#pragma once

#include <library/cpp/yt/error/error_code.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NShell {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((ShellExited)          (1800))
    ((ShellManagerShutDown) (1801))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShell

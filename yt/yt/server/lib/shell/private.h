#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NShell {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, ShellLogger, "Shell");

extern const std::string ShellToolDirectory;
extern const std::string ShellToolPath;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShell

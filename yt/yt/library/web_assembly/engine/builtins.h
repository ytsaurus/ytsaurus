#pragma once

#include "public.h"

#include <yt/yt/library/web_assembly/api/bytecode.h>

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

TModuleBytecode GetBuiltinMinimalRuntimeSdk();

TModuleBytecode GetBuiltinYtQlUdfs();

TModuleBytecode GetBuiltinSdk();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly

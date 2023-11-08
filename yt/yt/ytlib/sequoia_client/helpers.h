#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

TMangledSequoiaPath MangleSequoiaPath(const NYPath::TYPath& rawPath);

NYPath::TYPath DemangleSequoiaPath(const TMangledSequoiaPath& mangledPath);

TMangledSequoiaPath MakeLexicographicallyMaximalMangledSequoiaPathForPrefix(const TMangledSequoiaPath& prefix);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

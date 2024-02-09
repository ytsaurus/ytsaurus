#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/misc/property.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

struct TSelectRowsRequest
{
    std::vector<TString> Where;
    std::vector<TString> OrderBy;
    std::optional<int> Limit;
};

////////////////////////////////////////////////////////////////////////////////

TMangledSequoiaPath MangleSequoiaPath(NYPath::TYPathBuf rawPath);

NYPath::TYPath DemangleSequoiaPath(const TMangledSequoiaPath& mangledPath);

TMangledSequoiaPath MakeLexicographicallyMaximalMangledSequoiaPathForPrefix(const TMangledSequoiaPath& prefix);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

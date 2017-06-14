#pragma once

#include "public.h"

#include <yt/ytlib/ypath/public.h>

namespace NYT {
namespace NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetUserPath(const TString& name);
NYPath::TYPath GetGroupPath(const TString& name);

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityClient
} // namespace NYT


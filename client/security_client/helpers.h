#pragma once

#include "public.h"

#include <yt/client/ypath/public.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetUserPath(const TString& name);
NYPath::TYPath GetGroupPath(const TString& name);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient


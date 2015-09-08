#pragma once

#include "public.h"

#include <ytlib/ypath/public.h>

namespace NYT {
namespace NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetUserPath(const Stroka& name);
NYPath::TYPath GetGroupPath(const Stroka& name);

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityClient
} // namespace NYT


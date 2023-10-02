#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath MangleCypressPath(NYPath::TYPath rawPath);

NYPath::TYPath DemangleCypressPath(NYPath::TYPath mangledPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

#pragma once

#include <yt/yt/library/nbd/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <util/generic/size_literals.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IBlockDevice)
DECLARE_REFCOUNTED_STRUCT(INbdServer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd

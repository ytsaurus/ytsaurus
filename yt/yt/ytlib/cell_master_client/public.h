#pragma once

#include "public.h"

#include <yt/yt/core/misc/intrusive_ptr.h>

namespace NYT::NCellMasterClient {

///////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCellDirectory)
DECLARE_REFCOUNTED_STRUCT(ICellDirectorySynchronizer)

DECLARE_REFCOUNTED_CLASS(TCellDirectoryConfig)
DECLARE_REFCOUNTED_CLASS(TCellDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMasterClient

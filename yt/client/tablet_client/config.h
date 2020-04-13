#pragma once

#include "public.h"

#include <yt/core/misc/config.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

class TTableMountCacheConfig
    : public TAsyncExpiringCacheConfig
{
public:
    //! If entry is requested for the first time then allow only client who requested the entry to wait for it.
    bool RejectIfEntryIsRequestedButNotReady;


    TTableMountCacheConfig()
    {
        RegisterParameter("reject_if_entry_is_requested_but_not_ready", RejectIfEntryIsRequestedButNotReady)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TTableMountCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient

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

class TRemoteDynamicStoreReaderConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TDuration ClientReadTimeout;
    TDuration ServerReadTimeout;
    TDuration ClientWriteTimeout;
    TDuration ServerWriteTimeout;

    TRemoteDynamicStoreReaderConfig()
    {
        RegisterParameter("client_read_timeout", ClientReadTimeout)
            .Default(TDuration::Seconds(20));
        RegisterParameter("server_read_timeout", ServerReadTimeout)
            .Default(TDuration::Seconds(20));
        RegisterParameter("client_write_timeout", ClientWriteTimeout)
            .Default(TDuration::Seconds(20));
        RegisterParameter("server_write_timeout", ServerWriteTimeout)
            .Default(TDuration::Seconds(20));
    }
};

DEFINE_REFCOUNTED_TYPE(TRemoteDynamicStoreReaderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient

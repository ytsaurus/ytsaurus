#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NCellMasterClient {

///////////////////////////////////////////////////////////////////////////////

class TCellDirectoryConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    NApi::NNative::TMasterConnectionConfigPtr PrimaryMaster;
    std::vector<NApi::NNative::TMasterConnectionConfigPtr> SecondaryMasters;
    NApi::NNative::TMasterConnectionConfigPtr MasterCache;

    NObjectClient::TCachingObjectServiceConfigPtr CachingObjectService;

    TCellDirectoryConfig();
};

DEFINE_REFCOUNTED_TYPE(TCellDirectoryConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellDirectorySynchronizerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Interval between subsequent directory updates.
    std::optional<TDuration> SyncPeriod;

    //! Delay before the next directory update in case the last one was unsuccessful.
    //! Usually should be (significantly) less than #SyncPeriod.
    //! If null, #SyncPeriod is used instead.
    std::optional<TDuration> RetryPeriod;

    TDuration ExpireAfterSuccessfulUpdateTime;
    TDuration ExpireAfterFailedUpdateTime;

    TCellDirectorySynchronizerConfig();
};

DEFINE_REFCOUNTED_TYPE(TCellDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMasterClient

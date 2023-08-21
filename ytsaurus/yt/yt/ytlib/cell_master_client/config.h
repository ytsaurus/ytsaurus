#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NCellMasterClient {

///////////////////////////////////////////////////////////////////////////////

class TCellDirectoryConfig
    : public virtual NYTree::TYsonStruct
{
public:
    NApi::NNative::TMasterConnectionConfigPtr PrimaryMaster;
    std::vector<NApi::NNative::TMasterConnectionConfigPtr> SecondaryMasters;
    NApi::NNative::TMasterCacheConnectionConfigPtr MasterCache;

    NObjectClient::TCachingObjectServiceConfigPtr CachingObjectService;

    REGISTER_YSON_STRUCT(TCellDirectoryConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellDirectoryConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellDirectorySynchronizerConfig
    : public NYTree::TYsonStruct
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

    REGISTER_YSON_STRUCT(TCellDirectorySynchronizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMasterClient

#pragma once

#include "public.h"

#include <yt/yt/core/http/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NS3 {

////////////////////////////////////////////////////////////////////////////////

//! NB: This class is part of master snapshots, consider reign promotion when changing it.
class TS3ConnectionConfig
    : public virtual NYTree::TYsonStruct
{
public:
    //! Url of the S3 server, for example, http://my_bucket.s3.amazonaws.com
    TString Url;

    //! Name of the region.
    //! In some of the S3 implementations it is already included into
    //! address, in some not.
    TString Region;

    // TODO(achulkov2): [PLater] The bucket is not actually needed to establish a connection,
    // so there is no good reason for it to be part of the connection config.
    //! Name of the bucket to use.
    TString Bucket;

    //! Url for S3 proxy server. If empty, Url will be used.
    std::optional<TString> ProxyUrl;

    REGISTER_YSON_STRUCT(TS3ConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TS3ConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TS3ClientConfig
    : public TS3ConnectionConfig
    , public NHttp::TClientConfig
{
    REGISTER_YSON_STRUCT(TS3ClientConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TS3ClientConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3

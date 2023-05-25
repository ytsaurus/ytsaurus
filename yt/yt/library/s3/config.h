#pragma once

#include "public.h"

#include <yt/yt/core/http/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NS3 {

////////////////////////////////////////////////////////////////////////////////

class TS3ConnectionConfig
    : public NHttp::TClientConfig
{
public:
    //! Url of the S3 server, for example, http://my_bucket.s3.amazonaws.com
    TString Url;

    //! Name of the region.
    //! In some of the S3 implementations it is already included into
    //! address, in some not.
    TString Region;

    //! Name of the bucket to use.
    TString Bucket;

    //! Credentials.
    TString AccessKeyId;
    TString SecretAccessKey;

    REGISTER_YSON_STRUCT(TS3ConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TS3ConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3

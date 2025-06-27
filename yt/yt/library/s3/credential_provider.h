#pragma once

#include "public.h"

#include <yt/yt/core/http/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/tvm/service/public.h>

namespace NYT::NS3 {

////////////////////////////////////////////////////////////////////////////////

struct TCredentials
{
    TString AccessKeyId;
    TString SecretAccessKey;
    TString SessionToken;
};

////////////////////////////////////////////////////////////////////////////////

struct ICredentialsProvider
    : public TRefCounted
{
    virtual ~ICredentialsProvider() = default;

    virtual TCredentials GetCredentials() = 0;
};

DECLARE_REFCOUNTED_STRUCT(ICredentialsProvider)
DEFINE_REFCOUNTED_TYPE(ICredentialsProvider)

////////////////////////////////////////////////////////////////////////////////

ICredentialsProviderPtr CreateAnonymousCredentialProvider();
ICredentialsProviderPtr CreateStaticCredentialProvider(TString accessKey, TString secretKey);
ICredentialsProviderPtr CreateTVMCredentialProvider(NAuth::TTvmId selfTvm, NAuth::TTvmId s3Tvm, TString tvmSecret);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3

#pragma once

#include "public.h"

#include <yt/core/http/config.h>

namespace NYT {
namespace NHttps {

////////////////////////////////////////////////////////////////////////////////

class TPemBlobConfig
    : public NYTree::TYsonSerializable
{
public:
    TNullable<TString> FileName;
    TNullable<TString> Value;

    TPemBlobConfig()
    {
        RegisterParameter("file_name", FileName)
            .Optional();
        RegisterParameter("value", Value)
            .Optional();

        RegisterPostprocessor([&] {
            if (FileName && Value) {
                THROW_ERROR_EXCEPTION("Cannot specify both \"file_name\" and \"value\"");
            }
            if (!FileName && !Value) {
                THROW_ERROR_EXCEPTION("Must specify either \"file_name\" or \"value\"");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TPemBlobConfig)

////////////////////////////////////////////////////////////////////////////////

class TServerCredentialsConfig
    : public NYTree::TYsonSerializable
{
public:
    //TPemBlobConfigPtr PemRootCerts;
    //std::vector<TSslPemKeyCertPairConfigPtr> PemKeyCertPairs;
    //
    //TServerCredentialsConfig()
    //{
    //    RegisterParameter("pem_root_certs", PemRootCerts)
    //        .Optional();
    //    RegisterParameter("pem_key_cert_pairs", PemKeyCertPairs);
    //}
};

DEFINE_REFCOUNTED_TYPE(TServerCredentialsConfig)

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public NHttp::TServerConfig
{
public:
    TServerCredentialsConfigPtr Credentials;

    TServerConfig();
};

DEFINE_REFCOUNTED_TYPE(TServerConfig);

////////////////////////////////////////////////////////////////////////////////

class TClientConfig
    : public NHttp::TClientConfig
{
public:

    TClientConfig();
};

DEFINE_REFCOUNTED_TYPE(TClientConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttps
} // namespace NYT

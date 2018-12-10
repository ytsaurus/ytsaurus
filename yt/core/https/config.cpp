#include "config.h"

namespace NYT {
namespace NHttps {

////////////////////////////////////////////////////////////////////////////////

TServerCredentialsConfig::TServerCredentialsConfig()
{
    RegisterParameter("private_key", PrivateKey)
        .Optional();
    RegisterParameter("cert_chain", CertChain)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TServerConfig::TServerConfig()
{
    RegisterParameter("credentials", Credentials);
}

////////////////////////////////////////////////////////////////////////////////

TClientCredentialsConfig::TClientCredentialsConfig()
{
    RegisterParameter("private_key", PrivateKey)
        .Optional();
    RegisterParameter("cert_chain", CertChain)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TClientConfig::TClientConfig()
{
    RegisterParameter("credentials", Credentials)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttps
} // namespace NYT

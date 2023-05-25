#include "config.h"

namespace NYT::NS3 {

////////////////////////////////////////////////////////////////////////////////

void TS3ConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("url", &TThis::Url)
        .Default();
    registrar.Parameter("region", &TThis::Region)
        .Default();
    registrar.Parameter("bucket", &TThis::Bucket)
        .Default();

    registrar.Parameter("access_key_id", &TThis::AccessKeyId)
        .Default();
    registrar.Parameter("secret_access_key", &TThis::SecretAccessKey)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3

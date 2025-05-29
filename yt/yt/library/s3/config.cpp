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
}

////////////////////////////////////////////////////////////////////////////////

void TS3ClientConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3

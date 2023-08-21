#include "helpers.h"

namespace NYT::NDns {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TDnsResolveOptions* protoOptions, const TDnsResolveOptions& options)
{
    protoOptions->set_enable_ipv4(options.EnableIPv4);
    protoOptions->set_enable_ipv6(options.EnableIPv6);
}

void FromProto(TDnsResolveOptions* options, const NProto::TDnsResolveOptions& protoOptions)
{
    options->EnableIPv4 = protoOptions.enable_ipv4();
    options->EnableIPv6 = protoOptions.enable_ipv6();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns

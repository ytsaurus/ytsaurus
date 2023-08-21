#pragma once

#include "public.h"

#include <yt/yt/core/dns/dns_resolver.h>

#include <yt/yt/library/dns_over_rpc/client/proto/dns_over_rpc_service.pb.h>

namespace NYT::NDns {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TDnsResolveOptions* protoOptions, const TDnsResolveOptions& options);
void FromProto(TDnsResolveOptions* options, const NProto::TDnsResolveOptions& protoOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns

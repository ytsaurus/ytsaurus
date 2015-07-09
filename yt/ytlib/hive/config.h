#pragma once

#include "public.h"

#include <core/rpc/config.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

class TCellDirectoryConfig
    : public NRpc::TBalancingChannelConfigBase
{ };

DEFINE_REFCOUNTED_TYPE(TCellDirectoryConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT

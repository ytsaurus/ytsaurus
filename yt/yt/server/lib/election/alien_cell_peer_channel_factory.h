#pragma once

#include "public.h"

#include <yt/yt/ytlib/hive/public.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

IAlienCellPeerChannelFactoryPtr CreateAlienCellPeerChannelFactory(
    NHiveClient::TCellDirectoryPtr cellDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::Election

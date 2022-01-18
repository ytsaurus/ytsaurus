#pragma once

#include "public.h"

#include <yt/yt/ytlib/hive/public.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

IAlienCellPeerChannelFactoryPtr CreateAlienCellPeerChannelFactory(
    NHiveClient::ICellDirectoryPtr cellDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::Election

#pragma once

#include "public.h"

#include <yt/yt/ytlib/hive/public.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

IAlienCellPeerChannelFactoryPtr CreateAlienCellPeerChannelFactory(
    NHiveClient::ICellDirectoryPtr cellDirectory,
    NHiveClient::TClusterDirectoryPtr clusterDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::Election

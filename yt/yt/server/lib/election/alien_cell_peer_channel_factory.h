#pragma once

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/ytlib/hive/public.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

IAlienCellPeerChannelFactoryPtr CreateAlienCellPeerChannelFactory(
    NHiveClient::TClusterDirectoryPtr clusterDirectory,
    NHiveClient::TClusterDirectorySynchronizerPtr clusterDirectorySynchronizer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::Election

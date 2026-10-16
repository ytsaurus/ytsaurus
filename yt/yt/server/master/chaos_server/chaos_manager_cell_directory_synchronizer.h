#pragma once

#include "public.h"

#include <yt/yt/ytlib/hive/public.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

struct IChaosManagerCellDirectorySynchronizer
    : public TRefCounted
{
    virtual void AddCellDescriptor(NHiveClient::TCellDescriptor descriptor) = 0;
    virtual void AddCellDescriptors(std::vector<NHiveClient::TCellDescriptor> descriptors) = 0;
    virtual void Reconfigure(TDuration errorDelay) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChaosManagerCellDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

IChaosManagerCellDirectorySynchronizerPtr CreateChaosManagerCellDirectorySynchronizer(
    NHiveClient::ICellDirectoryPtr cellDirectory,
    NLogging::TLogger logger,
    TDuration errorDelay);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer

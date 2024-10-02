#pragma once

#include "public.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TDBConfig
{
    NObjects::ESetUpdateObjectMode UpdateObjectMode = NObjects::ESetUpdateObjectMode::Overwrite;
    NObjects::EDBVersionCompatibility VersionCompatibility = NObjects::EDBVersionCompatibility::LowerOrEqualThanDBVersion;
    bool EnableTombstones = false;
    bool EnableAnnotations = false;
    bool EnableHistorySnapshotColumn = false;
    bool EnableFinalizers = false;
    bool EnableAsynchronousRemovals = true;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

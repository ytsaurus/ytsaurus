#pragma once

#include "cluster_tracker.h"

#include <Interpreters/Cluster.h>
#include <Storages/IStorage.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageConcat(std::vector<TClickHouseTablePtr> tables);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
